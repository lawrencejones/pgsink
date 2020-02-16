package bigquery

import (
	"context"
	"fmt"
	"strings"
	"sync"

	bq "cloud.google.com/go/bigquery"
	kitlog "github.com/go-kit/kit/log"
	"github.com/lawrencejones/pg2sink/pkg/changelog"
	"github.com/pkg/errors"
)

type Dataset struct {
	dataset *bq.Dataset
	tables  map[string]*Table
	sync.RWMutex
}

func newDataset(dataset *bq.Dataset) *Dataset {
	return &Dataset{
		dataset: dataset,
		tables:  map[string]*Table{},
	}
}

type Table struct {
	raw         *bq.Table
	rawMetadata *bq.TableMetadata
	schema      *changelog.Schema
	fingerprint uint64
}

func (d *Dataset) Sync(ctx context.Context, logger kitlog.Logger, schema *changelog.Schema) error {
	logger = kitlog.With(logger, "namespace", schema.Spec.Namespace)
	fingerprint := schema.Spec.GetFingerprint()
	existingTable := d.get(schema.Spec.Namespace)

	if existingTable != nil && existingTable.fingerprint == fingerprint {
		logger.Log("event", "schema.already_fingerprinted", "fingerprint", fingerprint,
			"msg", "not updating BigQuery schema as fingerprint has not changed")
		return nil
	}

	logger.Log("event", "schema.new_fingerprint", "fingerprint", fingerprint,
		"msg", "fingerprint seen for the first time, syncing BigQuery schemas")
	raw, rawMetadata, err := d.syncRawTable(ctx, logger, schema)
	if err != nil {
		return err
	}

	if _, _, err := d.syncViewTable(ctx, logger, schema, raw); err != nil {
		return err
	}

	d.set(schema.Spec.Namespace, &Table{
		raw:         raw,
		rawMetadata: rawMetadata,
		schema:      schema,
		fingerprint: fingerprint,
	})

	return nil
}

func (d *Dataset) TableFor(modification *changelog.Modification) *Table {
	return d.get(modification.Namespace)
}

// syncRawTable creates or updates the raw changelog table that powers the most-recent row
// view. It is named the same as the Postgres table it represents, but with a _raw suffix.
func (d *Dataset) syncRawTable(ctx context.Context, logger kitlog.Logger, schema *changelog.Schema) (*bq.Table, *bq.TableMetadata, error) {
	tableName := fmt.Sprintf("%s_raw", stripPostgresSchema(schema.Spec.Namespace))
	table := d.dataset.Table(tableName)
	md, err := buildRaw(tableName, schema.Spec)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to build raw table metadata")
	}

	md, err = createOrUpdateTable(ctx, logger, table, md, bq.TableMetadataToUpdate{Schema: md.Schema})
	return table, md, err
}

// syncViewTable creates or updates the most-recent row state view, which depends on the
// raw changelog table.
func (d *Dataset) syncViewTable(ctx context.Context, logger kitlog.Logger, schema *changelog.Schema, raw *bq.Table) (*bq.Table, *bq.TableMetadata, error) {
	tableName := stripPostgresSchema(schema.Spec.Namespace)
	table := d.dataset.Table(tableName)
	md, err := buildView(tableName, raw)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to build raw table metadata")
	}

	md, err = createOrUpdateTable(ctx, logger, table, md, bq.TableMetadataToUpdate{ViewQuery: md.ViewQuery})
	return table, md, err
}

func (d *Dataset) get(name string) *Table {
	d.RLock()
	table := d.tables[name]
	d.RUnlock() // don't defer, as defer costs more than direct invocation
	return table
}

func (d *Dataset) set(name string, table *Table) {
	d.Lock()
	defer d.Unlock()

	d.tables[name] = table
}

// stripPostgresSchema removes the Postgres schema from the specification namespace.
// BigQuery doesn't support periods in the table name, so we have to remove the schema. If
// people have multiple tables in different namespaces and switch the source schema
// then...  they're gonna have a bad time.
func stripPostgresSchema(schemaNamespace string) string {
	elements := strings.SplitN(schemaNamespace, ".", 2)
	if len(elements) != 2 {
		panic(fmt.Sprintf("invalid Postgres schema.table_name string: %s", schemaNamespace))
	}

	return elements[1]
}

// createOrUpdateTable will idempotently create the table with configured metadata.
func createOrUpdateTable(ctx context.Context, logger kitlog.Logger, table *bq.Table, md *bq.TableMetadata, mdu bq.TableMetadataToUpdate) (*bq.TableMetadata, error) {
	logger = kitlog.With(logger, "table", table.FullyQualifiedName())
	existing, err := table.Metadata(ctx)
	if allowNotFound(err) != nil {
		return nil, errors.Wrap(err, "failed to get table metadata")
	}

	if existing == nil {
		logger.Log("event", "table.create", "msg", "table does not exist, creating")
		return md, errors.Wrap(table.Create(ctx, md), "failed to create table")
	}

	// Blind update (without etag) as all our updates need to be backward/forward
	// compatible, so it matters very little if someone has raced us.
	logger.Log("event", "table.update", "msg", "updating table with new metadata")
	md, err = table.Update(ctx, bq.TableMetadataToUpdate{Schema: md.Schema}, "")

	return md, errors.Wrap(err, "failed to update table")
}
