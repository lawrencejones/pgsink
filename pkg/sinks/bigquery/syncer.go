package bigquery

import (
	"context"
	"fmt"
	"strings"

	"go.opencensus.io/trace"

	bq "cloud.google.com/go/bigquery"
	kitlog "github.com/go-kit/kit/log"
	"github.com/lawrencejones/pg2sink/pkg/changelog"
	"github.com/lawrencejones/pg2sink/pkg/util"
	"github.com/pkg/errors"
)

// Syncer manages a BigQuery dataset that is the destination for the sink. It provides a
// method to SyncTables into the dataset, ensuring the BigQuery schema matches the
// incoming relations, and GetSyncedTable(modification) which can fetch a handle to the
// destination table in BigQuery.
type Syncer interface {
	SyncTables(context.Context, kitlog.Logger, *changelog.Schema) (*Table, SyncOutcome, error)
}

// Table is a cached BigQuery table entry, associated with a schema and fingerprint that
// created it.
type Table struct {
	Raw         *bq.Table
	RawMetadata *bq.TableMetadata
	Schema      *changelog.Schema
	Fingerprint uint64
}

type datasetSyncer struct {
	dataset *bq.Dataset
	cache   util.Cache
}

func newDatasetSyncer(dataset *bq.Dataset) *datasetSyncer {
	return &datasetSyncer{
		dataset: dataset,
		cache:   util.NewCache(),
	}
}

type SyncOutcome string

const (
	SyncFailed SyncOutcome = "failed"
	SyncNoop               = "noop"
	SyncUpdate             = "update"
)

// SyncTables attempts to reconcile the incoming schema against the BigQuery table it
// tracks. If the schema is unchanged since it was last synced, we do nothing. Once
// synced, the table is available via the GetSyncedTable method.
func (d *datasetSyncer) SyncTables(ctx context.Context, logger kitlog.Logger, schema *changelog.Schema) (*Table, SyncOutcome, error) {
	ctx, span := trace.StartSpan(ctx, "pkg/sinks/bigquery/datasetSyncer.SyncTables")
	defer span.End()

	logger = kitlog.With(logger, "namespace", schema.Spec.Namespace)
	fingerprint := schema.Spec.GetFingerprint()
	existing, ok := d.cache.Get(string(schema.Spec.Namespace)).(*Table)

	if ok && existing != nil && existing.Fingerprint == fingerprint {
		logger.Log("event", "schema.already_fingerprinted", "fingerprint", fingerprint,
			"msg", "not updating BigQuery schema as fingerprint has not changed")
		return existing, SyncNoop, nil
	}

	logger.Log("event", "schema.new_fingerprint", "fingerprint", fingerprint,
		"msg", "fingerprint seen for the first time, syncing BigQuery schemas")
	raw, rawMetadata, err := d.syncRawTable(ctx, logger, schema)
	if err != nil {
		return nil, SyncFailed, err
	}

	if _, _, err := d.syncViewTable(ctx, logger, schema, raw); err != nil {
		return nil, SyncFailed, err
	}

	table := &Table{
		Raw:         raw,
		RawMetadata: rawMetadata,
		Schema:      schema,
		Fingerprint: fingerprint,
	}

	d.cache.Set(string(schema.Spec.Namespace), table)

	return table, SyncUpdate, nil
}

// syncRawTable creates or updates the raw changelog table that powers the most-recent row
// view. It is named the same as the Postgres table it represents, but with a _raw suffix.
func (d *datasetSyncer) syncRawTable(ctx context.Context, logger kitlog.Logger, schema *changelog.Schema) (*bq.Table, *bq.TableMetadata, error) {
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
func (d *datasetSyncer) syncViewTable(ctx context.Context, logger kitlog.Logger, schema *changelog.Schema, raw *bq.Table) (*bq.Table, *bq.TableMetadata, error) {
	tableName := stripPostgresSchema(schema.Spec.Namespace)
	table := d.dataset.Table(tableName)
	md, err := buildView(tableName, raw)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to build raw table metadata")
	}

	md, err = createOrUpdateTable(ctx, logger, table, md, bq.TableMetadataToUpdate{ViewQuery: md.ViewQuery})
	return table, md, err
}

// stripPostgresSchema removes the Postgres schema from the specification namespace.
// BigQuery doesn't support periods in the table name, so we have to remove the schema. If
// people have multiple tables in different namespaces and switch the source schema
// then...  they're gonna have a bad time.
func stripPostgresSchema(ns changelog.Namespace) string {
	elements := strings.SplitN(string(ns), ".", 2)
	if len(elements) != 2 {
		panic(fmt.Sprintf("invalid Postgres schema.table_name string: %s", ns))
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
