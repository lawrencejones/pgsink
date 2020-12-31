package bigquery

import (
	"context"
	"fmt"
	"strings"

	"github.com/lawrencejones/pgsink/pkg/changelog"
	"github.com/lawrencejones/pgsink/pkg/sinks/generic"

	bq "cloud.google.com/go/bigquery"
	kitlog "github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"go.opencensus.io/trace"
)

type schemaHandler struct {
	dataset *bq.Dataset
}

func newSchemaHandler(dataset *bq.Dataset) *schemaHandler {
	return &schemaHandler{
		dataset: dataset,
	}
}

// Handle attempts to reconcile the incoming schema against the BigQuery table it tracks.
// If the schema is unchanged since it was last synced, we do nothing. Once synced, the
// table is available via the GetSyncedTable method.
func (d *schemaHandler) Handle(ctx context.Context, logger kitlog.Logger, schema *changelog.Schema) (generic.Inserter, generic.SchemaHandlerOutcome, error) {
	ctx, span := trace.StartSpan(ctx, "pkg/sinks/bigquery/schemaHandler.Handle")
	defer span.End()

	logger = kitlog.With(logger, "namespace", schema.Spec.Namespace)
	raw, rawMetadata, err := d.syncRawTable(ctx, logger, schema)
	if err != nil {
		return nil, generic.SchemaHandlerFailed, err
	}

	if _, _, err := d.syncViewTable(ctx, logger, schema, raw); err != nil {
		return nil, generic.SchemaHandlerFailed, err
	}

	return newTable(raw, rawMetadata, schema), generic.SchemaHandlerUpdate, nil
}

// syncRawTable creates or updates the raw changelog table that powers the most-recent row
// view. It is named the same as the Postgres table it represents, but with a _raw suffix.
func (d *schemaHandler) syncRawTable(ctx context.Context, logger kitlog.Logger, schema *changelog.Schema) (*bq.Table, *bq.TableMetadata, error) {
	tableName := fmt.Sprintf("%s_raw", stripPostgresSchema(schema.Spec.Namespace))
	table := d.dataset.Table(tableName)
	md, err := buildRaw(tableName, schema.Spec.Relation)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to build raw table metadata")
	}

	md, err = createOrUpdateTable(ctx, logger, table, md, bq.TableMetadataToUpdate{Schema: md.Schema})
	return table, md, err
}

// syncViewTable creates or updates the most-recent row state view, which depends on the
// raw changelog table.
func (d *schemaHandler) syncViewTable(ctx context.Context, logger kitlog.Logger, schema *changelog.Schema, raw *bq.Table) (*bq.Table, *bq.TableMetadata, error) {
	tableName := stripPostgresSchema(schema.Spec.Namespace)
	table := d.dataset.Table(tableName)
	md, err := buildView(tableName, raw.FullyQualifiedName(), schema.Spec.Relation)
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
