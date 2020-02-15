package bigquery

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"
	"text/template"

	"go.opencensus.io/trace"

	"google.golang.org/api/googleapi"

	kitlog "github.com/go-kit/kit/log"
	"github.com/lawrencejones/pg2sink/pkg/changelog"
	"github.com/lawrencejones/pg2sink/pkg/sinks"
	"github.com/pkg/errors"

	bq "cloud.google.com/go/bigquery"
)

var AvroBQTypeMap = map[string]bq.FieldType{
	"boolean": bq.BooleanFieldType,
	"int":     bq.IntegerFieldType,
	"long":    bq.IntegerFieldType,
	"float":   bq.FloatFieldType,
	"double":  bq.FloatFieldType,
	"string":  bq.StringFieldType,
}

func New(ctx context.Context, logger kitlog.Logger, opts Options) (sinks.Sink, error) {
	client, err := bq.NewClient(ctx, opts.ProjectID)
	if err != nil {
		return nil, err
	}

	kitlog.With(logger, "project", opts.ProjectID, "dataset", opts.Dataset, "location", opts.Location)

	dataset := client.Dataset(opts.Dataset)
	md, err := dataset.Metadata(ctx)
	if allowNotFound(err) != nil {
		return nil, err
	}

	if md == nil {
		logger.Log("event", "dataset.create", "msg", "dataset does not exist, creating")
		md = &bq.DatasetMetadata{
			Name:        opts.Dataset,
			Location:    opts.Location,
			Description: "Dataset created by pg2sink",
		}

		if err := dataset.Create(ctx, md); err != nil {
			return nil, err
		}
	}

	return &Sink{logger: logger, dataset: dataset, opts: opts}, nil
}

type Sink struct {
	logger  kitlog.Logger
	dataset *bq.Dataset
	opts    Options
}

type Options struct {
	ProjectID string
	Dataset   string
	Location  string
}

func (s *Sink) Consume(ctx context.Context, entries changelog.Changelog, ack sinks.AckCallback) error {
	ctx, span := trace.StartSpan(ctx, "pkg/sinks.BigQuery.Consume")
	defer span.End()

	specFingerprints := map[string]uint64{}
	rawSchemaCache := map[string]bq.Schema{}
	for envelope := range entries {
		switch entry := envelope.Unwrap().(type) {
		case *changelog.Schema:
			logger := kitlog.With(s.logger, "namespace", entry.Spec.Namespace)
			fingerprint := entry.Spec.GetFingerprint()
			if existingFingerprint := specFingerprints[entry.Spec.Namespace]; existingFingerprint > 0 {
				if existingFingerprint == fingerprint {
					logger.Log("event", "schema.already_fingerprinted", "fingerprint", existingFingerprint,
						"msg", "not updating BigQuery schema as fingerprint has not changed")
					continue
				}
			}

			logger.Log("event", "schema.new_fingerprint", "fingerprint", fingerprint,
				"msg", "fingerprint seen for the first time, syncing BigQuery schemas")
			raw, err := s.syncRawTable(ctx, entry)
			if err != nil {
				return err
			}

			rawSchemaCache[entry.Spec.Namespace] = raw.Schema
			if _, err := s.syncViewTable(ctx, entry, raw); err != nil {
				return err
			}
		case *changelog.Modification:
			rawSchema := rawSchemaCache[entry.Namespace]
			if rawSchema == nil {
				panic(fmt.Sprintf("no schema for %s, cannot proceed", entry.Namespace))
			}

			var payloadSchema bq.Schema
			for _, field := range rawSchema {
				if field.Name == "payload" {
					payloadSchema = field.Schema
					break
				}
			}

			// When deletion, we'll update this row with the contents at delete
			row := entry.AfterOrBefore()
			values := []bq.Value{}
			for _, field := range payloadSchema {
				values = append(values, row[field.Name])
			}

			table := s.dataset.Table(fmt.Sprintf("%s_raw", getTableName(entry.Namespace)))
			err := table.Inserter().Put(ctx, &bq.ValuesSaver{
				Schema: rawSchema,
				Row: []bq.Value{
					entry.Timestamp,
					entry.LSN,
					entry.Operation(),
					bq.Value(values),
				},
			})

			if err != nil {
				return errors.Wrapf(err, "failed to insert into table %s", table.FullyQualifiedName())
			}
		}
	}

	return nil
}

// syncRawTable creates or updates the raw changelog table that powers the most-recent row
// view. It is named the same as the Postgres table it represents, but with a _raw suffix.
func (s *Sink) syncRawTable(ctx context.Context, entry *changelog.Schema) (*bq.TableMetadata, error) {
	logger := kitlog.With(s.logger, "postgres_relation", entry.Spec.Namespace)

	tableName := fmt.Sprintf("%s_raw", getTableName(entry.Spec.Namespace))
	logger = kitlog.With(logger, "table", tableName)
	table := s.dataset.Table(tableName)
	existing, err := table.Metadata(ctx)
	if allowNotFound(err) != nil {
		return nil, err
	}

	md, err := s.buildRawTableMetadata(tableName, entry.Spec)
	if err != nil {
		return nil, errors.Wrap(err, "failed to build raw table metadata")
	}

	if existing == nil {
		logger.Log("event", "table.create", "msg", "table does not exist, creating")
		if err := table.Create(ctx, md); err != nil {
			return nil, errors.Wrap(err, "failed to create table")
		}
	}

	// Blind update (without etag) as all our updates need to be backward/forward
	// compatible, so it matters very little if someone has raced us.
	logger.Log("event", "table.update_schema", "msg", "updating table with new schema")
	md, err = table.Update(ctx, bq.TableMetadataToUpdate{Schema: md.Schema}, "")
	if err != nil {
		return nil, errors.Wrap(err, "failed to update table schema")
	}

	return md, nil
}

// buildRawMetadata generates a BigQuery schema from an avro-ish changelog entry. This schema
// is for the raw tables, those that contain each changelog entry. This table is what
// we'll query with our view to display only the most recent row.
//
// {
//    timestamp: "2020-02-15 19:33:32+00:00",
//    lsn: 0/19EC9B8,
//    payload: {
//      id: "PA123",
//      ...,
//    },
// }
func (s *Sink) buildRawTableMetadata(tableName string, spec changelog.SchemaSpecification) (*bq.TableMetadata, error) {
	fields := bq.Schema{}
	for _, field := range spec.Fields {
		bqType, ok := AvroBQTypeMap[field.GetType()]
		if !ok {
			return nil, fmt.Errorf("unsupported type %s for BigQuery", field.GetType())
		}

		fieldSchema := &bq.FieldSchema{
			Name:     field.Name,
			Type:     bqType,
			Required: false,
		}

		fields = append(fields, fieldSchema)
	}

	// Sort the schema columns just in case BigQuery is sensitive to column order
	sort.Slice(fields, func(i, j int) bool {
		return fields[i].Name < fields[j].Name
	})

	schema := bq.Schema{
		&bq.FieldSchema{
			Name:        "timestamp",
			Type:        bq.TimestampFieldType,
			Description: "Timestamp at which the row was read from database",
			Required:    true,
		},
		&bq.FieldSchema{
			Name:        "lsn",
			Type:        bq.IntegerFieldType,
			Description: "Database log sequence number at time of read, optional",
			Required:    false,
		},
		&bq.FieldSchema{
			Name:        "operation",
			Type:        bq.StringFieldType,
			Description: "Either IMPORT, INSERT, UPDATE or DELETE",
			Required:    true,
		},
		&bq.FieldSchema{
			Name:        "payload",
			Type:        bq.RecordFieldType,
			Description: "Contents of database row",
			Schema:      fields,
		},
	}

	md := &bq.TableMetadata{
		Name:   tableName,
		Schema: schema,
		TimePartitioning: &bq.TimePartitioning{
			Field: "timestamp",
		},
	}

	return md, nil
}

// syncViewTable creates or updates the most-recent row state view, which depends on the
// raw changelog table.
func (s *Sink) syncViewTable(ctx context.Context, entry *changelog.Schema, raw *bq.TableMetadata) (*bq.TableMetadata, error) {
	tableName := getTableName(entry.Spec.Namespace)
	logger := kitlog.With(s.logger, "raw_table", raw.Name, "table", tableName)

	table := s.dataset.Table(tableName)
	existing, err := table.Metadata(ctx)
	if allowNotFound(err) != nil {
		return nil, err
	}

	md, err := s.buildViewTableMetadata(tableName, raw)
	if err != nil {
		return nil, errors.Wrap(err, "failed to build view table metadata")
	}

	if existing == nil {
		logger.Log("event", "table.create", "msg", "table does not exist, creating")
		if err := table.Create(ctx, md); err != nil {
			return nil, errors.Wrap(err, "failed to create table")
		}
	}

	logger.Log("event", "table.update_view_query", "msg", "updating view table with query")
	md, err = table.Update(ctx, bq.TableMetadataToUpdate{ViewQuery: md.ViewQuery}, "")
	if err != nil {
		return nil, errors.Wrap(err, "failed to update table view query")
	}

	return md, nil
}

// TODO: Support arbitrary primary key column names
var viewQueryTemplate = template.Must(template.New("view_query_template").Parse(`
select payload.*, from (
	select *, row_number() over (partition by payload.id order by timestamp desc) as row_number
	from {{.EscapedRawTableIdentifier}}
)
where row_number = 1
and operation != 'DELETE'
`))

func (s *Sink) buildViewTableMetadata(tableName string, raw *bq.TableMetadata) (*bq.TableMetadata, error) {
	var buffer bytes.Buffer
	err := viewQueryTemplate.Execute(
		&buffer, struct{ EscapedRawTableIdentifier string }{
			fmt.Sprintf("`%s`", strings.Replace(raw.FullID, ":", ".", 1)),
		},
	)

	if err != nil {
		return nil, err
	}

	md := &bq.TableMetadata{
		Name:      tableName,
		ViewQuery: buffer.String(),
		Schema:    nil, // we don't use schema for a view
	}

	return md, nil
}

// getTableName removes the Postgres schema from the specification namespace. BigQuery
// doesn't support periods in the table name, so we have to remove the schema. If people
// have multiple tables in different namespaces and switch the source schema then...
// they're gonna have a bad time.
func getTableName(schemaNamespace string) string {
	elements := strings.SplitN(schemaNamespace, ".", 2)
	if len(elements) != 2 {
		panic(fmt.Sprintf("invalid Postgres schema.table_name string: %s", schemaNamespace))
	}

	return elements[1]
}

func allowNotFound(err error) error {
	if err, ok := err.(*googleapi.Error); ok && err.Code == 404 {
		return nil
	}

	return err
}
