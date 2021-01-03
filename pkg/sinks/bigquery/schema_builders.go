package bigquery

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"github.com/lawrencejones/pgsink/pkg/changelog"

	bq "cloud.google.com/go/bigquery"
	"github.com/alecthomas/template"
)

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
func buildRaw(tableName string, schema *changelog.Schema) (*bq.TableMetadata, error) {
	fields := bq.Schema{}
	for _, column := range schema.Spec.Columns {
		externalType, repeated, err := fieldTypeFor(column.Type)
		if err != nil {
			return nil, err
		}

		fieldSchema := &bq.FieldSchema{
			Name:     column.Name,
			Type:     externalType,
			Repeated: repeated,
			Required: false,
		}

		fields = append(fields, fieldSchema)
	}

	// Sort the schema columns just in case BigQuery is sensitive to column order
	sort.Slice(fields, func(i, j int) bool {
		return fields[i].Name < fields[j].Name
	})

	bqSchema := bq.Schema{
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
		Schema: bqSchema,
		TimePartitioning: &bq.TimePartitioning{
			Field: "timestamp",
		},
	}

	return md, nil
}

// buildView creates a BigQuery view that presents only the most recent row content in the
// raw table to the user. We expect the rawTableName to be in projectID:datasetID.tableID
// form.
func buildView(tableName, rawTableName string, schema *changelog.Schema) (*bq.TableMetadata, error) {
	keys := []string{}
	for _, column := range schema.Spec.Columns {
		if column.Key {
			keys = append(keys, column.Name)
		}
	}

	if len(keys) == 0 {
		return nil, fmt.Errorf("table %s has no detected primary key columns", tableName)
	}

	var buffer bytes.Buffer
	err := viewQueryTemplate.Execute(
		&buffer, struct {
			EscapedRawTableIdentifier string
			PrimaryKeyColumns         []string
		}{
			fmt.Sprintf("`%s`", strings.Replace(rawTableName, ":", ".", 1)),
			keys,
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

// TODO: Support composite primary keys
var viewQueryTemplate = template.Must(template.New("view_query_template").Parse(
	`select payload.*, from (
  select *, row_number() over (
    partition by
      {{ $select := "" }}
      {{ range $index, $column := .PrimaryKeyColumns }}
        {{ if $index}},{{end}}
        payload.{{ $column }}
      {{ end }}
    order by timestamp desc
  ) as row_number
  from {{.EscapedRawTableIdentifier}}
)
where row_number = 1
and operation != 'DELETE'
`))
