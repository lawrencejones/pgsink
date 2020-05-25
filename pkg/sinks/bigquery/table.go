package bigquery

import (
	"context"

	bq "cloud.google.com/go/bigquery"
	"github.com/lawrencejones/pgsink/pkg/changelog"
)

// Table is a cached BigQuery table entry, associated with a schema. This is the table we
// insert modifications into.
type Table struct {
	Raw         *bq.Table
	RawMetadata *bq.TableMetadata
	Schema      *changelog.Schema
}

func newTable(raw *bq.Table, rawMetadata *bq.TableMetadata, schema *changelog.Schema) *Table {
	return &Table{
		Raw:         raw,
		RawMetadata: rawMetadata,
		Schema:      schema,
	}
}

// Insert bulk inserts changelog modifications into a BigQuery table. It returns the last
// non-nil LSN from the batch of modifications.
func (i *Table) Insert(ctx context.Context, modifications []*changelog.Modification) (int, *uint64, error) {
	var payloadSchema bq.Schema
	for _, field := range i.RawMetadata.Schema {
		if field.Name == "payload" {
			payloadSchema = field.Schema
			break
		}
	}

	var highestLSN *uint64
	rows := make([]*bq.ValuesSaver, len(modifications))
	for idx, modification := range modifications {
		// When deletion, we'll update this row with the contents at delete
		payload := modification.AfterOrBefore()
		payloadValues := make([]bq.Value, len(payloadSchema))
		for idx, field := range payloadSchema {
			payloadValues[idx] = payload[field.Name]
		}

		rows[idx] = &bq.ValuesSaver{
			Schema: i.RawMetadata.Schema,
			Row: []bq.Value{
				modification.Timestamp,
				modification.LSN,
				modification.Operation(),
				bq.Value(payloadValues),
			},
		}

		if modification.LSN != nil {
			highestLSN = modification.LSN
		}
	}

	return len(rows), highestLSN, i.Raw.Inserter().Put(ctx, rows)
}
