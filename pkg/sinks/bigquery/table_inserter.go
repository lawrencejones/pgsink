package bigquery

import (
	"context"

	bq "cloud.google.com/go/bigquery"
	"github.com/lawrencejones/pg2sink/pkg/changelog"
)

type tableInserter struct {
	Table *Table
}

// Insert bulk inserts changelog modifications into a BigQuery table. It returns the last
// non-nil LSN from the batch of modifications.
func (i *tableInserter) Insert(ctx context.Context, modifications []*changelog.Modification) (int, *uint64, error) {
	var payloadSchema bq.Schema
	for _, field := range i.Table.RawMetadata.Schema {
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
			Schema: i.Table.RawMetadata.Schema,
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

	return len(rows), highestLSN, i.Table.Raw.Inserter().Put(ctx, rows)
}
