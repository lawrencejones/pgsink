package bigquery

import (
	"time"

	bq "cloud.google.com/go/bigquery"
	"github.com/jackc/pgtype"
	"github.com/lawrencejones/pgsink/pkg/types"
)

// Decoder configures the BigQuery type mappings, from Postgres OID, to the scanner used
// to parse the Golang type, to the BQ field type.
var Decoder = types.DecoderFunc(func(oid uint32) (scanner types.ValueScanner, empty interface{}, externalType interface{}) {
	switch oid {
	case pgtype.BoolOID:
		return &pgtype.Bool{}, true, bq.BooleanFieldType
	case pgtype.Int2OID:
		return &pgtype.Int2{}, int16(0), bq.IntegerFieldType
	case pgtype.Int4OID:
		return &pgtype.Int4{}, int32(0), bq.IntegerFieldType
	case pgtype.Int8OID:
		return &pgtype.Int8{}, int64(0), bq.IntegerFieldType
	case pgtype.Float4OID:
		return &pgtype.Float4{}, float32(0.0), bq.FloatFieldType
	case pgtype.Float8OID:
		return &pgtype.Float8{}, float64(0.0), bq.FloatFieldType
	case pgtype.TimestamptzOID:
		return &pgtype.Timestamptz{}, time.Time{}, bq.TimestampFieldType
	}

	return &pgtype.Text{}, "", bq.StringFieldType
})
