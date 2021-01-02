package bigquery

import (
	"time"

	bq "cloud.google.com/go/bigquery"
	"github.com/jackc/pgtype"
	"github.com/lawrencejones/pgsink/pkg/decode"
)

// Decoder configures the BigQuery type mappings, from Postgres OID, to the scanner used
// to parse the Golang type, to the BQ field type.
var Decoder = decode.Build(
	decode.Build.WithType(pgtype.BoolOID, &pgtype.Bool{}, true, bq.BooleanFieldType),
	decode.Build.WithType(pgtype.Int2OID, &pgtype.Int2{}, int16(0), bq.IntegerFieldType),
	decode.Build.WithType(pgtype.Int4OID, &pgtype.Int4{}, int32(0), bq.IntegerFieldType),
	decode.Build.WithType(pgtype.Int8OID, &pgtype.Int8{}, int64(0), bq.IntegerFieldType),
	decode.Build.WithType(pgtype.Float4OID, &pgtype.Float4{}, float32(0.0), bq.FloatFieldType),
	decode.Build.WithType(pgtype.Float8OID, &pgtype.Float8{}, float64(0.0), bq.FloatFieldType),
	decode.Build.WithType(pgtype.TimestampOID, &pgtype.Timestamp{}, time.Time{}, bq.TimestampFieldType),
	decode.Build.WithType(pgtype.TimestamptzOID, &pgtype.Timestamptz{}, time.Time{}, bq.TimestampFieldType),
)
