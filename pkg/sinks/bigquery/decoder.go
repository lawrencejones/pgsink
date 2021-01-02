package bigquery

import (
	"fmt"

	bq "cloud.google.com/go/bigquery"
	"github.com/jackc/pgtype"
)

// fieldTypeFor maps Postgres OID types to BigQuery types, allowing us to build BigQuery
// schemas from Postgres type information.
func fieldTypeFor(oid uint32) (fieldType bq.FieldType, repeated bool, err error) {
	switch oid {
	case pgtype.BoolOID:
		return bq.BooleanFieldType, false, nil
	case pgtype.Int2OID, pgtype.Int4OID, pgtype.Int8OID:
		return bq.IntegerFieldType, false, nil
	case pgtype.Float4OID, pgtype.Float8OID:
		return bq.FloatFieldType, false, nil
	case pgtype.TimestampOID, pgtype.TimestamptzOID:
		return bq.TimestampFieldType, false, nil
	case pgtype.TextOID, pgtype.VarcharOID:
		return bq.StringFieldType, false, nil
	case pgtype.TextArrayOID, pgtype.VarcharArrayOID:
		return bq.StringFieldType, true, nil
	}

	return "", false, fmt.Errorf("no BigQuery field for Postgres oid %v", oid)
}
