package decode

import (
	"time"

	"github.com/jackc/pgtype"
)

// Cached connection info, used to lookup Postgres OID type names
var ci *pgtype.ConnInfo

func init() {
	ci = pgtype.NewConnInfo()
}

// GetTypeName attempts to find the type name given the Postgres OID. Very inefficient,
// and intended to be used for debugging only.
func GetTypeName(oid uint32) string {
	dt, found := ci.DataTypeForOID(oid)
	if !found {
		return "unrecognised"
	}

	return dt.Name
}

// Mappings are set on each of the decoders, and exposed to allow tests to run against the
// mapping configuration.
var Mappings = []TypeMapping{
	// Primitive types
	{pgtype.BoolOID, &pgtype.Bool{}, true},
	{pgtype.Int8OID, &pgtype.Int8{}, int64(0)},
	{pgtype.Int2OID, &pgtype.Int2{}, int16(0)},
	{pgtype.Int4OID, &pgtype.Int4{}, int32(0)},
	{pgtype.Float4OID, &pgtype.Float4{}, float32(0.0)},
	{pgtype.Float8OID, &pgtype.Float8{}, float64(0.0)},
	{pgtype.TimestampOID, &pgtype.Timestamp{}, time.Time{}},
	{pgtype.TimestamptzOID, &pgtype.Timestamptz{}, time.Time{}},
	// Text types
	{pgtype.VarcharOID, &pgtype.Varchar{}, ""},
	{pgtype.TextOID, &pgtype.Text{}, ""},
	// Complex types
	{pgtype.TextArrayOID, &pgtype.TextArray{}, []string{}},
	{pgtype.VarcharArrayOID, &pgtype.VarcharArray{}, []string{}},
}

func EachMapping(do func(name string, mapping TypeMapping)) {
	for _, mapping := range Mappings {
		do(GetTypeName(mapping.OID), mapping)
	}
}
