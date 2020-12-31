package logical

import "github.com/jackc/pgtype"

// TypeForOID returns the pgtype for the given Postgres oid. This function defines the
// scope of type support for this project: if it doesn't appear here, your type will be
// exported in text, and probably not the way you want it!
//
// When Relations are used to marshal Tuples, we'll produce Golang maps with values parsed
// using these scanners. It is the reponsibility of the sink or whatever caller to make
// sure they handle all the types this function might produce.
func TypeForOID(oid uint32) ValueScanner {
	switch oid {
	case pgtype.BoolOID:
		return &pgtype.Bool{}
	case pgtype.Int2OID:
		return &pgtype.Int2{}
	case pgtype.Int4OID:
		return &pgtype.Int4{}
	case pgtype.Int8OID:
		return &pgtype.Int8{}
	case pgtype.Float4OID:
		return &pgtype.Float4{}
	case pgtype.Float8OID:
		return &pgtype.Float8{}
	default:
		return &pgtype.Text{}
	}
}
