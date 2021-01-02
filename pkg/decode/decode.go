package decode

import (
	"database/sql"
	"fmt"
	"reflect"

	"github.com/jackc/pgtype"
)

func NewDecoder(fallback *TypeMapping) Decoder {
	return Decoder{
		fallback: fallback,
		mappings: Mappings,
	}
}

type Decoder struct {
	fallback *TypeMapping  // if set, will be the fallback mapping for unrecognised types
	mappings []TypeMapping // list of all available type mappings
}

type TypeMapping struct {
	OID     uint32       // Postgres type OID
	Scanner valueScanner // scanner for parsing type from database
	Empty   interface{}  // Golang empty type produced by the scanner
}

// NewScanner initialises a new scanner, using the mapping Scanner as a template
func (t TypeMapping) NewScanner() valueScanner {
	return reflect.New(reflect.TypeOf(t.Scanner).Elem()).Interface().(valueScanner)
}

// TextFallback should be used as a fallback type, when we want pgsink to coerce
// unrecognised types to text when pushing them into the sink. Providing this when
// constructing the decoder prevents us from rejecting tables with unknown types, but
// comes at the cost of a column being represented in a potentially strange way.
var TextFallback = &TypeMapping{OID: pgtype.TextOID, Scanner: &pgtype.Text{}}

// valueScanner combines the pgx and sql interfaces to provide a useful API surface for a
// variety of pgsink operations.
type valueScanner interface {
	pgtype.Value
	sql.Scanner

	// Not strictly part of the Value and Scanner interfaces, but included in all our
	// supported types
	EncodeText(*pgtype.ConnInfo, []byte) ([]byte, error)
}

// UnregisteredType is returned whenever we see a Postgres OID that has no associated type
// mapping. How we handle this depends on the caller.
type UnregisteredType struct {
	OID uint32
}

func (e *UnregisteredType) Error() string {
	return fmt.Sprintf("decoder has no type mapping for Postgres OID '%v'", e.OID)
}

func (d Decoder) ScannerForOID(oid uint32) (scanner valueScanner, err error) {
	for _, mapping := range d.mappings {
		if oid == mapping.OID {
			return mapping.NewScanner(), nil
		}
	}

	if d.fallback != nil {
		return d.fallback.NewScanner(), nil
	}

	return nil, &UnregisteredType{oid}
}

func (d Decoder) EmptyForOID(oid uint32) (empty interface{}, err error) {
	for _, mapping := range d.mappings {
		if oid == mapping.OID {
			return mapping.Empty, nil
		}
	}

	return nil, &UnregisteredType{oid}
}
