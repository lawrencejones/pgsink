package decode

//go:generate go run cmd/generate_mappings.go -- gen/mappings/mappings.go

import (
	"database/sql"
	"fmt"
	"reflect"

	"github.com/jackc/pgtype"
)

func NewDecoder(mappings []TypeMapping) Decoder {
	return &decoder{
		mappings: mappings,
	}
}

type Decoder interface {
	TypeMappingForOID(oid uint32) (mapping *TypeMapping, err error)
	ScannerForOID(oid uint32) (scanner ValueScanner, err error)
	EmptyForOID(oid uint32) (empty interface{}, err error)
}

type decoder struct {
	mappings []TypeMapping // mappings, usually set from the auto-generated mappings
}

// ValueScanner combines the pgx and sql interfaces to provide a useful API surface for a
// variety of pgsink operations. We only generate mappings for OIDs that support this
// interface, as importing currently relies on the EncodeText method.
//
// TODO: Remove dependency on EncodeText, or limit failures to situations where the
// primary key of a table doesn't support EncodeText.
type ValueScanner interface {
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

func (d decoder) TypeMappingForOID(oid uint32) (mapping *TypeMapping, err error) {
	for _, mapping := range d.mappings {
		if oid == mapping.OID {
			return mapping.Clone(), nil
		}
	}

	return nil, &UnregisteredType{oid}
}

func (d decoder) ScannerForOID(oid uint32) (scanner ValueScanner, err error) {
	for _, mapping := range d.mappings {
		if oid == mapping.OID {
			return mapping.NewScanner(), nil
		}
	}

	return nil, &UnregisteredType{oid}
}

func (d decoder) EmptyForOID(oid uint32) (empty interface{}, err error) {
	for _, mapping := range d.mappings {
		if oid == mapping.OID {
			return mapping.NewEmpty(), nil
		}
	}

	return nil, &UnregisteredType{oid}
}

type TypeMapping struct {
	Name    string       // human recognisable type-name
	OID     uint32       // Postgres type OID
	Scanner ValueScanner // scanner for parsing type from database
	Empty   interface{}  // Golang empty type produced by the scanner
}

// Clone produces a new copy of the TypeMapping, which is essential, given the scanners
// are stateful.
func (t TypeMapping) Clone() *TypeMapping {
	return &TypeMapping{
		Name:    t.Name,
		OID:     t.OID,
		Scanner: t.NewScanner(),
		Empty:   t.NewEmpty(),
	}
}

// NewScanner initialises a new scanner, using the mapping Scanner as a template
func (t TypeMapping) NewScanner() ValueScanner {
	return reflect.New(reflect.TypeOf(t.Scanner).Elem()).Interface().(ValueScanner)
}

// NewEmpty allocates a new destination for a given type-mapping. This can be used with
// the scanner's AssignTo method to construct results of the correct type.
//
// It will return a handle to the exact type of Empty. This means something like a string
// will be given as a *string, likewise with *[]string, etc.
func (t TypeMapping) NewEmpty() interface{} {
	return reflect.New(reflect.TypeOf(t.Empty).Elem()).Interface()
}
