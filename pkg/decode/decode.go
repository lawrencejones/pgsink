// Implements decoding of Postgres types into Golang, using mappings that are generated
// from pgtype.
//
// The Decoder provides consistent scanners across pgsink components, ensuring the logical
// subscription decodes Postgres values into the same types as the import.
package decode

//go:generate go run cmd/generate_mappings.go -- gen/mappings/mappings.go

import (
	"fmt"
	"reflect"

	"github.com/jackc/pgtype"
)

type Decoder interface {
	ScannerFor(oid uint32) (scanner Scanner, dest interface{}, err error)
}

// UnregisteredType is returned whenever we see a Postgres OID that has no associated type
// mapping. How we handle this depends on the caller.
type UnregisteredType struct {
	OID uint32
}

func (e *UnregisteredType) Error() string {
	return fmt.Sprintf("decoder has no type mapping for Postgres OID '%v'", e.OID)
}

func NewDecoder(mappings []TypeMapping) Decoder {
	return &decoder{
		mappings: mappings,
	}
}

type decoder struct {
	mappings []TypeMapping // mappings, usually set from the auto-generated mappings
}

func (d *decoder) ScannerFor(oid uint32) (scanner Scanner, dest interface{}, err error) {
	for _, mapping := range d.mappings {
		if oid == mapping.OID {
			return mapping.NewScanner(), mapping.NewEmpty(), nil
		}
	}

	return nil, nil, &UnregisteredType{oid}
}

// TypeMapping binds a Postgres type, denoted by the name and oid, to a Golang type. It is
// used as a database scanner, but with a restricted interface that ensures Get()ing the
// scanned value can return only the type allowed by this mapping.
type TypeMapping struct {
	Name    string      // human recognisable type-name
	OID     uint32      // Postgres type OID
	Scanner Scanner     // scanner for parsing type from database
	Empty   interface{} // Golang empty type produced by the scanner
}

// NewScanner initialises a new scanner, using the mapping Scanner as a template
func (t TypeMapping) NewScanner() Scanner {
	return reflect.New(reflect.TypeOf(t.Scanner).Elem()).Interface().(Scanner)
}

// NewEmpty allocates a new destination for a given type-mapping. This can be used with
// the scanner's AssignTo method to construct results of the correct type.
//
// It will return a handle to the exact type of Empty. This means something like a string
// will be given as a *string, likewise with *[]string, etc.
func (t TypeMapping) NewEmpty() interface{} {
	return reflect.New(reflect.TypeOf(t.Empty)).Interface()
}

// Scanner defines what pgtypes must support to be included in the decoder. It is used to
// filter available types in the generation of mappings.
type Scanner interface {
	// Scan satisfies the sql.Scanner interface, allowing us to pass instances to sql.DB
	// method calls
	Scan(src interface{}) error

	// Value is the common interface to all the pgtype constructs
	pgtype.Value
}
