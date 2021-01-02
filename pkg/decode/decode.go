package decode

import (
	"database/sql"
	"fmt"
	"reflect"

	"github.com/jackc/pgtype"
)

// Decoder stores the mapping from Postgres OID to scanner, which can parse a Golang type
// from the Postgres string, and the Golang type that those scanners will produce.
type Decoder interface {
	ScannerForOID(oid uint32) (scanner valueScanner, err error)
	EmptyForOID(oid uint32) (empty interface{}, err error)
	ExternalTypeForOID(oid uint32) (externalType interface{}, err error)
}

// Build is how we expect decoders to be built, and allows us to perform validations on
// the mappings.
var Build = decoderBuilderFunc(func(opts ...func(*decoder)) Decoder {
	d := &decoder{}
	for _, opt := range opts {
		opt(d)
	}

	// Check if we have any duplicate mappings assigned to the same Postgres OID, as this
	// would result in undefined behaviour
	{
		mappedOIDs := map[uint32]bool{}
		for _, mapping := range d.mappings {
			_, alreadyMapped := mappedOIDs[mapping.OID]
			if alreadyMapped {
				panic(fmt.Sprintf("duplicate mapping found for Postgres OID %v", mapping.OID))
			}
		}
	}

	return d
})

type decoderBuilderFunc func(opts ...func(*decoder)) Decoder

func (b decoderBuilderFunc) WithType(oid uint32, scanner valueScanner, empty interface{}, externalType interface{}) func(*decoder) {
	return func(d *decoder) {
		d.mappings = append(d.mappings, typeMapping{
			OID:          oid,
			Scanner:      scanner,
			Empty:        empty,
			ExternalType: externalType,
		})
	}
}

// valueScanner combines the pgx and sql interfaces to provide a useful API surface for a
// variety of pgsink operations.
type valueScanner interface {
	pgtype.Value
	sql.Scanner

	// Not strictly part of the Value and Scanner interfaces, but included in all our
	// supported types
	EncodeText(*pgtype.ConnInfo, []byte) ([]byte, error)
}

type decoder struct {
	mappings []typeMapping
}

type typeMapping struct {
	OID          uint32       // Postgres type OID
	Scanner      valueScanner // scanner for parsing type from database
	Empty        interface{}  // Golang empty type produced by the scanner
	ExternalType interface{}  // associated type for user of the decoder
}

// UnregisteredType is returned whenever we see a Postgres OID that has no associated type
// mapping. How we handle this depends on the caller.
type UnregisteredType struct {
	OID uint32
}

func (e *UnregisteredType) Error() string {
	return fmt.Sprintf("decoder has no type mapping for Postgres OID '%v'", e.OID)
}

func (d *decoder) ScannerForOID(oid uint32) (scanner valueScanner, err error) {
	for _, mapping := range d.mappings {
		if oid == mapping.OID {
			newScanner := reflect.New(reflect.TypeOf(mapping.Scanner).Elem()).Interface()
			return newScanner.(valueScanner), nil
		}
	}

	return nil, &UnregisteredType{oid}
}

func (d *decoder) EmptyForOID(oid uint32) (empty interface{}, err error) {
	for _, mapping := range d.mappings {
		if oid == mapping.OID {
			return mapping.Empty, nil
		}
	}

	return nil, &UnregisteredType{oid}
}

func (d *decoder) ExternalTypeForOID(oid uint32) (externalType interface{}, err error) {
	for _, mapping := range d.mappings {
		if oid == mapping.OID && mapping.ExternalType != nil {
			return mapping.ExternalType, nil
		}
	}

	return nil, &UnregisteredType{oid}
}
