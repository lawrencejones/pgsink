package types

import (
	"database/sql"

	"github.com/jackc/pgtype"
)

// Decoder stores the mapping from Postgres OID to scanner, which can parse a Golang type
// from the Postgres string, and the Golang type that those scanners will produce.
type Decoder interface {
	ForOID(oid uint32) (scanner ValueScanner, empty interface{}, externalType interface{})
	ScannerForOID(oid uint32) (scanner ValueScanner)
	ValueForOID(oid uint32) (empty interface{})
	ExternalTypeForOID(oid uint32) (externalType interface{})
}

// DecoderFunc wraps a single function that defines the mapping from
type DecoderFunc func(oid uint32) (scanner ValueScanner, empty interface{}, externalType interface{})

func (f DecoderFunc) ForOID(oid uint32) (scanner ValueScanner, empty interface{}, externalType interface{}) {
	return f(oid)
}

func (f DecoderFunc) ScannerForOID(oid uint32) ValueScanner {
	scanner, _, _ := f(oid)
	return scanner
}

func (f DecoderFunc) ValueForOID(oid uint32) interface{} {
	_, val, _ := f(oid)
	return val
}

func (f DecoderFunc) ExternalTypeForOID(oid uint32) interface{} {
	_, _, externalType := f(oid)
	return externalType
}

// ValueScanner combines the pgx and sql interfaces to provide a useful API surface for a
// variety of pgsink operations.
type ValueScanner interface {
	pgtype.Value
	sql.Scanner

	// Not strictly part of the Value and Scanner interfaces, but included in all our
	// supported types
	EncodeText(*pgtype.ConnInfo, []byte) ([]byte, error)
}
