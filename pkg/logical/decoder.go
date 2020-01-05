package logical

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/jackc/pgx/pgtype"
)

// PGOutput is the Postgres recognised name of our desired encoding
const PGOutput = "pgoutput"

type ValueScanner interface {
	pgtype.Value
	sql.Scanner
}

// TypeForOID returns the pgtype for the given Postgres oid. This function defines the
// scope of type support for this project: if it doesn't appear here, your type will be
// exported in text.
//
// Any schema representation must support all these types.
func TypeForOID(oid uint32) ValueScanner {
	switch oid {
	case pgtype.BoolOID:
		return &pgtype.Bool{}
	case pgtype.Int2OID, pgtype.Int4OID:
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

// DecodePGOutput parses a pgoutput logical replication message, as per the format
// specification at:
//
// https://www.postgresql.org/docs/current/static/protocol-logicalrep-message-formats.html
func DecodePGOutput(src []byte) (interface{}, error) {
	dec := decoder{bytes.NewBuffer(src[1:])}

	switch src[0] {
	case 'B':
		m := &Begin{}
		m.LSN = dec.Uint64()
		m.Timestamp = dec.Time()
		m.XID = dec.Uint32()

		return m, nil

	case 'C':
		m := &Commit{}
		m.Flags = dec.Uint8()
		m.LSN = dec.Uint64()
		m.TransactionLSN = dec.Uint64()
		m.Timestamp = dec.Time()

		return m, nil

	case 'O':
		m := &Origin{}
		m.LSN = dec.Uint64()
		m.Name = dec.String()

		return m, nil

	case 'R':
		m := &Relation{}
		m.ID = dec.Uint32()
		m.Namespace = dec.String()
		m.Name = dec.String()
		m.ReplicaIdentity = dec.Uint8()
		m.Columns = []Column{}

		for i := dec.Uint16(); i > 0; i-- {
			c := Column{}
			c.Key = dec.Uint8() == 1
			c.Name = dec.String()
			c.Type = dec.Uint32()
			c.Modifier = dec.Uint32()

			m.Columns = append(m.Columns, c)
		}

		return m, nil

	case 'Y':
		m := &Type{}
		m.ID = dec.Uint32()
		m.Namespace = dec.String()
		m.Name = dec.String()

		return m, nil

	case 'I':
		m := &Insert{}
		m.ID = dec.Uint32()

		if dec.Uint8() != 'N' {
			return nil, fmt.Errorf("malformed insert message")
		}

		m.Row = dec.TupleData()

		return m, nil

	case 'U':
		m := &Update{}
		m.ID = dec.Uint32()

		// These flags are optional, so we have to attempt to find them and be prepared to
		// rollback if we've trepassed into the new tuple area.
		m.Key = dec.Try('K')
		m.Old = dec.Try('O')
		if m.Key || m.Old {
			m.OldRow = dec.TupleData()
		}

		// Expecting the new tuple value to be provided here
		if dec.Uint8() != 'N' {
			return nil, fmt.Errorf("malformed update message")
		}

		m.Row = dec.TupleData()

		return m, nil

	case 'D':
		m := Delete{}
		m.ID = dec.Uint32()

		switch dec.Uint8() {
		case 'K':
			m.Key = true
		case 'O':
			m.Old = true
		default:
			return nil, fmt.Errorf("malformed delete message")
		}

		m.OldRow = dec.TupleData()

		return m, nil
	}

	return Message{}, fmt.Errorf("decoding not implemented: %c", src[0])
}

// Message is a parsed pgoutput message received from the replication stream
type Message struct{}

type Begin struct {
	LSN       uint64    // The final LSN of the transaction.
	Timestamp time.Time // Commit timestamp of the transaction.
	XID       uint32    // Xid of the transaction.
}

type Commit struct {
	Flags          uint8     // Flags; currently unused (must be 0).
	LSN            uint64    // The LSN of the commit.
	TransactionLSN uint64    // The end LSN of the transaction.
	Timestamp      time.Time // Commit timestamp of the transaction.
}

type Origin struct {
	LSN  uint64 // The LSN of the commit on the origin server.
	Name string // Name of the origin.
}

// Relation would normally include a column count field, but given Go slices track their
// size it becomes unnecessary.
type Relation struct {
	ID              uint32   // ID of the relation.
	Namespace       string   // Namespace (empty string for pg_catalog).
	Name            string   // Relation name.
	ReplicaIdentity uint8    // Replica identity setting for the relation (same as relreplident in pg_class).
	Columns         []Column // Repeating message of column definitions.
}

func (r Relation) String() string {
	return fmt.Sprintf("%s.%s", r.Namespace, r.Name)
}

// Marshal converts a tuple into a dynamic Golang map type. Values are represented in Go
// native types.
func (r *Relation) Marshal(tuple []Element) map[string]interface{} {
	// This tuple doesn't match our relation, if the sizes aren't the same
	if len(tuple) != len(r.Columns) {
		return nil
	}

	row := map[string]interface{}{}
	for idx, column := range r.Columns {
		decoded, err := column.Decode(tuple[idx].Value)

		// TODO: Consider if panic is appropriate here
		if err != nil {
			panic(err)
		}

		row[column.Name] = decoded
	}

	return row
}

type Column struct {
	Key      bool   // Interpreted from flags, which are either 0 or 1 which marks the column as part of the key.
	Name     string // Name of the column.
	Type     uint32 // ID of the column's data type.
	Modifier uint32 // Type modifier of the column (atttypmod).
}

// Decode generates a native Go type from the textual pgoutput representation. This can be
// extended to support more types if necessary.
func (c Column) Decode(src []byte) (interface{}, error) {
	scanner := TypeForOID(c.Type)
	if err := scanner.Scan(src); err != nil {
		return nil, err
	}

	return scanner.Get(), nil
}

type Type struct {
	ID        uint32 // ID of the data type.
	Namespace string // Namespace (empty string for pg_catalog).
	Name      string // Name of data type.
}

type Insert struct {
	ID  uint32    // ID of the relation corresponding to the ID in the relation message.
	Row []Element // TupleData message part representing the contents of new tuple.
}

type Update struct {
	ID     uint32    // ID of the relation corresponding to the ID in the relation message.
	Key    bool      // True if the update changed data in any of the column(s) that are part of the REPLICA IDENTITY index.
	Old    bool      // True if populated the OldRow value.
	New    bool      // True if populated the Row value.
	OldRow []Element // Old value of this row, only present if Old or Key.
	Row    []Element // New contents of the tuple.
}

type Delete struct {
	ID     uint32    // ID of the relation corresponding to the ID in the relation message.
	Key    bool      // True if the update changed data in any of the column(s) that are part of the REPLICA IDENTITY index.
	Old    bool      // True if populated the OldRow value.
	OldRow []Element // Old value of this row.
}

type Element struct {
	Type  byte   // Either 'n' (NULL), 'u' (unchanged TOASTed value) or 't' (test formatted).
	Value []byte // Will only be populated if Type is 't'.
}

// decoder provides stateful methods that advance the given buffer, parsing the contents
// into native types. We're parsing content sent over a TCP stream so expecting network
// byte order (Big Endian). In future we should adjust this interface to collect errors as
// we go, enabling us to report properly instead of panic.
type decoder struct {
	*bytes.Buffer
}

// Try attempts to find the given byte, and will rewind if it's not present
func (d decoder) Try(b byte) bool {
	if d.Uint8() == b {
		return true
	}

	d.UnreadByte()
	return false
}

func (d decoder) Uint8() uint8 {
	return uint8(d.Next(1)[0])
}

func (d decoder) Uint16() uint16 {
	return binary.BigEndian.Uint16(d.Next(2))
}

func (d decoder) Uint32() uint32 {
	return binary.BigEndian.Uint32(d.Next(4))
}

func (d decoder) Uint64() uint64 {
	return binary.BigEndian.Uint64(d.Next(8))
}

func (d decoder) String() string {
	bytes, err := d.ReadBytes(0)
	if err != nil {
		panic(err) // for now
	}

	return string(bytes[:len(bytes)-1])
}

// Time parses a uint64 microseconds from Postgres epoch into a Go time.Time
func (d decoder) Time() time.Time {
	return time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).Add(
		time.Microsecond * time.Duration(int64(d.Uint64())),
	)
}

func (d decoder) TupleData() []Element {
	tuple := []Element{}
	for noOfColumns := d.Uint16(); noOfColumns > 0; noOfColumns-- {
		e := Element{}
		e.Type = d.Uint8()

		if e.Type == 't' {
			e.Value = d.Next(int(d.Uint32()))
		}

		tuple = append(tuple, e)
	}

	return tuple
}