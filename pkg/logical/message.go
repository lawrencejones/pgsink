package logical

import "time"

// Message is a parsed pgoutput message received from the replication stream
type Message interface{}

type (
	Begin struct {
		LSN       uint64    // The final LSN of the transaction.
		Timestamp time.Time // Commit timestamp of the transaction.
		XID       uint32    // Xid of the transaction.
	}

	Commit struct {
		Flags          uint8     // Flags; currently unused (must be 0).
		LSN            uint64    // The LSN of the commit.
		TransactionLSN uint64    // The end LSN of the transaction.
		Timestamp      time.Time // Commit timestamp of the transaction.
	}

	Origin struct {
		LSN  uint64 // The LSN of the commit on the origin server.
		Name string // Name of the origin.
	}

	// Relation would normally include a column count field, but given Go slices track their
	// size it becomes unnecessary.
	Relation struct {
		ID              uint32   `json:"id"`               // ID of the relation.
		Namespace       string   `json:"namespace"`        // Namespace (empty string for pg_catalog).
		Name            string   `json:"name"`             // Relation name.
		ReplicaIdentity uint8    `json:"replica_identity"` // Replica identity setting for the relation (same as relreplident in pg_class).
		Columns         []Column `json:"columns"`          // Repeating message of column definitions.
	}

	Column struct {
		Key      bool   `json:"key"`      // Interpreted from flags, which are either 0 or 1 which marks the column as part of the key.
		Name     string `json:"name"`     // Name of the column.
		Type     uint32 `json:"type"`     // ID of the column's data type.
		Modifier uint32 `json:"modifier"` // Type modifier of the column (atttypmod).
	}

	Type struct {
		ID        uint32 // ID of the data type.
		Namespace string // Namespace (empty string for pg_catalog).
		Name      string // Name of data type.
	}

	Insert struct {
		ID  uint32    // ID of the relation corresponding to the ID in the relation message.
		Row []Element // TupleData message part representing the contents of new tuple.
	}

	Update struct {
		ID     uint32    // ID of the relation corresponding to the ID in the relation message.
		Key    bool      // True if the update changed data in any of the column(s) that are part of the REPLICA IDENTITY index.
		Old    bool      // True if populated the OldRow value.
		New    bool      // True if populated the Row value.
		OldRow []Element // Old value of this row, only present if Old or Key.
		Row    []Element // New contents of the tuple.
	}

	Delete struct {
		ID     uint32    // ID of the relation corresponding to the ID in the relation message.
		Key    bool      // True if the update changed data in any of the column(s) that are part of the REPLICA IDENTITY index.
		Old    bool      // True if populated the OldRow value.
		OldRow []Element // Old value of this row.
	}

	Element struct {
		Type  byte   // Either 'n' (NULL), 'u' (unchanged TOASTed value) or 't' (test formatted).
		Value []byte // Will only be populated if Type is 't'.
	}
)
