package changelog

import (
	"time"
)

// Modification represents a row that was changed in the source database. The Before and
// After fields contain Golang native types that represent the row, both before and after
// this modification.
type Modification struct {
	Timestamp time.Time   `json:"timestamp"` // commit timestamp, or time of import
	Namespace string      `json:"namespace"` // <schema>.<table>
	LSN       *uint64     `json:"lsn"`       // log sequence number, where appropriate
	Before    interface{} `json:"before"`    // row before modification, if relevant
	After     interface{} `json:"after"`     // row after modification
}

const (
	ModificationOperationImport = "IMPORT"
	ModificationOperationInsert = "INSERT"
	ModificationOperationUpdate = "UPDATE"
	ModificationOperationDelete = "DELETE"
)

// Operation infers the type of operation that generated this entry. It may become
// expensive to compute these on the fly each time, at which point we should make it a
// struct field on Modification.
func (m Modification) Operation() string {
	if m.LSN == nil {
		return ModificationOperationImport
	} else if m.Before == nil {
		return ModificationOperationInsert
	} else if m.After == nil {
		return ModificationOperationDelete
	} else {
		return ModificationOperationUpdate
	}
}

// AfterOrBefore provides the last existing row contents in the database. If this is a
// deletion, we'll get the contents of the row before the deletion, otherwise the after.
func (m Modification) AfterOrBefore() map[string]interface{} {
	if m.After != nil {
		return m.After.(map[string]interface{})
	}

	return m.Before.(map[string]interface{})
}
