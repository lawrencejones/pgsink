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
	Before    interface{} `json:"before"`    // row before modification, if relevant
	After     interface{} `json:"after"`     // row after modification
}
