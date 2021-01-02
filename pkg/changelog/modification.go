package changelog

import (
	"time"
)

// Modification represents a row that was changed in the source database. The Before and
// After fields contain Golang native types that represent the row, both before and after
// this modification.
type Modification struct {
	Timestamp time.Time   `json:"timestamp"` // commit timestamp, or time of import
	Namespace string      `json:"namespace"` // Postgres schema
	Name      string      `json:"name"`      // Postgres table name
	LSN       *uint64     `json:"lsn"`       // log sequence number, where appropriate
	Before    interface{} `json:"before"`    // row before modification, if relevant
	After     interface{} `json:"after"`     // row after modification
}

// ModificationBuilder provides a fluent interface around constructing Modifications. This
// is used by tests to easily create fixtures.
var ModificationBuilder = modificationBuilderFunc(func(opts ...func(*Modification)) *Modification {
	m := &Modification{}
	for _, opt := range opts {
		opt(m)
	}

	return m
})

type modificationBuilderFunc func(opts ...func(*Modification)) *Modification

func (b modificationBuilderFunc) WithBase(base *Modification) func(*Modification) {
	return func(m *Modification) {
		*m = *base
	}
}

func (b modificationBuilderFunc) WithTimestampNow() func(*Modification) {
	return func(m *Modification) {
		m.Timestamp = time.Now()
	}
}

func (b modificationBuilderFunc) WithName(namespace, name string) func(*Modification) {
	return func(m *Modification) {
		m.Namespace = namespace
		m.Name = name
	}
}

func (b modificationBuilderFunc) WithLSN(lsn uint64) func(*Modification) {
	return func(m *Modification) {
		m.LSN = &lsn
	}
}

func (b modificationBuilderFunc) WithBefore(before map[string]interface{}) func(*Modification) {
	return func(m *Modification) {
		m.Before = before
	}
}

func (b modificationBuilderFunc) WithAfter(after map[string]interface{}) func(*Modification) {
	return func(m *Modification) {
		m.After = after
	}
}

func (b modificationBuilderFunc) WithUpdate(afterDiff map[string]interface{}) func(*Modification) {
	return func(m *Modification) {
		if m.After == nil {
			panic("cannot use WithUpdate without first configuring an After")
		}

		// What was the after, is now the before
		m.Before = m.After

		after := map[string]interface{}{}
		for key, value := range m.Before.(map[string]interface{}) {
			after[key] = value
		}

		for key, value := range afterDiff {
			after[key] = value
		}

		m.After = after
	}
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
