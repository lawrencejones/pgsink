package changelog

import "fmt"

// Changelog is the goal of pgsink, a channel of schema and modification messages. Any
// source of these changes, be it a logical replication subscription or an import job,
// must produce this channel.
type Changelog chan Entry

// Namespace makes it clear when a string represents a <schema>.<table-name> reference to
// a Postgres table.
type Namespace string

func BuildNamespace(schema, name string) Namespace {
	return Namespace(fmt.Sprintf("%s.%s", schema, name))
}

// Entry is a poor-mans sum type for generic changelog entries
type Entry struct {
	*Modification
	*Schema
}

func (e Entry) Unwrap() interface{} {
	if e.Modification != nil {
		return e.Modification
	} else if e.Schema != nil {
		return e.Schema
	}

	panic("entry has no content")
}
