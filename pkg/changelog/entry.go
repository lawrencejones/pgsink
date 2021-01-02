package changelog

// Changelog is the goal of pgsink, a channel of schema and modification messages. Any
// source of these changes, be it a logical replication subscription or an import job,
// must produce this channel.
type Changelog chan Entry

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
