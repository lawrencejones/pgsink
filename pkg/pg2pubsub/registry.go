package pg2pubsub

import (
	"sync"

	kitlog "github.com/go-kit/kit/log"
)

// BuildRegistry taps a stream of logically replicated commits, extracting the Relation
// messages. Any relations are added to the relation registry, which is returned from this
// function, and pushed down a Schema channel for later publishing.
func BuildRegistry(logger kitlog.Logger, commits <-chan Committed) (*Registry, <-chan Schema, <-chan Committed) {
	registry := &Registry{relations: map[uint32]*Relation{}}
	schemas, output := make(chan Schema), make(chan Committed)

	go func() {
		for committed := range commits {
			if entry, ok := committed.Entry.(*Relation); ok {
				logger.Log("event", "adding_relation", "id", entry.ID, "name", entry.Name)
				registry.Register(entry)

				schemas <- Schema{Timestamp: committed.Timestamp, Spec: buildSchemaSpecification(entry)}
			} else {
				output <- committed
			}
		}

		close(output)
	}()

	return registry, schemas, output
}

// Registry is a race-safe data structure that pins Relation messages against their
// Postgres OIDs. It can be used to marshal Modifications from committed messages.
type Registry struct {
	relations map[uint32]*Relation
	sync.RWMutex
}

func (r *Registry) Register(relation *Relation) {
	r.Lock()
	defer r.Unlock()

	r.relations[relation.ID] = relation
}

func (r *Registry) Get(oid uint32) *Relation {
	r.RLock()
	relation := r.relations[oid]
	r.RUnlock() // don't defer, as defer costs more than direct invocation
	return relation
}

// Marshal uses the schema information in the registry to.
func (r *Registry) Marshal(committed Committed) Modification {
	modification := Modification{
		Timestamp: committed.Timestamp,
		LSN:       committed.LSN,
		Sequence:  committed.Sequence,
	}

	var relation *Relation

	switch cast := committed.Entry.(type) {
	case *Insert:
		relation = r.Get(cast.ID)
		modification.Before = relation.Marshal(cast.Row)
	case *Update:
		relation = r.Get(cast.ID)
		modification.Before, modification.After = relation.Marshal(cast.OldRow), relation.Marshal(cast.Row)
	case *Delete:
		relation = r.Get(cast.ID)
		modification.Before = relation.Marshal(cast.OldRow)
	default:
		panic("invalid message type")
	}

	modification.Namespace = relation.Namespace
	modification.Name = relation.Name

	return modification
}
