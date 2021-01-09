package logical

import (
	"fmt"
	"sync"

	"github.com/davecgh/go-spew/spew"
	kitlog "github.com/go-kit/kit/log"
	"github.com/lawrencejones/pgsink/pkg/decode"
)

// BuildRegistry taps a stream of logically replicated messages, extracting the Relations
// and storing them in the returned registry.
func BuildRegistry(logger kitlog.Logger, decoder decode.Decoder, messages <-chan interface{}) (*Registry, <-chan interface{}) {
	registry := &Registry{
		decoder:   decoder,
		relations: map[uint32]*Relation{},
	}

	output := make(chan interface{})

	go func() {
		for msg := range messages {
			if relation, ok := msg.(*Relation); ok {
				logger.Log("event", "registry.schema.add", "relation_id", relation.ID, "relation_name", relation.Name)
				registry.Register(relation)
			}

			output <- msg
		}

		close(output)
	}()

	return registry, output
}

// Registry is a race-safe data structure that pins Relation messages against their
// Postgres OIDs. It can be used to marshal Modifications from committed messages.
type Registry struct {
	decoder   decode.Decoder
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
	defer r.RUnlock()

	return r.relations[oid]
}

// Marshal uses the schema information in the registry to marshal Golang native structures
// from logical messages.
func (r *Registry) Marshal(msg interface{}) (relation *Relation, before interface{}, after interface{}, err error) {
	switch cast := msg.(type) {
	case *Insert:
		relation = r.Get(cast.ID)
		after, err = relation.Marshal(r.decoder, cast.Row)

	case *Update:
		relation = r.Get(cast.ID)
		before, err = relation.Marshal(r.decoder, cast.OldRow)
		if err != nil {
			return
		}
		after, err = relation.Marshal(r.decoder, cast.Row)
		if err != nil {
			return
		}

	case *Delete:
		relation = r.Get(cast.ID)
		before, err = relation.Marshal(r.decoder, cast.OldRow)

	default:
		panic(fmt.Sprintf("invalid message type (not insert/update/delete): %s", spew.Sdump(msg)))
	}

	return
}
