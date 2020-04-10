package generic

import (
	"context"
	"sync"

	kitlog "github.com/go-kit/kit/log"
	"github.com/lawrencejones/pg2sink/pkg/changelog"
)

type Router interface {
	// Register notifies the router that all subsequent insertions for the given namespace
	// should be routed to a new inserter. Register returns an InsertResult from flushing
	// any inserters that were previously routed via this namespace. Any previous inserters
	// flush is added to the routers in-flight result, preserving the semantics of flush to
	// ensure we appropriately handle failed flushes.
	Register(context.Context, changelog.Namespace, AsyncInserter) InsertResult

	// Otherwise, a router looks exactly like a normal AsyncInserter. It should be
	// transparent that each insert is routed to other inserters, and it should be possible
	// to compose this with other inserter constructs.
	AsyncInserter
}

type router struct {
	logger kitlog.Logger
	routes map[changelog.Namespace]AsyncInserter
	result InsertResult
	sync.RWMutex
}

func NewRouter(logger kitlog.Logger) Router {
	return &router{
		logger: logger,
		routes: map[changelog.Namespace]AsyncInserter{},
		result: EmptyInsertResult,
	}
}

func (r *router) Register(ctx context.Context, ns changelog.Namespace, i AsyncInserter) InsertResult {
	r.Lock()
	defer r.Unlock()

	// By default, we'll return an empty insertion result. This is what you'll get if a
	// route was never registered.
	flushResult := EmptyInsertResult

	logger := kitlog.With(r.logger, "namespace", ns)
	existing, ok := r.routes[ns]
	if ok && existing != nil {
		logger.Log("event", "existing_route.flush", "msg", "inserter already registered, flushing")
		flushResult = existing.Flush(ctx)

		// Ensure we add the flush result to our current buffered result, so any subsequent
		// flushes on the router can succeed only if the flush has also been successful.
		// Failing to do this means that the router Flush() might return an LSN that is
		// greater than those we have tried flushing, causing us to break the AsyncInserter
		// contract and potentially drop changes.
		r.result = r.result.Fold(flushResult)
	}

	logger.Log("event", "route.register", "msg", "registering inserter for namespace")
	r.routes[ns] = i

	return flushResult
}

// Insert splits modifications into the inserters, folding each async result into a single
// return value.
//
// TODO: This method follows the inserter interface, but it's not optimised for
// performance. At present, sinks will usually provide a single modification at a time,
// which means we pay the overhead for reslicing our modifications several times. Once
// everything is working, it's worth checking the performance penalty of implementing the
// insert like this, rather than an InsertSingle method.
func (r *router) Insert(ctx context.Context, modifications []*changelog.Modification) InsertResult {
	r.RLock()
	defer r.RUnlock()

	result := EmptyInsertResult
	for _, modification := range modifications {
		inserter, ok := r.routes[modification.Namespace]
		if !ok || inserter == nil {
			panic("asked to route modification before namespace was registered")
		}

		if insertResult := inserter.Insert(ctx, []*changelog.Modification{modification}); insertResult != nil {
			result = result.Fold(insertResult)
		}
	}

	return result
}

// Flush triggers a flush on all routed inserters. It returns a result that resolves only
// once all inserter routes have completed, and fails if any error. Any pending insertion
// from flushing old routes (triggered by Register) will also be included in this result.
func (r *router) Flush(ctx context.Context) InsertResult {
	r.Lock()
	defer r.Unlock()

	result := r.result
	r.result = EmptyInsertResult
	for _, inserter := range r.routes {
		result = result.Fold(inserter.Flush(ctx))
	}

	return result
}
