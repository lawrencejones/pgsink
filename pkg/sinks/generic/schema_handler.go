package generic

import (
	"context"
	"sync"

	"github.com/lawrencejones/pgsink/pkg/changelog"

	kitlog "github.com/go-kit/kit/log"
)

// SchemaHandler responds to new schemas by idempotently updating and configuring the Sink
// to receive corresponding modifications. It returns an Inserter that can be used to
// handle modification associated with the given schema.
type SchemaHandler interface {
	Handle(context.Context, kitlog.Logger, *changelog.Schema) (Inserter, SchemaHandlerOutcome, error)
}

type SchemaHandlerOutcome string

const (
	SchemaHandlerFailed SchemaHandlerOutcome = "failed" // configuration failed
	SchemaHandlerNoop                        = "noop"   // nothing was changed, no action required
	SchemaHandlerUpdate                      = "update" // sink was updated, the returned inserter takes precedence
)

// SchemaHandlerFunc is shorthand for creating a handler from a function
type SchemaHandlerFunc func(context.Context, kitlog.Logger, *changelog.Schema) (Inserter, SchemaHandlerOutcome, error)

func (s SchemaHandlerFunc) Handle(ctx context.Context, logger kitlog.Logger, schema *changelog.Schema) (Inserter, SchemaHandlerOutcome, error) {
	return s(ctx, logger, schema)
}

// SchemaHandlerGlobalInserter is used to register a single global inserter for all
// modifications to this sink, along with a handler function that is used to respond to
// new schemas but is not expected to return a modified inserter.
func SchemaHandlerGlobalInserter(inserter Inserter, schemaHandler func(context.Context, kitlog.Logger, *changelog.Schema) error) SchemaHandler {
	var hasRun bool
	return SchemaHandlerFunc(
		func(ctx context.Context, logger kitlog.Logger, schema *changelog.Schema) (Inserter, SchemaHandlerOutcome, error) {
			if err := schemaHandler(ctx, logger, schema); err != nil {
				return nil, SchemaHandlerFailed, err
			}

			// We only want to return SchemaHandlerUpdate once, as we never change the inserter
			var outcome SchemaHandlerOutcome = SchemaHandlerNoop
			if !hasRun {
				hasRun = true
				outcome = SchemaHandlerUpdate
			}

			return inserter, outcome, nil
		},
	)
}

// SchemaHandlerCacheOnFingerprint caches schema handler responses on the fingerprint of
// the received schema. This means any subsequent identical schemas are provided the old,
// cached version of the previous handler call.
func SchemaHandlerCacheOnFingerprint(handler SchemaHandler) SchemaHandler {
	return &schemaHandlerCached{
		handler: handler,
		cache:   map[string]Inserter{},
	}
}

type schemaHandlerCached struct {
	handler SchemaHandler
	cache   map[string]Inserter
	sync.Mutex
}

type fingerprintedInserter struct {
	inserter    Inserter
	fingerprint uint64
}

func (s *schemaHandlerCached) Handle(ctx context.Context, logger kitlog.Logger, schema *changelog.Schema) (Inserter, SchemaHandlerOutcome, error) {
	s.Lock()
	defer s.Unlock()

	logger = kitlog.With(logger, "schema", schema.String())
	fingerprint := schema.GetFingerprint()
	existing, ok := s.cache[fingerprint]

	if ok && existing != nil {
		logger.Log("event", "schema.already_fingerprinted", "fingerprint", fingerprint,
			"msg", "returning cached inserter from previous fingerprint")
		return existing, SchemaHandlerNoop, nil
	}

	logger.Log("event", "schema.new_fingerprint", "fingerprint", fingerprint,
		"msg", "fingerprint seen for the first time, calling schema handler")
	inserter, outcome, err := s.handler.Handle(ctx, logger, schema)

	// Cache the inserter. Callers need to be aware that we'll do this even if we fail to
	// handle the schema.
	s.cache[fingerprint] = inserter

	return inserter, outcome, err
}
