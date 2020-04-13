package generic

import (
	"context"

	"github.com/lawrencejones/pg2sink/pkg/changelog"

	kitlog "github.com/go-kit/kit/log"
)

// SchemaHandler responds to new schemas by idempotently updating and configuring the Sink
// to receive corresponding modifications. It returns an Inserter that can be used to
// handle modification associated with the given schema, along with the
// changelog.Namespace that should be configured to route to this schema in the Router.
type SchemaHandler interface {
	Handle(context.Context, kitlog.Logger, *changelog.Schema) (changelog.Namespace, Inserter, SchemaHandlerOutcome, error)
}

type SchemaHandlerOutcome string

const (
	SchemaHandlerFailed SchemaHandlerOutcome = "failed" // configuration failed
	SchemaHandlerNoop                        = "noop"   // nothing was changed, no action required
	SchemaHandlerUpdate                      = "update" // sink was updated, the returned inserter takes precedence
)

// SchemaHandlerFunc is shorthand for creating a handler from a function
type SchemaHandlerFunc func(context.Context, kitlog.Logger, *changelog.Schema) (changelog.Namespace, Inserter, SchemaHandlerOutcome, error)

func (s SchemaHandlerFunc) Handle(ctx context.Context, logger kitlog.Logger, schema *changelog.Schema) (changelog.Namespace, Inserter, SchemaHandlerOutcome, error) {
	return s(ctx, logger, schema)
}

// SchemaHandlerGlobalInserter is used to register a single global inserter for all
// modifications to this sink, along with a handler function that is used to respond to
// new schemas but is not expected to return a modified inserter.
func SchemaHandlerGlobalInserter(inserter Inserter, schemaHandler func(context.Context, kitlog.Logger, *changelog.Schema) error) SchemaHandler {
	var hasRun bool
	return SchemaHandlerFunc(
		func(ctx context.Context, logger kitlog.Logger, schema *changelog.Schema) (changelog.Namespace, Inserter, SchemaHandlerOutcome, error) {
			if err := schemaHandler(ctx, logger, schema); err != nil {
				return "", nil, SchemaHandlerFailed, err
			}

			// We only want to return SchemaHandlerUpdate once, as we never change the inserter
			var outcome SchemaHandlerOutcome = SchemaHandlerNoop
			if !hasRun {
				hasRun = true
				outcome = SchemaHandlerUpdate
			}

			return RouterMatchAll, inserter, outcome, nil
		},
	)
}
