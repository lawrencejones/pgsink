package generic

import (
	"context"

	"github.com/lawrencejones/pg2sink/pkg/changelog"

	kitlog "github.com/go-kit/kit/log"
)

// Syncer generalises responding to new schemas by providing an interface to idempotently
// update and configure the Sink, returning an Inserter that can be used to push new data
// for the schema. The returned namespace is the associated route for this inserter.
type Syncer interface {
	Sync(context.Context, kitlog.Logger, *changelog.Schema) (changelog.Namespace, Inserter, SyncOutcome, error)
}

type SyncOutcome string

const (
	SyncFailed SyncOutcome = "failed" // configuration failed
	SyncNoop               = "noop"   // nothing was changed, no action required
	SyncUpdate             = "update" // sink was updated, the returned inserter takes precedence
)

// SyncFunc is shorthand for creating a syncer from a function
type SyncFunc func(context.Context, kitlog.Logger, *changelog.Schema) (changelog.Namespace, Inserter, SyncOutcome, error)

type SyncAlwaysUpdateFunc func(context.Context, kitlog.Logger, *changelog.Schema) (changelog.Namespace, Inserter, error)

func (s SyncAlwaysUpdateFunc) Sync(ctx context.Context, logger kitlog.Logger, schema *changelog.Schema) (changelog.Namespace, Inserter, SyncOutcome, error) {
	ns, inserter, err := s(ctx, logger, schema)
	if err != nil {
		return ns, nil, SyncFailed, err
	}

	return ns, inserter, SyncUpdate, nil
}
