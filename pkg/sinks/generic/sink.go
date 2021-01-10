package generic

import (
	"context"
	"time"

	"github.com/lawrencejones/pgsink/internal/telem"
	"github.com/lawrencejones/pgsink/pkg/changelog"

	kitlog "github.com/go-kit/kit/log"
	"github.com/oklog/run"
)

// AckCallback will acknowledge successful publication of up-to this message. It is not
// guaranteed to be called for any intermediate messages.
type AckCallback func(changelog.Entry)

// Sink is a generic sink destination for a changelog. It will consume entries until
// either an error, or the entries run out.
//
// If the process producing the changelog is long-running, then the AckCallback is used to
// acknowledge successfully writes into the sync. If you to wait for all writes to be
// completely processed to the sync, then wait for Consume to return.
type Sink interface {
	Consume(context.Context, changelog.Changelog, AckCallback) error
}

// SinkBuilder allows sink implementations to easily compose the sink-specific
// implementations into a generic sink implementation that fulfils the Sink contract.
var SinkBuilder = sinkBuilderFunc(func(opts ...func(*sink)) Sink {
	s := &sink{
		builders: []func(AsyncInserter) AsyncInserter{},
		router:   NewRouter(),
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
})

type sinkBuilderFunc func(opts ...func(*sink)) Sink

func (b sinkBuilderFunc) WithSchemaHandler(schemaHandler SchemaHandler) func(*sink) {
	return func(s *sink) {
		s.schemaHandler = schemaHandler
	}
}

func (b sinkBuilderFunc) WithInstrumentation(instrument bool) func(*sink) {
	return func(s *sink) {
		s.instrument = instrument
	}
}

func (b sinkBuilderFunc) WithFlushInterval(flushInterval time.Duration) func(*sink) {
	return func(s *sink) {
		s.flushInterval = flushInterval
	}
}

func (b sinkBuilderFunc) WithBuffer(size int) func(*sink) {
	return func(s *sink) {
		s.builders = append(s.builders, func(i AsyncInserter) AsyncInserter { return NewBufferedInserter(i, size) })
	}
}

type sink struct {
	builders      []func(AsyncInserter) AsyncInserter
	instrument    bool
	flushInterval time.Duration
	router        Router
	schemaHandler SchemaHandler
}

// Consume runs two concurrent threads, one that continually (every flush interval)
// flushes the router and another that consumes changelog entries, pushing them into the
// router. When the changelog entries have finished, we quit the insertion and wait for a
// final flush to return.
func (s *sink) Consume(ctx context.Context, entries changelog.Changelog, ack AckCallback) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var g run.Group

	flushDone := make(chan struct{})
	g.Add(
		func() error {
			return s.startFlush(ctx, ack, flushDone)
		},
		func(err error) {
			close(flushDone) // this triggers a final flush
		},
	)

	g.Add(
		func() error {
			return s.startConsume(ctx, entries, ack)
		},
		func(err error) {
			telem.LoggerFrom(ctx).Log("msg", "finished consume", "error", err)
		},
	)

	return g.Run()
}

func (s *sink) startConsume(ctx context.Context, entries changelog.Changelog, ack AckCallback) error {
	for envelope := range entries {
		switch entry := envelope.Unwrap().(type) {
		case *changelog.Schema:
			if err := s.handleSchema(ctx, entry); err != nil {
				return err
			}
		case *changelog.Modification:
			s.router.Insert(ctx, []*changelog.Modification{entry})
		}
	}

	return nil
}

func (s *sink) handleSchema(ctx context.Context, schema *changelog.Schema) error {
	ctx, span, logger := telem.StartSpan(ctx, "pkg/sinks/generic.Sink.handleSchema")
	defer span.End()

	logger = kitlog.With(logger, "event", "handle_schema", "schema", schema.TableReference())
	defer logger.Log()

	inserter, outcome, err := s.schemaHandler.Handle(ctx, schema)
	logger = kitlog.With(logger, "outcome", string(outcome))
	if err != nil {
		return err
	}

	route := Route(schema.TableReference())
	if outcome == SchemaHandlerUpdate {
		s.router.Register(ctx, route, s.buildInserter(route, inserter))
	}

	return nil
}

func (s *sink) buildInserter(route Route, sync Inserter) AsyncInserter {
	// If instrumentation is enabled, we want to instrument the sync interface. This ensures
	// we track the lowest level operation, which is often what we'll be interested in.
	if s.instrument {
		sync = NewInstrumentedInserter(route, sync)
	}

	inserter := NewAsyncInserter(sync)
	for _, builder := range s.builders {
		inserter = builder(inserter)
	}

	return inserter
}

func (s *sink) startFlush(ctx context.Context, ack AckCallback, done chan struct{}) error {
	logger := telem.LoggerFrom(ctx)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-done:
			logger.Log("event", "triggering_final_flush", "msg", "told to finish, trying one more flush")
			return s.flush(ctx, ack)
		case <-time.After(s.flushInterval):
			if err := s.flush(ctx, ack); err != nil {
				logger.Log("event", "flush_fail", "msg", "failed to flush, cannot recover")
				return err
			}
		}
	}
}

func (s *sink) flush(ctx context.Context, ack AckCallback) error {
	ctx, span, logger := telem.StartSpan(ctx, "pkg/sinks/generic.Sink.flush")
	defer span.End()

	count, lsn, err := s.router.Flush(ctx).Get(ctx)
	logger.Log("event", "flush", "count", count, "lsn", lsn, "error", err)
	if err != nil {
		return err
	}

	if lsn != nil && ack != nil {
		ack(changelog.Entry{Modification: &changelog.Modification{LSN: lsn}})
	}

	return nil
}
