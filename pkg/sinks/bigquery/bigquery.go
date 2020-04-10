package bigquery

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/lawrencejones/pg2sink/pkg/changelog"
	"github.com/lawrencejones/pg2sink/pkg/sinks"
	"github.com/lawrencejones/pg2sink/pkg/sinks/generic"

	bq "cloud.google.com/go/bigquery"
	kitlog "github.com/go-kit/kit/log"
	"github.com/oklog/run"
	"go.opencensus.io/trace"
	"google.golang.org/api/googleapi"
)

func New(ctx context.Context, logger kitlog.Logger, opts Options) (sinks.Sink, error) {
	client, err := bq.NewClient(ctx, opts.ProjectID)
	if err != nil {
		return nil, err
	}

	kitlog.With(logger, "project", opts.ProjectID, "dataset", opts.Dataset, "location", opts.Location)

	dataset := client.Dataset(opts.Dataset)
	md, err := dataset.Metadata(ctx)
	if allowNotFound(err) != nil {
		return nil, err
	}

	if md == nil {
		logger.Log("event", "dataset.create", "msg", "dataset does not exist, creating")
		md = &bq.DatasetMetadata{
			Name:        opts.Dataset,
			Location:    opts.Location,
			Description: "Dataset created by pg2sink",
		}

		if err := dataset.Create(ctx, md); err != nil {
			return nil, err
		}
	}

	return &Sink{
		logger:  logger,
		dataset: dataset,
		syncer:  newDatasetSyncer(dataset),
		router:  generic.NewRouter(logger),
		opts:    opts,
	}, nil
}

type Sink struct {
	logger  kitlog.Logger
	dataset *bq.Dataset
	syncer  Syncer
	router  generic.Router
	opts    Options
}

type Options struct {
	ProjectID       string
	Dataset         string
	Location        string
	FlushInterval   time.Duration // TODO
	FlushLimit      int
	TableBufferSize int
}

func (s *Sink) Consume(ctx context.Context, entries changelog.Changelog, ack sinks.AckCallback) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var g run.Group
	var bufferedCount uint64

	g.Add(
		func() error {
			return s.periodicallyFlush(ctx, &bufferedCount, ack)
		},
		func(error) { cancel() },
	)

	g.Add(
		func() error {
			return s.consumeEntries(ctx, entries, &bufferedCount, ack)
		},
		func(error) { cancel() },
	)

	if err := g.Run(); err != nil {
		if !errors.Is(err, context.Canceled) {
			return err
		}
	}

	if err := s.flush(ctx, &bufferedCount, ack, true); err != nil {
		return err
	}

	if count := atomic.LoadUint64(&bufferedCount); count > 0 {
		return fmt.Errorf("failed to flush buffer, still %d remaining", count)
	}

	return nil
}

func (s *Sink) periodicallyFlush(ctx context.Context, bufferedCount *uint64, ack sinks.AckCallback) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(s.opts.FlushInterval):
			s.logger.Log("event", "flush", "msg", "flushing BigQuery router")
			if err := s.flush(ctx, bufferedCount, ack, false); err != nil {
				return err
			}
		}
	}
}

func (s *Sink) consumeEntries(ctx context.Context, entries changelog.Changelog, bufferedCount *uint64, ack sinks.AckCallback) (err error) {
	for envelope := range entries {
		switch entry := envelope.Unwrap().(type) {
		case *changelog.Schema:
			if err := s.handleSchema(ctx, entry); err != nil {
				return err
			}
		case *changelog.Modification:
			s.router.Insert(ctx, []*changelog.Modification{entry})
			atomic.AddUint64(bufferedCount, 1)
			if atomic.LoadUint64(bufferedCount) > uint64(s.opts.FlushLimit) {
				if err := s.flush(ctx, bufferedCount, ack, true); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (s *Sink) handleSchema(ctx context.Context, schema *changelog.Schema) error {
	ctx, span := trace.StartSpan(ctx, "pkg/sinks/bigquery/Sink.handleSchema")
	span.AddAttributes(trace.StringAttribute("namespace", string(schema.Spec.Namespace)))
	defer span.End()

	table, outcome, err := s.syncer.SyncTables(ctx, s.logger, schema)
	if err != nil {
		return err
	}

	span.AddAttributes(trace.StringAttribute("outcome", string(outcome)))
	if outcome == SyncUpdate {
		s.router.Register(ctx, schema.Spec.Namespace, s.configureAsyncInserter(table))
	}

	return nil
}

// configureAsyncInserter builds a generic.AsyncInserter from a BigQuery table. It ensures
// we apply uniform configuration to all our inserters, such as buffer sizes.
func (s *Sink) configureAsyncInserter(table *Table) generic.AsyncInserter {
	var inserter generic.AsyncInserter

	inserter = generic.WrapAsync(&tableInserter{table})
	inserter = generic.WithBuffer(inserter, s.opts.TableBufferSize)

	return inserter
}

func (s *Sink) flush(ctx context.Context, bufferedCount *uint64, ack sinks.AckCallback, forced bool) error {
	ctx, span := trace.StartSpan(ctx, "pkg/sinks/bigquery/Sink.flush")
	span.AddAttributes(trace.Attribute(trace.BoolAttribute("forced", forced)))
	defer span.End()

	count, lsn, err := s.router.Flush(ctx).Get(ctx)
	if err != nil {
		return err
	}

	span.Annotate([]trace.Attribute{trace.Int64Attribute("flush_count", int64(count))}, "Successful flush")
	atomic.AddUint64(bufferedCount, uint64(count))

	if lsn != nil && ack != nil {
		ack(changelog.Entry{Modification: &changelog.Modification{LSN: lsn}})
	}

	return nil
}

func allowNotFound(err error) error {
	if err, ok := err.(*googleapi.Error); ok && err.Code == 404 {
		return nil
	}

	return err
}
