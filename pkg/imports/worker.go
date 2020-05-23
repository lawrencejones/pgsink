package imports

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/lawrencejones/pgsink/pkg/sinks/generic"

	"github.com/alecthomas/kingpin"
	kitlog "github.com/go-kit/kit/log"
)

type WorkerOptions struct {
	PollInterval    time.Duration
	SnapshotTimeout time.Duration
	BatchLimit      int
	BufferSize      int
}

func (opt *WorkerOptions) Bind(cmd *kingpin.CmdClause, prefix string) *WorkerOptions {
	cmd.Flag(fmt.Sprintf("%spoll-interval", prefix), "Interval to check for new import jobs").Default("15s").DurationVar(&opt.PollInterval)
	cmd.Flag(fmt.Sprintf("%ssnapshot-timeout", prefix), "Hold snapshots for no longer than this").Default("1m").DurationVar(&opt.SnapshotTimeout)
	cmd.Flag(fmt.Sprintf("%sbatch-limit", prefix), "Max rows to pull from database per import iteration").Default("5000").IntVar(&opt.BatchLimit)
	cmd.Flag(fmt.Sprintf("%sbuffer-size", prefix), "Channel buffer between Postgres and the sink").Default("5000").IntVar(&opt.BufferSize)

	return opt
}

func NewWorker(logger kitlog.Logger, db *sql.DB, sink generic.Sink, opts WorkerOptions) *Worker {
	return &Worker{
		logger:   logger,
		db:       db,
		sink:     sink,
		shutdown: make(chan struct{}),
		done:     make(chan error, 1), // buffered by 1, to ensure progress when reporting an error
		opts:     opts,
	}
}

type Worker struct {
	logger   kitlog.Logger
	db       *sql.DB
	sink     generic.Sink
	shutdown chan struct{}
	done     chan error
	opts     WorkerOptions
}

func (w Worker) Start(ctx context.Context) error {
	defer func() {
		close(w.done)
	}()

	w.logger.Log("event", "start", "msg", "starting worker loop")
	for {
		// work

		select {
		case <-ctx.Done():
			w.logger.Log("event", "finish", "msg", "context expired, finishing sync")
			return ctx.Err()
		case <-w.shutdown:
			w.logger.Log("event", "shutdown", "msg", "shutdown requested, exiting")
			return nil
		case <-time.After(w.opts.PollInterval):
			// continue
		}
	}
}

func (w Worker) Shutdown(ctx context.Context) error {
	close(w.shutdown)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-w.done:
		return err
	}
}
