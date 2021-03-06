package imports

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/lawrencejones/pgsink/internal/dbschema/pgsink/model"
	. "github.com/lawrencejones/pgsink/internal/dbschema/pgsink/table"

	"github.com/alecthomas/kingpin"
	. "github.com/go-jet/jet/v2/postgres"
	kitlog "github.com/go-kit/kit/log"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/stdlib"
)

type WorkerOptions struct {
	SubscriptionID   string        // fixes this worker to only work jobs associated with the current subscription
	PollInterval     time.Duration // interval between polling for new jobs
	RetryInterval    time.Duration // retry interval for the exponential backoff
	RetryExponent    int           // retry exponent to calculate backoff
	MaxRetryInterval time.Duration // maximum interval between retries
}

func (opt *WorkerOptions) Bind(cmd *kingpin.CmdClause, prefix string) *WorkerOptions {
	cmd.Flag(fmt.Sprintf("%spoll-interval", prefix), "Interval to check for new import jobs").
		Default("15s").DurationVar(&opt.PollInterval)
	cmd.Flag(fmt.Sprintf("%sretry-interval", prefix), "Retry interval for the exponential backoff").
		Default("5s").DurationVar(&opt.RetryInterval)
	cmd.Flag(fmt.Sprintf("%sretry-exponent", prefix), "Retry exponent to calculate backoff").
		Default("3").IntVar(&opt.RetryExponent)
	cmd.Flag(fmt.Sprintf("%smax-retry-interval", prefix), "Maximum interval between retry attempts").
		Default("1h").DurationVar(&opt.MaxRetryInterval)

	return opt
}

func NewWorker(logger kitlog.Logger, db *sql.DB, opts WorkerOptions) *Worker {
	return &Worker{
		logger:   logger,
		db:       db,
		shutdown: make(chan struct{}),
		done:     make(chan error, 1), // buffered by 1, to ensure progress when reporting an error
		opts:     opts,
	}
}

type Worker struct {
	logger   kitlog.Logger
	db       *sql.DB
	shutdown chan struct{}
	done     chan error
	opts     WorkerOptions
}

// Start begin working the queue, using the given Importer to process jobs
func (w Worker) Start(ctx context.Context, importer Importer) error {
	defer func() {
		close(w.done)
	}()

	w.logger.Log("event", "start", "msg", "starting worker loop")
	for {
		job, err := w.AcquireAndWork(ctx, importer)

		// Each combination of job and err being nil should be handled differently. Use a
		// switch to visually warn readers to pay attention to those differences.
		switch {
		case err == nil && job == nil:
			w.logger.Log("event", "job_not_found", "msg", "no jobs available for working, pausing")

		case err == nil && job != nil:
			w.logger.Log("event", "job_worked", "msg", "job worked, checking for more work")

		// Log whenever we receive an error, but also activate the next case
		case err != nil:
			w.logger.Log("event", "job_worked_error", "error", err, "msg", "failed to work job")
			if job != nil {
				if err := w.setError(ctx, *job, err); err != nil {
					w.logger.Log("event", "job_set_error", "error", err,
						"msg", "found job and failed to work it, then failed to record error on job row. perhaps the database is down?")
				}
			}
		}

		// By default, wait our poll interval. But if we just successfully processed a job, we
		// should try again immediately.
		nextWorkLoopDelay := w.opts.PollInterval
		if job != nil && err != nil {
			nextWorkLoopDelay = time.Duration(0)
		}

		select {
		case <-ctx.Done():
			w.logger.Log("event", "finish", "msg", "context expired, finishing sync")
			return ctx.Err()
		case <-w.shutdown:
			w.logger.Log("event", "shutdown", "msg", "shutdown requested, exiting")
			return nil
		case <-time.After(nextWorkLoopDelay):
			// continue
		}
	}
}

// AcquireAndWork finds a job and works it. The method is public to make testing easy, and
// it should normally be called indirectly via a worker's Start method.
func (w Worker) AcquireAndWork(ctx context.Context, importer Importer) (*model.ImportJobs, error) {
	// Imports pull a lot of data from the database and want to do it quickly. pgx is much
	// more efficient than the standard driver, and allow for more complex manipulation of
	// types. For this reason, we use pgx connections for import work.
	var conn *pgx.Conn
	conn, err := stdlib.AcquireConn(w.db)
	if err != nil {
		return nil, err
	}
	defer stdlib.ReleaseConn(w.db, conn)

	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) // no-op if committed

	job, err := w.acquire(ctx, tx)
	if job == nil || err != nil {
		return job, err
	}

	if err := importer.Do(ctx, w.logger, tx, *job); err != nil {
		return job, err
	}

	return job, tx.Commit(ctx)
}

func (w Worker) acquire(ctx context.Context, tx pgx.Tx) (*model.ImportJobs, error) {
	// We use an exponential backoff calculation of:
	//
	//	delay = retryInterval * (retryExponent ^ errorCount)
	//	delay = maxRetryInterval if delay > maxRetryInterval
	//
	// It is useful to have the retry parameters in Postgres Int expressions, hence the
	// alias.
	retryIntervalSeconds := Int(int64(w.opts.RetryInterval.Seconds()))
	retryExponent := Int(int64(w.opts.RetryExponent))
	maxRetryInterval := Int(int64(w.opts.MaxRetryInterval.Seconds()))

	// Note that the retry delay is capped at the max retry interval, so we avoid silly
	// situations like a job being retried only a year from the last error.
	retryDelaySeconds := LEAST(
		maxRetryInterval,
		retryIntervalSeconds.MUL(
			retryExponent.POW(ImportJobs.ErrorCount),
		),
	)

	// Cast the seconds into an interval, as it's easier to use in the next query
	retryDelayInterval := IntervalExp(Raw("'1 second'::interval")).MUL(CAST(retryDelaySeconds).AS_NUMERIC())

	// Each import worker locks an import_jobs row while that job is being processed. This
	// allows running concurrent workers without doubly running the jobs.
	query, args := ImportJobs.
		SELECT(
			ImportJobs.ID,
			ImportJobs.SubscriptionID,
			ImportJobs.Schema,
			ImportJobs.TableName,
			ImportJobs.Cursor,
		).
		FOR(UPDATE().SKIP_LOCKED()). // conflict against any other workers
		WHERE(
			ImportJobs.SubscriptionID.EQ(String(w.opts.SubscriptionID)).
				AND(ImportJobs.CompletedAt.IS_NULL()). // incomplete
				AND(ImportJobs.ExpiredAt.IS_NULL()).   // still active
				AND(
					// The job has no errors, or we should respect the backoff delay
					ImportJobs.ErrorCount.LT(Int(1)).OR(
						TimestampzExp(Raw("now()")).GT(ImportJobs.LastErrorAt.ADD(retryDelayInterval)),
					),
				),
		).
		ORDER_BY(ImportJobs.Error.IS_NULL().DESC()).
		LIMIT(1).
		Sql()

	var job model.ImportJobs
	if err := tx.QueryRow(ctx, query, args...).Scan(&job.ID, &job.SubscriptionID, &job.Schema, &job.TableName, &job.Cursor); err != nil {
		// It's expected that sometimes we'll have worked everything, and no job will remain
		if err == pgx.ErrNoRows {
			err = nil
		}

		return nil, err
	}

	return &job, nil
}

func (w Worker) setError(ctx context.Context, job model.ImportJobs, workErr error) error {
	stmt := ImportJobs.UPDATE().
		SET(
			ImportJobs.Error.SET(String(workErr.Error())),
			ImportJobs.ErrorCount.SET(Int(1).ADD(ImportJobs.ErrorCount)),
			ImportJobs.LastErrorAt.SET(TimestampzExp(Raw("now()"))),
			ImportJobs.UpdatedAt.SET(TimestampzExp(Raw("now()"))),
		).
		WHERE(ImportJobs.ID.EQ(Int(job.ID)))

	_, err := stmt.ExecContext(ctx, w.db)
	return err
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
