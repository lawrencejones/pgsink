package imports

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/lawrencejones/pg2sink/pkg/changelog"
	"github.com/lawrencejones/pg2sink/pkg/logical"
	"github.com/lawrencejones/pg2sink/pkg/models"
	"github.com/lawrencejones/pg2sink/pkg/sinks"

	kitlog "github.com/go-kit/kit/log"
	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgtype"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	uuid "github.com/satori/go.uuid"
	"go.opencensus.io/trace"
)

type ImporterOptions struct {
	WorkerCount      int           // maximum parallel import workers
	PublicationID    string        // identifier for Postgres publication
	SubscriptionName string        // name of subscription (should match replication slot name)
	PollInterval     time.Duration // interval to check for new import jobs
	SnapshotTimeout  time.Duration // max duration to hold open a Postgres snapshot
	BatchLimit       int           // max rows to pull from the database per import job
	BufferSize       int           // channel buffer between Postgres and sink
}

func NewImporter(logger kitlog.Logger, pool *pgx.ConnPool, sink sinks.Sink, opts ImporterOptions) *Importer {
	return &Importer{
		logger: logger,
		pool:   pool,
		sink:   sink,
		opts:   opts,
	}
}

type Importer struct {
	logger kitlog.Logger
	pool   *pgx.ConnPool
	sink   sinks.Sink
	opts   ImporterOptions
}

func (i Importer) Run(ctx context.Context) {
	var wg sync.WaitGroup

	for worker := 0; worker < i.opts.WorkerCount; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			i.runWorker(ctx)
		}()
	}

	wg.Wait()
}

func (i Importer) runWorker(ctx context.Context) {
	logger := kitlog.With(i.logger, "worker_id", uuid.NewV4().String())

	for {
		job, err := i.acquireAndWork(ctx, logger)
		if err != nil {
			logger.Log("error", err)
		}

		if err == nil && job != nil {
			logger.Log("event", "job_worked", "msg", "job worked, checking for more work")
			continue
		}

		if err == nil && job == nil {
			logger.Log("event", "no_job_found", "msg", "no jobs available for working, pausing")
		}

		select {
		case <-ctx.Done():
			logger.Log("event", "done", "msg", "context expired, stopping worker")
			return
		case <-time.After(i.opts.PollInterval):
			// retry loop
		}
	}
}

func (i Importer) acquireAndWork(ctx context.Context, logger kitlog.Logger) (*models.ImportJob, error) {
	tx, err := i.pool.Begin()
	if err != nil {
		return nil, errors.Wrap(err, "failed to open job acquire transaction")
	}

	defer func() {
		if err := tx.Commit(); err != nil {
			logger.Log("error", err, "msg", "failed to commit transaction")
		}
	}()

	job, err := models.ImportJobStore{i.pool}.Acquire(ctx, tx, i.opts.PublicationID, i.opts.SubscriptionName)
	if err != nil {
		return nil, errors.Wrap(err, "failed to acquire job")
	}

	if job == nil {
		return job, nil
	}

	logger = kitlog.With(logger, "job_id", job.ID, "table_name", job.TableName)
	if err := i.work(ctx, logger, tx, job); err != nil {
		if err := i.jobStore(tx).SetError(ctx, job.ID, err); err != nil {
			logger.Log("error", err, "msg", "failed to set the error on our job")
		}

		return nil, errors.Wrap(err, "failed to work job")
	}

	return job, nil
}

var (
	workPostgresRowsReadTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pg2sink_importer_work_postgres_rows_read_total",
			Help: "Total number of rows read from postgres, labelled per-table",
		},
		[]string{"table"},
	)
	workQueryDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "pg2sink_importer_work_query_duration_seconds",
			Help:    "Distribution of time import queries were held open",
			Buckets: prometheus.ExponentialBuckets(0.125, 2, 12), // 0.125 -> 512s
		},
		[]string{"table"},
	)
	workSinkFlushDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "pg2sink_importer_work_sink_flush_duration_seconds",
			Help:    "Distribution of time taken to flush the sink",
			Buckets: prometheus.ExponentialBuckets(0.125, 2, 9), // 0.125 -> 64s
		},
		[]string{"table"},
	)
)

func (i Importer) work(ctx context.Context, logger kitlog.Logger, tx *pgx.Tx, job *models.ImportJob) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	ctx, span := trace.StartSpan(ctx, "pkg/imports.Importer.work")
	defer span.End()
	span.AddAttributes(
		trace.StringAttribute("publication_id", job.PublicationID),
		trace.StringAttribute("subscription_name", job.SubscriptionName),
		trace.StringAttribute("table", job.TableName),
		trace.StringAttribute("cursor", fmt.Sprintf("%v", job.Cursor)),
		trace.Int64Attribute("job_id", job.ID),
		trace.Int64Attribute("buffer_size", int64(i.opts.BufferSize)),
		trace.Int64Attribute("batch_limit", int64(i.opts.BatchLimit)),
	)

	cfg, err := buildJobConfig(ctx, logger, tx, job)
	if err != nil {
		return errors.Wrap(err, "failed to build job config")
	}

	// Open a channel for changelog messages. We'll first send a schema, then the
	// modifications
	entries := make(changelog.Changelog, i.opts.BufferSize)
	sinkDoneChan := make(chan error)
	go func() {
		sinkDoneChan <- i.sink.Consume(ctx, entries, nil)
		close(sinkDoneChan)
	}()

	logger.Log("event", "changelog.push_schema", "msg", "pushing relation schema")
	schema := changelog.SchemaFromRelation(time.Now(), nil, cfg.Relation)
	entries <- changelog.Entry{Schema: &schema}

	logger.Log("event", "changelog.scan_modification_batch")
	batchSize, lastCursor, earlyExit, err := i.scanBatch(ctx, logger, tx, cfg, entries)
	if err != nil {
		return errors.Wrap(err, "failed to scan batch of imported rows")
	}

	// Now we've scanned all the rows, we should close our entries channel
	close(entries)

	// Wait for the sink to flush, or our context to end
	logger.Log("event", "changelog.wait_for_sink", "msg", "pausing until sink has flushed all changes")
	if err := i.waitForSink(ctx, logger, job, sinkDoneChan); err != nil {
		return errors.Wrap(err, "failed waiting for sink")
	}

	logger.Log("event", "import.update_cursor", "cursor", lastCursor)
	if err := i.jobStore(tx).UpdateCursor(ctx, job.ID, lastCursor); err != nil {
		return errors.Wrap(err, "failed to update cursor")
	}

	if !earlyExit && batchSize < i.opts.BatchLimit {
		logger.Log("event", "import.complete", "msg", "imported all rows, marking as complete")
		if _, err := i.jobStore(tx).Complete(ctx, job.ID); err != nil {
			return errors.Wrap(err, "failed to mark job as complete")
		}
	}

	return nil
}

// scanBatch runs a query against the import table and scans each row into the changelog.
// It holds the query for up-to the SnapshotTimeout, ensuring we don't block vacuums or
// other maintenance tasks.
func (i Importer) scanBatch(ctx context.Context, logger kitlog.Logger, tx *pgx.Tx, cfg *JobConfig, entries changelog.Changelog) (int, string, bool, error) {
	ctx, span := trace.StartSpan(ctx, "pkg/imports.Importer.scanBatch")
	defer span.End()

	fail := func(err error) (int, string, bool, error) {
		return -1, "", false, err
	}

	var (
		timestamp         pgtype.Timestamp
		earlyExit         = false
		modificationCount = 0
		holdSnapshotUntil = time.After(i.opts.SnapshotTimeout)
		tableRowsRead     = workPostgresRowsReadTotal.With(
			prometheus.Labels{"table": cfg.TableName},
		)
	)

	rows, err := cfg.Query(ctx, tx, i.opts.BatchLimit)
	if err != nil {
		return fail(errors.Wrap(err, "failed to query table"))
	}

	defer rows.Close()
	defer prometheus.NewTimer(prometheus.ObserverFunc(func(v float64) {
		logger.Log("event", "import.query_close", "duration", v,
			"early_exit", earlyExit, "modification_count", modificationCount)
		workQueryDurationSeconds.WithLabelValues(cfg.TableName).Observe(v)
	})).ObserveDuration()

forEachRow:
	for rows.Next() {
		if err := rows.Scan(append([]interface{}{&timestamp}, cfg.Scanners...)...); err != nil {
			return fail(errors.Wrap(err, "failed to scan table"))
		}

		row := map[string]interface{}{}
		for idx, column := range cfg.Relation.Columns {
			row[column.Name] = cfg.Scanners[idx].(logical.ValueScanner).Get()
		}

		tableRowsRead.Inc()
		modificationCount++
		modification := &changelog.Modification{
			Timestamp: timestamp.Get().(time.Time),
			Namespace: changelog.Namespace(cfg.Relation.String()),
			After:     row,
		}

		entries <- changelog.Entry{Modification: modification}

		select {
		case <-holdSnapshotUntil:
			span.Annotate([]trace.Attribute{}, "Exceeded snapshot deadline")
			earlyExit = true
			break forEachRow
		default: // continue
		}
	}

	// The last processed primary key is the last scanned row, so we can use EncodeText to
	// retrieve it.
	lastCursor, err := cfg.PrimaryKeyScanner.EncodeText(nil, []byte{})
	if err != nil {
		return fail(errors.Wrap(err, "failed to encode last processed cursor"))
	}

	// Release our snapshot, which was held for the benefit of our cursor
	return modificationCount, string(lastCursor), earlyExit, nil
}

func (i Importer) waitForSink(ctx context.Context, logger kitlog.Logger, job *models.ImportJob, sinkDoneChan chan error) error {
	ctx, span := trace.StartSpan(ctx, "pkg/imports.Importer.waitForSink")
	defer span.End()

	timer := prometheus.NewTimer(prometheus.ObserverFunc(func(v float64) {
		logger.Log("event", "wait_for_sink", "duration", v)
		workSinkFlushDurationSeconds.WithLabelValues(job.TableName).Observe(v)
	}))
	defer timer.ObserveDuration()

	select {
	case <-ctx.Done():
		span.Annotate([]trace.Attribute{}, "Timed out before sink could flush")
		return fmt.Errorf("timed out before sink could flush")
	case err := <-sinkDoneChan:
		return errors.Wrap(err, "failed to flush sink")
	}
}

func (i Importer) jobStore(conn models.Connection) models.ImportJobStore {
	if conn == nil {
		conn = i.pool
	}

	return models.ImportJobStore{conn}
}
