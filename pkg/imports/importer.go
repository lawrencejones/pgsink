package imports

import (
	"context"
	"fmt"
	"time"

	"github.com/lawrencejones/pgsink/pkg/changelog"
	"github.com/lawrencejones/pgsink/pkg/dbschema/pgsink/model"
	. "github.com/lawrencejones/pgsink/pkg/dbschema/pgsink/table"
	"github.com/lawrencejones/pgsink/pkg/decode"
	"github.com/lawrencejones/pgsink/pkg/sinks/generic"
	"github.com/lawrencejones/pgsink/pkg/telem"

	"github.com/alecthomas/kingpin"
	. "github.com/go-jet/jet/postgres"
	kitlog "github.com/go-kit/kit/log"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opencensus.io/trace"
)

type ImporterOptions struct {
	SnapshotTimeout time.Duration
	BatchLimit      int
	BufferSize      int
}

func (opt *ImporterOptions) Bind(cmd *kingpin.CmdClause, prefix string) *ImporterOptions {
	cmd.Flag(fmt.Sprintf("%ssnapshot-timeout", prefix), "Hold snapshots for no longer than this").Default("1m").DurationVar(&opt.SnapshotTimeout)
	cmd.Flag(fmt.Sprintf("%sbatch-limit", prefix), "Max rows to pull from database per import iteration").Default("5000").IntVar(&opt.BatchLimit)
	cmd.Flag(fmt.Sprintf("%sbuffer-size", prefix), "Channel buffer between Postgres and the sink").Default("5000").IntVar(&opt.BufferSize)

	return opt
}

type Importer interface {
	Do(ctx context.Context, logger kitlog.Logger, tx pgx.Tx, job model.ImportJobs) error
}

func NewImporter(sink generic.Sink, decoder decode.Decoder, opts ImporterOptions) Importer {
	return &importer{
		sink:    sink,
		decoder: decoder,
		opts:    opts,
	}
}

type importer struct {
	sink    generic.Sink
	decoder decode.Decoder
	opts    ImporterOptions
}

var (
	workPostgresRowsReadTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pgsink_importer_work_postgres_rows_read_total",
			Help: "Total number of rows read from postgres, labelled per-table",
		},
		[]string{"table"},
	)
	workQueryDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "pgsink_importer_work_query_duration_seconds",
			Help:    "Distribution of time import queries were held open",
			Buckets: prometheus.ExponentialBuckets(0.125, 2, 12), // 0.125 -> 512s
		},
		[]string{"table"},
	)
	workSinkFlushDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "pgsink_importer_work_sink_flush_duration_seconds",
			Help:    "Distribution of time taken to flush the sink",
			Buckets: prometheus.ExponentialBuckets(0.125, 2, 9), // 0.125 -> 64s
		},
		[]string{"table"},
	)
)

// Do works an import job.
func (i importer) Do(ctx context.Context, logger kitlog.Logger, tx pgx.Tx, job model.ImportJobs) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	ctx, span, logger := telem.Logger(logger)(trace.StartSpan(ctx, "pkg/imports.importer.Do"))
	defer span.End()
	span.AddAttributes(
		trace.Int64Attribute("job_id", job.ID),
		trace.StringAttribute("subscription_id", job.SubscriptionID),
		trace.StringAttribute("table", job.TableName),
		trace.StringAttribute("cursor", fmt.Sprintf("%v", job.Cursor)),
		trace.Int64Attribute("buffer_size", int64(i.opts.BufferSize)),
		trace.Int64Attribute("batch_limit", int64(i.opts.BatchLimit)),
	)

	cfg, err := Build(ctx, logger, i.decoder, tx, job)
	if err != nil {
		return err
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
		return fmt.Errorf("failed to scan batch of imported rows: %w", err)
	}

	// Now we've scanned all the rows, we should close our entries channel
	close(entries)

	// Wait for the sink to flush, or our context to end
	logger.Log("event", "changelog.wait_for_sink", "msg", "pausing until sink has flushed all changes")
	if err := i.waitForSink(ctx, logger, job, sinkDoneChan); err != nil {
		return fmt.Errorf("failed waiting for sink: %w", err)
	}

	logger.Log("event", "import.update_job", "cursor", lastCursor)

	// If we didn't exit early, but we consumed less than our batch limit, we can infer that
	// this import is now complete and no more rows remain.
	complete := !earlyExit && batchSize < i.opts.BatchLimit
	completedAt := Raw("NULL")
	if complete {
		completedAt = Raw("now()")
	}

	query, args := ImportJobs.
		UPDATE(ImportJobs.Cursor, ImportJobs.CompletedAt).
		SET(lastCursor, completedAt).
		WHERE(ImportJobs.ID.EQ(Int(job.ID))).
		Sql()
	if _, err := tx.Exec(ctx, query, args...); err != nil {
		return fmt.Errorf("failed to update job: %w", err)
	}

	if complete {
		logger.Log("event", "import.complete", "msg", "imported all rows, marking as complete")
	}

	return nil
}

// scanBatch runs a query against the import table and scans each row into the changelog.
// It holds the query for up-to the SnapshotTimeout, ensuring we don't block vacuums or
// other maintenance tasks.
func (i importer) scanBatch(ctx context.Context, logger kitlog.Logger, tx pgx.Tx, cfg *Import, entries changelog.Changelog) (int, []byte, bool, error) {
	ctx, span := trace.StartSpan(ctx, "pkg/imports.importer.scanBatch")
	defer span.End()

	fail := func(err error, msg string) (int, []byte, bool, error) {
		return -1, nil, false, fmt.Errorf("%s: %w", msg, err)
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

	query, args := cfg.buildQuery(i.opts.BatchLimit)
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return fail(err, "failed to query table")
	}

	defer rows.Close()
	defer prometheus.NewTimer(prometheus.ObserverFunc(func(v float64) {
		logger.Log("event", "import.query_close", "duration", v,
			"early_exit", earlyExit, "modification_count", modificationCount)
		workQueryDurationSeconds.WithLabelValues(cfg.TableName).Observe(v)
	})).ObserveDuration()

	var scanners []interface{}
	for _, typeMapping := range cfg.TypeMappings {
		scanners = append(scanners, typeMapping.Scanner)
	}

forEachRow:
	for rows.Next() {
		if err := rows.Scan(append([]interface{}{&timestamp}, scanners...)...); err != nil {
			return fail(err, "failed to scan table")
		}

		row := map[string]interface{}{}
		for idx, column := range cfg.Relation.Columns {
			typeMapping := cfg.TypeMappings[idx]
			dest := typeMapping.NewEmpty()
			typeMapping.Scanner.AssignTo(dest)

			row[column.Name] = dest
		}

		tableRowsRead.Inc()
		modificationCount++
		modification := &changelog.Modification{
			Timestamp: timestamp.Get().(time.Time),
			Namespace: cfg.Relation.Namespace,
			Name:      cfg.Relation.Name,
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
	var lastCursor []byte

	// If we never processed anything, we won't have a cursor!
	if modificationCount > 0 {
		lastCursor, err = cfg.PrimaryKeyScanner.EncodeText(nil, []byte{})
		if err != nil {
			return fail(err, "failed to encode last processed cursor")
		}
	}

	// Release our snapshot, which was held for the benefit of our cursor
	return modificationCount, lastCursor, earlyExit, nil
}

func (i importer) waitForSink(ctx context.Context, logger kitlog.Logger, job model.ImportJobs, sinkDoneChan chan error) error {
	ctx, span := trace.StartSpan(ctx, "pkg/imports.importer.waitForSink")
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
		if err != nil {
			return fmt.Errorf("failed to flush sink: %w", err)
		}
	}

	return nil
}
