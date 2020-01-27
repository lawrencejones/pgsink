package imports

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/lawrencejones/pg2sink/pkg/changelog"
	"github.com/lawrencejones/pg2sink/pkg/logical"
	"github.com/lawrencejones/pg2sink/pkg/models"
	"github.com/lawrencejones/pg2sink/pkg/sinks"
	"github.com/lawrencejones/pg2sink/pkg/util"

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
	sinkConsumeChan := make(chan error)
	go func() {
		sinkConsumeChan <- i.sink.Consume(ctx, entries, nil)
		close(sinkConsumeChan)
	}()

	logger.Log("event", "changelog.push_schema", "msg", "pushing relation schema")
	schema := changelog.SchemaFromRelation(time.Now(), nil, cfg.Relation)
	entries <- changelog.Entry{Schema: &schema}

	logger.Log("event", "scanning_batch")
	batchSize, lastCursor, earlyExit, err := i.scanBatch(ctx, logger, tx, cfg, entries)
	if err != nil {
		return errors.Wrap(err, "failed to scan batch of imported rows")
	}

	// Now we've scanned all the rows, we should close our entries channel
	close(entries)

	// Wait for the sink to flush, or our context to end
	err = func() error {
		ctx, span := trace.StartSpan(ctx, "pkg/imports.Importer.work(flush)")
		defer span.End()

		timer := prometheus.NewTimer(prometheus.ObserverFunc(func(v float64) {
			logger.Log("event", "sink_consume.flush", "duration", v)
			workSinkFlushDurationSeconds.WithLabelValues(job.TableName).Observe(v)
		}))
		defer timer.ObserveDuration()

		select {
		case <-ctx.Done():
			span.Annotate([]trace.Attribute{}, "Timed out waiting for flush")
			return fmt.Errorf("timed out before sink could flush")
		case err := <-sinkConsumeChan:
			logger.Log("event", "sink_consume.flush", "duration", timer.ObserveDuration())
			return errors.Wrap(err, "failed to flush sink")
		}
	}()

	if err != nil {
		return err
	}

	if !earlyExit && batchSize < i.opts.BatchLimit {
		logger.Log("event", "import.complete")
		_, err := i.jobStore(tx).Complete(ctx, job.ID)
		return err
	}

	logger.Log("event", "import.update_cursor", "cursor", lastCursor)
	return i.jobStore(tx).UpdateCursor(ctx, job.ID, lastCursor)
}

func (i Importer) scanBatch(ctx context.Context, logger kitlog.Logger, tx *pgx.Tx, cfg *JobConfig, entries changelog.Changelog) (int, string, bool, error) {
	ctx, span := trace.StartSpan(ctx, "pkg/imports.Importer.scanBatch")
	defer span.End()

	var (
		timestamp         pgtype.Timestamp
		earlyExit         = false
		modificationCount = 0
		queryStart        = time.Now()
		holdSnapshotUntil = time.After(i.opts.SnapshotTimeout)
		tableRowsRead     = workPostgresRowsReadTotal.With(
			prometheus.Labels{"table": cfg.TableName},
		)
	)

	rows, err := cfg.Query(ctx, tx, i.opts.BatchLimit)
	if err != nil {
		return -1, "", false, errors.Wrap(err, "failed to query table")
	}

	defer rows.Close()

forEachRow:
	for rows.Next() {
		if err := rows.Scan(append([]interface{}{&timestamp}, cfg.Scanners...)...); err != nil {
			return -1, "", false, errors.Wrap(err, "failed to scan table")
		}

		row := map[string]interface{}{}
		for idx, column := range cfg.Relation.Columns {
			row[column.Name] = cfg.Scanners[idx].(logical.ValueScanner).Get()
		}

		tableRowsRead.Inc()
		modificationCount++
		modification := &changelog.Modification{
			Timestamp: timestamp.Get().(time.Time),
			Namespace: cfg.Relation.String(),
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

	logger.Log("event", "import.query_close", "duration", time.Since(queryStart).Seconds(),
		"modification_count", modificationCount)

	// The last processed primary key is the last scanned row, so we can use EncodeText to
	// retrieve it.
	lastCursor, err := cfg.PrimaryKeyScanner.EncodeText(nil, []byte{})
	if err != nil {
		return -1, "", false, errors.Wrap(err, "failed to encode last processed cursor")
	}

	// Release our snapshot, which was held for the benefit of our cursor
	return modificationCount, string(lastCursor), earlyExit, nil
}

func (i Importer) jobStore(conn models.Connection) models.ImportJobStore {
	if conn == nil {
		conn = i.pool
	}

	return models.ImportJobStore{conn}
}

type JobConfig struct {
	TableName         string
	PrimaryKey        string
	PrimaryKeyScanner logical.ValueScanner
	Relation          *logical.Relation
	Scanners          []interface{}
	Cursor            interface{}
}

func (j JobConfig) Query(ctx context.Context, conn models.Connection, batchLimit int) (*pgx.Rows, error) {
	query := buildQuery(j.Relation, j.PrimaryKey, batchLimit, j.Cursor)
	return conn.QueryEx(ctx, query, nil, util.Compact([]interface{}{j.Cursor})...)
}

// buildJobConfig attempts to generate some boilerplate information that is required to
// process an import, things such as interpreting the previous cursor value, and the
// primary key of the table.
func buildJobConfig(ctx context.Context, logger kitlog.Logger, tx *pgx.Tx, job *models.ImportJob) (*JobConfig, error) {
	// We should query for the primary key as the first thing we do, as this may fail if the
	// table is misconfigured. It's better to fail here, before we've pushed anything into
	// the changelog, than after pushing the schema when we discover the table is
	// incompatible.
	logger.Log("event", "primary_key.lookup", "msg", "querying Postgres for relations primary key column")
	primaryKey, err := getPrimaryKeyColumn(ctx, tx, job.TableName)
	if err != nil {
		return nil, err
	}

	logger.Log("event", "relation.build", "msg", "querying Postgres for relation type information")
	relation, err := buildRelation(ctx, tx, job.TableName)
	if err != nil {
		return nil, errors.Wrap(err, "failed to build relation for table")
	}

	// Build scanners for decoding column types. We'll need the primary key scanner for
	// interpreting the cursor.
	primaryKeyScanner, scanners := buildScanners(relation, primaryKey)

	// We need to translate the import_jobs.cursor value, which is text, into a type that
	// will be supported for querying into the table. We can use the primaryKeyScanner for
	// this, which ensures we reliably encode/decode Postgres types.
	var cursor interface{}
	if job.Cursor != nil {
		if err := primaryKeyScanner.Scan(*job.Cursor); err != nil {
			return nil, errors.Wrap(err, "incompatible cursor in import_jobs table")
		}

		cursor = primaryKeyScanner.Get()
	}

	cfg := &JobConfig{
		TableName:         job.TableName,
		PrimaryKey:        primaryKey,
		PrimaryKeyScanner: primaryKeyScanner,
		Relation:          relation,
		Scanners:          scanners,
		Cursor:            cursor,
	}

	return cfg, nil
}

// buildScanners produces pgx type scanners, returning a scanner for the relation primary
// key and a slice of scanners for the other columns.
func buildScanners(relation *logical.Relation, primaryKey string) (primaryKeyScanner logical.ValueScanner, scanners []interface{}) {
	// Go can't handle splatting non-empty-interface types into a parameter list of
	// empty-interfaces, so we have to construct an interface{} slice of scanners.
	scanners = make([]interface{}, len(relation.Columns))
	for idx, column := range relation.Columns {
		scanner := logical.TypeForOID(column.Type)
		scanners[idx] = scanner

		// We'll need this scanner to convert the cursor value between what the table accepts
		// and what we'll store in import_jobs
		if column.Name == primaryKey {
			primaryKeyScanner = scanner
		}
	}

	return
}

// buildRelation generates the logical.Relation structure by querying Postgres catalog
// tables. Importantly, this populates the relation.Columns slice, providing type
// information that can later be used to marshal Golang types.
func buildRelation(ctx context.Context, conn models.Connection, tableName string) (*logical.Relation, error) {
	ctx, span := trace.StartSpan(ctx, "pkg/imports.buildRelation")
	defer span.End()

	// Eg. oid = 16411, namespace = public, relname = example
	query := `
	select pg_class.oid as oid
	     , nspname as namespace
	     , relname as name
		from pg_class join pg_namespace on pg_class.relnamespace=pg_namespace.oid
	 where pg_class.oid = $1::regclass::oid;
	`

	relation := &logical.Relation{Columns: []logical.Column{}}
	err := conn.QueryRowEx(ctx, query, nil, tableName).Scan(&relation.ID, &relation.Namespace, &relation.Name)
	if err != nil {
		return nil, errors.Wrap(err, "failed to identify table namespace and name")
	}

	// Eg. name = id, type = 20
	columnQuery := `
	select attname as name
			 , atttypid as type
	  from pg_attribute
	 where attrelid = $1 and attnum > 0 and not attisdropped
	 order by attnum;
	`

	rows, err := conn.QueryEx(ctx, columnQuery, nil, relation.ID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to query pg_attribute for relation columns")
	}

	defer rows.Close()

	for rows.Next() {
		column := logical.Column{}
		if err := rows.Scan(&column.Name, &column.Type); err != nil {
			return nil, errors.Wrap(err, "failed to scan column types")
		}

		relation.Columns = append(relation.Columns, column)
	}

	return relation, nil
}

// buildQuery creates a query string for the given relation, with an optional cursor.
// Prepended to the columns is now(), which enables us to timestamp our imported rows to
// the database time.
func buildQuery(relation *logical.Relation, primaryKey string, limit int, cursor interface{}) string {
	columnNames := make([]string, len(relation.Columns))
	for idx, column := range relation.Columns {
		columnNames[idx] = column.Name
	}

	query := fmt.Sprintf(`select now(), %s from %s`, strings.Join(columnNames, ", "), relation.String())
	if cursor != nil {
		query += fmt.Sprintf(` where %s > $1`, primaryKey)
	}
	query += fmt.Sprintf(` order by %s limit %d`, primaryKey, limit)

	return query
}

type multiplePrimaryKeysError []string

func (m multiplePrimaryKeysError) Error() string {
	return fmt.Sprintf("unsupported multiple primary keys: %s", strings.Join(m, ","))
}

type noPrimaryKeyError struct{}

func (n noPrimaryKeyError) Error() string {
	return "no primary key found"
}

// getPrimaryKeyColumn identifies the primary key column of the given table. It only
// supports tables with primary keys, and of those, only single column primary keys.
func getPrimaryKeyColumn(ctx context.Context, conn models.Connection, tableName string) (string, error) {
	ctx, span := trace.StartSpan(ctx, "pkg/imports.getPrimaryKeyColumn")
	defer span.End()

	query := `
	select array_agg(pg_attribute.attname)
	from pg_index join pg_attribute
	on pg_attribute.attrelid = pg_index.indrelid and pg_attribute.attnum = ANY(pg_index.indkey)
	where pg_index.indrelid = $1::regclass
	and pg_index.indisprimary;
	`

	primaryKeysTextArray := pgtype.TextArray{}
	err := conn.QueryRowEx(ctx, query, nil, tableName).Scan(&primaryKeysTextArray)
	if err != nil {
		return "", err
	}

	var primaryKeys []string
	if err := primaryKeysTextArray.AssignTo(&primaryKeys); err != nil {
		return "", err
	}

	if len(primaryKeys) == 0 {
		return "", new(noPrimaryKeyError)
	} else if len(primaryKeys) > 1 {
		return "", multiplePrimaryKeysError(primaryKeys)
	}

	return primaryKeys[0], nil
}
