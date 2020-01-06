package imports

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	kitlog "github.com/go-kit/kit/log"
	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgtype"
	"github.com/lawrencejones/pg2sink/pkg/changelog"
	"github.com/lawrencejones/pg2sink/pkg/logical"
	"github.com/lawrencejones/pg2sink/pkg/util"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

type ImporterOptions struct {
	WorkerCount     int           // maximum parallel import workers
	PublicationID   string        // identifier for Postgres publication
	PollInterval    time.Duration // interval to check for new import jobs
	SnapshotTimeout time.Duration // max duration to hold open a Postgres snapshot
	BatchLimit      int           // max rows to pull from the database per import job
}

func NewImporter(logger kitlog.Logger, pool *pgx.ConnPool, opts ImporterOptions) *Importer {
	return &Importer{
		logger: logger,
		pool:   pool,
		opts:   opts,
	}
}

type Importer struct {
	logger kitlog.Logger
	pool   *pgx.ConnPool
	opts   ImporterOptions
}

func (i Importer) Work(ctx context.Context) changelog.Changelog {
	output := make(changelog.Changelog)

	var wg sync.WaitGroup

	for worker := 0; worker < i.opts.WorkerCount; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			i.runWorker(ctx, output)
		}()
	}

	// Close the output channel when all workers have finished working
	go func() {
		wg.Wait()
		close(output)
	}()

	return output
}

func (i Importer) runWorker(ctx context.Context, output changelog.Changelog) {
	logger := kitlog.With(i.logger, "worker_id", uuid.NewV4().String())

	for {
		job, err := i.acquireAndWork(ctx, logger, output)
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

func (i Importer) acquireAndWork(ctx context.Context, logger kitlog.Logger, output changelog.Changelog) (*Job, error) {
	tx, err := i.pool.Begin()
	if err != nil {
		return nil, errors.Wrap(err, "failed to open job acquire transaction")
	}

	defer func() {
		if err := tx.Commit(); err != nil {
			logger.Log("error", err, "msg", "failed to commit transaction")
		}
	}()

	job, err := JobStore{i.pool}.Acquire(ctx, tx, i.opts.PublicationID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to acquire job")
	}

	if job == nil {
		return job, nil
	}

	logger = kitlog.With(logger, "job_id", job.ID, "table_name", job.TableName)
	if err = (Import{job}).Work(ctx, logger, tx, output, i.opts.BatchLimit, time.After(i.opts.SnapshotTimeout)); err != nil {
		return nil, errors.Wrap(err, "failed to work job")
	}

	return job, nil
}

type Import struct {
	*Job
}

func (i Import) Work(ctx context.Context, logger kitlog.Logger, tx *pgx.Tx, output changelog.Changelog, limit int, until <-chan time.Time) error {
	// We should query for the primary key as the first thing we do, as this may fail if the
	// table is misconfigured. It's better to fail here, before we've pushed anything into
	// the changelog, than after pushing the schema when we discover the table is
	// incompatible.
	logger.Log("event", "primary_key.lookup", "msg", "querying Postgres for relations primary key column")
	primaryKey, err := getPrimaryKeyColumn(ctx, tx, i.TableName)
	if err != nil {
		return err
	}

	logger.Log("event", "relation.build", "msg", "querying Postgres for relation type information")
	relation, err := i.buildRelation(ctx, tx)
	if err != nil {
		return errors.Wrap(err, "failed to build relation for table")
	}

	logger.Log("event", "changelog.push_schema", "msg", "pushing relation schema")
	schema := changelog.SchemaFromRelation(time.Now(), nil, relation)
	output <- changelog.Entry{Schema: &schema}

	var primaryKeyScanner logical.ValueScanner

	// Go can't handle splatting non-empty-interface types into a parameter list of
	// empty-interfaces, so we have to construct an interface{} slice of scanners.
	scanners := make([]interface{}, len(relation.Columns))
	for idx, column := range relation.Columns {
		scanner := logical.TypeForOID(column.Type)
		scanners[idx] = scanner

		// We'll need this scanner to convert the cursor value between what the table accepts
		// and what we'll store in import_jobs
		if column.Name == primaryKey {
			primaryKeyScanner = scanner
		}
	}

	// We need to translate the import_jobs.cursor value, which is text, into a type that
	// will be supported for querying into the table. We can use the primaryKeyScanner for
	// this, which ensures we reliably encode/decode Postgres types.
	var cursor interface{}
	if i.Job.Cursor != nil {
		if err := primaryKeyScanner.Scan(*i.Job.Cursor); err != nil {
			return errors.Wrap(err, "incompatible cursor in import_jobs table")
		}

		cursor = primaryKeyScanner.Get()
	}

	logger.Log("event", "import.query", "msg", "executing an import query", "cursor", cursor)
	query := buildQuery(relation, primaryKey, limit, cursor)
	rows, err := tx.QueryEx(ctx, query, nil, util.Compact([]interface{}{cursor})...)
	if err != nil {
		return errors.Wrap(err, "failed to query table")
	}

	defer rows.Close()

	var (
		timestamp     pgtype.Timestamp
		modifications = []*changelog.Modification{}
	)

forEachRow:
	for rows.Next() {
		if err := rows.Scan(append([]interface{}{&timestamp}, scanners...)...); err != nil {
			return errors.Wrap(err, "failed to scan table")
		}

		row := map[string]interface{}{}
		for idx, column := range relation.Columns {
			row[column.Name] = scanners[idx].(logical.ValueScanner).Get()
		}

		modifications = append(modifications, &changelog.Modification{
			Timestamp: timestamp.Get().(time.Time),
			Namespace: relation.String(),
			After:     row,
		})

		select {
		case <-until:
			break forEachRow
		default: // continue
		}
	}

	// Release our snapshot, which was held for the benefit of our cursor
	rows.Close()

	if len(modifications) == 0 {
		logger.Log("event", "import.complete")
		_, err := JobStore{tx}.Complete(ctx, i.Job.ID)
		return err
	}

	logger.Log("event", "changelog.push_modifications", "count", len(modifications))
	for _, modification := range modifications {
		output <- changelog.Entry{Modification: modification}
	}

	lastCursor := modifications[len(modifications)-1].After.(map[string]interface{})[primaryKey]
	lastCursorText, err := primaryKeyScanner.EncodeText(nil, []byte{})
	if err != nil {
		return errors.Wrap(err, "failed to encode last processed cursor")
	}

	logger.Log("event", "import.update_cursor", "cursor", lastCursor)
	return JobStore{tx}.UpdateCursor(ctx, i.Job.ID, string(lastCursorText))
}

// buildRelation generates the logical.Relation structure by querying Postgres catalog
// tables. Importantly, this populates the relation.Columns slice, providing type
// information that can later be used to marshal Golang types.
func (i Import) buildRelation(ctx context.Context, conn Connection) (*logical.Relation, error) {
	// Eg. oid = 16411, namespace = public, relname = example
	query := `
	select pg_class.oid as oid
	     , nspname as namespace
	     , relname as name
		from pg_class join pg_namespace on pg_class.relnamespace=pg_namespace.oid
	 where pg_class.oid = $1::regclass::oid;
	`

	relation := &logical.Relation{Columns: []logical.Column{}}
	err := conn.QueryRowEx(ctx, query, nil, i.TableName).Scan(&relation.ID, &relation.Namespace, &relation.Name)
	if err != nil {
		return nil, errors.Wrap(err, "failed to identify table namespace and name")
	}

	// Eg. name = id, type = 20
	columnQuery := `
	select attname as name
			 , atttypid as type
	  from pg_attribute
	 where attrelid = $1 and attnum > 0
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

// getPrimaryKeyColumn identifies the primary key column of the given table. It only
// supports tables with primary keys, and of those, only single column primary keys.
func getPrimaryKeyColumn(ctx context.Context, conn Connection, tableName string) (string, error) {
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

	if len(primaryKeys) != 1 {
		return "", multiplePrimaryKeysError(primaryKeys)
	}

	return primaryKeys[0], nil
}
