package imports

import (
	"context"
	"fmt"
	"strings"

	kitlog "github.com/go-kit/kit/log"
	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgtype"
	"github.com/lawrencejones/pg2sink/pkg/logical"
	"github.com/lawrencejones/pg2sink/pkg/models"
	"github.com/lawrencejones/pg2sink/pkg/util"
	"github.com/pkg/errors"
	"go.opencensus.io/trace"
)

// JobConfig is built for each job by querying the underlying database. It uses runtime
// information about the schema to interp
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