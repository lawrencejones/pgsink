package imports

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/lawrencejones/pgsink/internal/dbschema/pgsink/model"
	"github.com/lawrencejones/pgsink/internal/telem"
	"github.com/lawrencejones/pgsink/pkg/decode"
	"github.com/lawrencejones/pgsink/pkg/logical"
	"github.com/pkg/errors"

	kitlog "github.com/go-kit/kit/log"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"go.opencensus.io/trace"
)

// querier allows each helper to accept either transaction or connection objects
type querier interface {
	Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row
}

// textTranscodingScanner is a pgtype scanner that supports transcoding to text. This is
// how we store non-text formats inside the textual column of the import jobs table, for
// the purpose of cursoring.
type textTranscodingScanner interface {
	pgtype.Value
	pgtype.TextEncoder
	pgtype.TextDecoder
}

// Import is built for each job in the database, having resolved contextual information
// that can help run the job from the database whenever the job was enqueued.
type Import struct {
	Schema            string
	TableName         string
	PrimaryKey        string
	PrimaryKeyScanner textTranscodingScanner
	Relation          *logical.Relation
	Scanners          []decode.Scanner
	Destinations      []interface{}
	Cursor            interface{}
}

// Build queries the database for information required to perform an import, given an
// import job to process.
func Build(ctx context.Context, logger kitlog.Logger, decoder decode.Decoder, tx querier, job model.ImportJobs) (*Import, error) {
	ctx, span, logger := telem.Logger(ctx, logger)(trace.StartSpan(ctx, "pkg/imports.Build"))
	defer span.End()

	// We should query for the primary key as the first thing we do, as this may fail if the
	// table is misconfigured. It's better to fail here, before we've pushed anything into
	// the changelog, than after pushing the schema when we discover the table is
	// incompatible.
	logger.Log("event", "lookup_primary_key", "msg", "querying Postgres for relations primary key column")
	primaryKey, err := getPrimaryKeyColumn(ctx, tx, job.Schema, job.TableName)
	if err != nil {
		return nil, fmt.Errorf("failed to lookup primary key: %w", err)
	}

	logger.Log("event", "build_relation", "msg", "querying Postgres for relation type information")
	relation, err := buildRelation(ctx, tx, job.Schema, job.TableName, primaryKey)
	if err != nil {
		return nil, fmt.Errorf("failed to build relation for table: %w", err)
	}

	// Build scanners for decoding column types. We'll need the primary key scanner for
	// interpreting the cursor.
	primaryKeyScanner, scanners, destinations, err := buildScanners(relation, decoder, primaryKey)
	if err != nil {
		return nil, err
	}

	// We need to translate the import_jobs.cursor value, which is text, into a type that
	// will be supported for querying into the table. We can use the primaryKeyScanner for
	// this, which ensures we reliably encode/decode Postgres types.
	var cursor interface{}
	if job.Cursor != nil {
		if err := primaryKeyScanner.DecodeText(nil, []byte(*job.Cursor)); err != nil {
			return nil, errors.Wrap(err, "incompatible cursor in import_jobs table")
		}

		// We don't need to enforce our decoder's choice of type here, as we only ever use
		// EncodeText to retrieve the value, and anything that Get() produces should be
		// useable as a query parameter of this columns type.
		cursor = primaryKeyScanner.Get()
	}

	cfg := &Import{
		Schema:            job.Schema,
		TableName:         job.TableName,
		PrimaryKey:        primaryKey,
		PrimaryKeyScanner: primaryKeyScanner,
		Relation:          relation,
		Scanners:          scanners,
		Destinations:      destinations,
		Cursor:            cursor,
	}

	return cfg, nil
}

// buildScanners generates scanners that can be used to decode the import results. Each
// scanner is a separate instance, and will cache the scanned contents between subsequent
// scans: this is how we retrieve the primary key from the result of the last scan.
func buildScanners(relation *logical.Relation, decoder decode.Decoder, primaryKey string) (primaryKeyScanner textTranscodingScanner, scanners []decode.Scanner, destinations []interface{}, err error) {
	scanners = make([]decode.Scanner, len(relation.Columns))
	destinations = make([]interface{}, len(relation.Columns))

	for idx, column := range relation.Columns {
		scanner, dest, err := decoder.ScannerFor(column.Type)
		if err != nil {
			return nil, nil, nil, errors.Wrap(err, "table contains Postgres type that we cannot decode")
		}

		// Assign the scanner to match the column order
		scanners[idx] = scanner
		destinations[idx] = dest

		// If we're the primary key, we need the scanner to support text transcoding, so we
		// can store the cursor value as text into our field.
		if column.Name == primaryKey {
			scanner, ok := scanner.(textTranscodingScanner)
			if !ok {
				return nil, nil, nil, errors.Errorf(
					"primary key has oid that is not text transcodable (oid=%v), cannot import table", column.Type)
			}

			primaryKeyScanner = scanner
		}
	}

	return
}

// buildRelation generates the logical.Relation structure by querying Postgres catalog
// tables. Importantly, this populates the relation.Columns slice, providing type
// information that can later be used to marshal Golang types.
func buildRelation(ctx context.Context, tx querier, schema, tableName, primaryKeyColumn string) (*logical.Relation, error) {
	ctx, span := trace.StartSpan(ctx, "pkg/imports.buildRelation")
	defer span.End()

	// Eg. oid = 16411, namespace = public, relname = example
	query := `
	select pg_class.oid as oid
	     , nspname as namespace
	     , relname as name
		from pg_class join pg_namespace on pg_class.relnamespace=pg_namespace.oid
	 where nspname = $1 and relname = $2;
	`

	relation := &logical.Relation{Columns: []logical.Column{}}
	err := tx.QueryRow(ctx, query, schema, tableName).Scan(&relation.ID, &relation.Namespace, &relation.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to identify table with namespace '%s' and name '%s': %w", schema, tableName, err)
	}

	// Eg. name = id, type = 20
	columnQuery := `
	select attname as name
			 , atttypid as type
	  from pg_attribute
	 where attrelid = $1 and attnum > 0 and not attisdropped
	 order by attnum;
	`

	rows, err := tx.Query(ctx, columnQuery, relation.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to query pg_attribute for relation columns: %w", err)
	}

	defer rows.Close()

	for rows.Next() {
		column := logical.Column{}
		if err := rows.Scan(&column.Name, &column.Type); err != nil {
			return nil, err
		}

		// We don't strictly require this, but it's asking for trouble to generate
		// logical.Relation structs that have incorrect key metadata.
		if column.Name == primaryKeyColumn {
			column.Key = true
		}

		relation.Columns = append(relation.Columns, column)
	}

	return relation, nil
}

// buildQuery creates a query string and arguments for the configured relation. It knows
// how to build the query to incorporate the import cursor, and to provide the cursor
// value as an argument.
//
// Prepended to the columns is now(), which enables us to timestamp our imported rows to
// the database time.
func (i Import) buildQuery(limit int) (query string, args []interface{}) {
	columnNames := make([]string, len(i.Relation.Columns))
	for idx, column := range i.Relation.Columns {
		columnNames[idx] = column.Name
	}

	query = fmt.Sprintf(`SELECT NOW(), %s FROM %s.%s`, strings.Join(columnNames, ", "), i.Relation.Namespace, i.Relation.Name)
	if i.Cursor != nil {
		query += fmt.Sprintf(` WHERE %s > $1`, i.PrimaryKey)
		args = append(args, i.Cursor)
	}
	query += fmt.Sprintf(` order by %s limit %d`, i.PrimaryKey, limit)

	return
}

type multiplePrimaryKeysError []string

func (m multiplePrimaryKeysError) Error() string {
	return fmt.Sprintf("unsupported multiple primary keys: %s", strings.Join(m, ", "))
}

var NoPrimaryKeyError = fmt.Errorf("no primary key found")

// getPrimaryKeyColumn identifies the primary key column of the given table. It only
// supports tables with primary keys, and of those, only single column primary keys.
func getPrimaryKeyColumn(ctx context.Context, tx querier, schema, tableName string) (string, error) {
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
	err := tx.QueryRow(ctx, query, tableName).Scan(&primaryKeysTextArray)
	if err != nil {
		return "", err
	}

	var primaryKeys []string
	if err := primaryKeysTextArray.AssignTo(&primaryKeys); err != nil {
		return "", err
	}

	if len(primaryKeys) == 0 {
		return "", NoPrimaryKeyError
	} else if len(primaryKeys) > 1 {
		// Sort the keys to make any errors deterministic
		return "", multiplePrimaryKeysError(sort.StringSlice(primaryKeys))
	}

	return primaryKeys[0], nil
}
