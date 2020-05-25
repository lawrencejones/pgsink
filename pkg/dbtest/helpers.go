package dbtest

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/stdlib"

	_ "github.com/onsi/ginkgo"
	_ "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
	_ "github.com/onsi/gomega/gstruct"
)

// DB is used to help test interactions with Postgres. Unlike a conventional application,
// we create global state that lives outside of a schema, and require creation of tables
// with arbitrary schemas. We also reach deeper into the connection pools than most,
// requiring us to explicitly close any connections we create.
//
// This test helper can be used to ensure connections are closed, and test structure
// clean-up happens reliably.
type DB struct {
	db          *sql.DB
	repdb       *sql.DB
	schema      string
	connections []*pgx.Conn
	createFuncs []func(context.Context, *sql.DB, *sql.DB) (sql.Result, error)
	cleanFuncs  []func(context.Context, *sql.DB, *sql.DB) (sql.Result, error)
}

// defaultOptions is appended to all user supplied options
var defaultOptions = []func(*DB){
	WithTruncate("pgsink.import_jobs"),
}

func Configure(opts ...func(*DB)) *DB {
	dbtest := &DB{}
	for _, opt := range append(defaultOptions, opts...) {
		opt(dbtest)
	}

	return dbtest
}

func (d *DB) Setup(ctx context.Context, timeout time.Duration) (context.Context, func()) {
	ctx, cancel := context.WithTimeout(ctx, timeout)

	// Close any connections that we explicitly acquired
	for _, conn := range d.connections {
		Expect(conn.Close(ctx)).To(Succeed())
	}

	// Force close the connection pools, which shouldn't have any connections still open
	if d.db != nil {
		Expect(d.db.Close()).To(Succeed(), "closing database should always succeed")
	}
	if d.repdb != nil {
		Expect(d.repdb.Close()).To(Succeed(), "closing replication database should always succeed")
	}

	// Re-open connection pools with a search_path that matches our schema, preventing
	// accidental creation/querying of resources in the public namespace.
	var err error
	d.db, err = sql.Open("pgx", fmt.Sprintf("search_path=%s,public", d.schema))
	Expect(err).NotTo(HaveOccurred(), "failed to open database connection")

	d.repdb, err = sql.Open("pgx", fmt.Sprintf("search_path=%s,public replication=database", d.schema))
	Expect(err).NotTo(HaveOccurred(), "failed to open replication database connection")

	// In case previous tests exited abruptly, clean-up before we begin
	for _, clean := range d.cleanFuncs {
		_, err := clean(ctx, d.db, d.repdb)
		Expect(err).NotTo(HaveOccurred(), "failed to run cleanup before test start")
	}

	// Just before we begin testing, run all the creation functions
	for _, create := range d.createFuncs {
		_, err := create(ctx, d.db, d.repdb)
		Expect(err).NotTo(HaveOccurred(), "failed to run creation before test start")
	}

	return ctx, cancel
}

func (d *DB) MustExec(ctx context.Context, query string, args ...interface{}) {
	_, err := d.db.ExecContext(ctx, query, args...)
	Expect(err).NotTo(HaveOccurred())
}

func (d *DB) GetDB() *sql.DB {
	return d.db
}

type Option func(*DB)

func (o Option) And(other func(*DB)) Option {
	return func(db *DB) {
		o(db)
		other(db)
	}
}

func (d *DB) GetConnection(ctx context.Context) *pgx.Conn {
	conn, err := stdlib.AcquireConn(d.db)
	Expect(err).NotTo(HaveOccurred(), "failed to checkout connection")

	d.connections = append(d.connections, conn)

	return conn
}

func (d *DB) GetReplicationConnection(ctx context.Context) *pgx.Conn {
	conn, err := stdlib.AcquireConn(d.repdb)
	Expect(err).NotTo(HaveOccurred(), "failed to checkout replication connection")

	d.connections = append(d.connections, conn)

	return conn
}

func WithLifecycle(createFunc, cleanFunc func(context.Context, *sql.DB, *sql.DB) (sql.Result, error)) func(*DB) {
	return func(db *DB) {
		if createFunc != nil {
			db.createFuncs = append(db.createFuncs, createFunc)
		}
		if cleanFunc != nil {
			db.cleanFuncs = append(db.cleanFuncs, cleanFunc)
		}
	}
}

func WithReplicationSlotClean(prefix string) func(*DB) {
	query := `select pg_drop_replication_slot(slot_name) from pg_replication_slots where slot_name like '%s%%';`
	return WithLifecycle(
		nil, // allow the test to create this
		func(ctx context.Context, db, _ *sql.DB) (sql.Result, error) {
			return db.ExecContext(ctx, fmt.Sprintf(query, prefix))
		},
	)
}

func WithPublication(name string) func(*DB) {
	return Option(WithPublicationClean(name)).And(WithLifecycle(
		func(ctx context.Context, db, _ *sql.DB) (sql.Result, error) {
			return db.ExecContext(ctx, fmt.Sprintf(`create publication %s;`, name))
		},
		nil, // added by publication clean
	))
}

func WithPublicationClean(name string) func(*DB) {
	return WithLifecycle(
		nil, // allow the test to create this
		func(ctx context.Context, db, _ *sql.DB) (sql.Result, error) {
			return db.ExecContext(ctx, fmt.Sprintf(`drop publication if exists %s;`, name))
		},
	)
}

func WithSchema(name string) func(*DB) {
	return func(db *DB) {
		db.schema = name

		WithLifecycle(
			func(ctx context.Context, db, _ *sql.DB) (sql.Result, error) {
				return db.ExecContext(ctx, fmt.Sprintf(`create schema %s;`, name))
			},
			func(ctx context.Context, db, _ *sql.DB) (sql.Result, error) {
				return db.ExecContext(ctx, fmt.Sprintf(`drop schema if exists %s cascade;`, name))
			},
		)(db)
	}
}

func WithTruncate(name string) func(*DB) {
	return WithLifecycle(
		nil, // allow the test to create data
		func(ctx context.Context, db, _ *sql.DB) (sql.Result, error) {
			return db.ExecContext(ctx, fmt.Sprintf(`truncate %s;`, name))
		},
	)
}

func WithTable(name string, fieldDefinitions ...string) func(*DB) {
	return WithLifecycle(
		func(ctx context.Context, db, _ *sql.DB) (sql.Result, error) {
			return db.ExecContext(ctx, fmt.Sprintf("create table %s (%s);", name, strings.Join(fieldDefinitions, ", ")))
		},
		func(ctx context.Context, db, _ *sql.DB) (sql.Result, error) {
			return db.ExecContext(ctx, fmt.Sprintf(`drop table if exists %s cascade;`, name))
		},
	)
}
