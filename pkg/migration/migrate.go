package migration

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	kitlog "github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/pressly/goose"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

// BeforeSuite is a Ginkgo hook that runs database migrations whenever imported into a
// Ginkgo test suite. This will be a no-op whenever Ginkgo is not running.
var _ = BeforeSuite(func() {
	db, err := sql.Open("pgx", "")
	Expect(err).NotTo(HaveOccurred(), "failed to connect to database")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	Expect(
		Migrate(ctx, kitlog.NewLogfmtLogger(GinkgoWriter), db),
	).To(
		Succeed(), "failed to migrate test database",
	)
})

// tableNames contains all the tables managed by pgsink, so we can truncate them
// automatically in our test suites.
var tableNames = []string{
	"pgsink.import_jobs",
}

// Truncate all tables before the start of each test, ensuring we start from a blank slate
// for each test.
var _ = BeforeEach(func() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db, err := sql.Open("pgx", "")
	Expect(err).NotTo(HaveOccurred(), "failed to connect to database")

	for _, tableName := range tableNames {
		_, err = db.ExecContext(ctx, fmt.Sprintf(`truncate %s;`, tableName))
		Expect(err).NotTo(HaveOccurred())
	}
})

// exec is a helper to clean-up most migrations, where we want to return the error given
// by a SQL operation and no more.
func exec(tx *sql.Tx, query string, args ...interface{}) error {
	_, err := tx.Exec(query, args...)
	return err
}

func Migrate(ctx context.Context, logger kitlog.Logger, db *sql.DB) error {
	logger.Log("event", "schema.create", "schema", "pgsink")
	if _, err := db.ExecContext(ctx, `create schema if not exists pgsink;`); err != nil {
		return errors.Wrap(err, "failed to create internal schema")
	}

	goose.SetTableName("pgsink.schema_migrations")
	if err := goose.Up(db, "."); err != nil {
		return errors.Wrap(err, "failed to migrate database")
	}

	return nil
}
