package migration

import (
	"context"
	"database/sql"
	"time"

	kitlog "github.com/go-kit/kit/log"
	"github.com/jackc/pgx"
	"github.com/jackc/pgx/stdlib"
	"github.com/pkg/errors"
	"github.com/pressly/goose"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

// BeforeSuite is a Ginkgo hook that runs database migrations whenever imported into a
// Ginkgo test suite. This will be a no-op whenever Ginkgo is not running.
var _ = BeforeSuite(func() {
	cfg, err := pgx.ParseEnvLibpq()
	Expect(err).NotTo(HaveOccurred(), "failed to connect to database")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	Expect(
		Migrate(ctx, kitlog.NewLogfmtLogger(GinkgoWriter), cfg),
	).To(
		Succeed(), "failed to migrate test database",
	)
})

func Migrate(ctx context.Context, logger kitlog.Logger, cfg pgx.ConnConfig) error {
	driverCfg := &stdlib.DriverConfig{ConnConfig: cfg}
	stdlib.RegisterDriverConfig(driverCfg)
	db, err := sql.Open("pgx", driverCfg.ConnectionString(""))
	if err != nil {
		return errors.Wrap(err, "failed to create database connection")
	}

	logger.Log("event", "schema.create", "schema", "pg2pubsub")
	_, err = db.ExecContext(ctx, `create schema if not exists pg2pubsub;`)
	if err != nil {
		return errors.Wrap(err, "failed to create internal schema")
	}

	goose.SetTableName("pg2pubsub.schema_migrations")
	if err := goose.Up(db, "."); err != nil {
		return errors.Wrap(err, "failed to migrate database")
	}

	return nil
}
