package migration

import (
	"context"
	"database/sql"

	kitlog "github.com/go-kit/kit/log"
	"github.com/jackc/pgx"
	"github.com/jackc/pgx/stdlib"
	"github.com/pkg/errors"
	"github.com/pressly/goose"
)

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
