package migration

import (
	"context"
	"database/sql"

	kitlog "github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/pressly/goose"
)

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
