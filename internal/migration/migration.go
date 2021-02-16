package migration

import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"

	kitlog "github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/pressly/goose"
)

func Migrate(ctx context.Context, logger kitlog.Logger, db *sql.DB, schemaName string) error {
	logger.Log("msg", "initialising schema", "schema", schemaName)
	if _, err := db.Exec(fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s;`, schemaName)); err != nil {
		return errors.Wrap(err, "failed to create schema")
	}

	logger.Log("msg", "running migrations", "schema", schemaName)
	goose.SetTableName(fmt.Sprintf("%s.schema_migrations", schemaName))

	// There is no good reason we have to do this, but goose chokes whenever we give it our
	// current directory with "no separator" errors. We don't even use text migrations in
	// this project, so create a random scratch directory and remove it as soon as we're
	// done.
	dir, err := ioutil.TempDir("", "goose-migrations-")
	if err != nil {
		return errors.Wrap(err, "failed to create scratch migration directory")
	}
	defer os.RemoveAll(dir)

	if err := goose.Up(db, dir); err != nil {
		return errors.Wrap(err, "failed to migrate database")
	}

	return nil
}
