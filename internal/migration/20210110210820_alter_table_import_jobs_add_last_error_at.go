package migration

import (
	"database/sql"

	"github.com/pressly/goose"
)

func init() {
	goose.AddMigration(Up20210110210820, Down20210110210820)
}

func Up20210110210820(tx *sql.Tx) error {
	_, err := tx.Exec(`
	alter table import_jobs
	add column last_error_at timestamptz;
	`)

	return err
}

func Down20210110210820(tx *sql.Tx) error {
	_, err := tx.Exec(`
	alter table import_jobs
	drop column last_error_at;
	`)

	return err
}
