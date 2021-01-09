package migration

import (
	"database/sql"

	"github.com/pressly/goose"
)

func init() {
	goose.AddMigration(Up20200525133118, Down20200525133118)
}

func Up20200525133118(tx *sql.Tx) error {
	_, err := tx.Exec(`
	alter table import_jobs
	add column error text;
	`)

	return err
}

func Down20200525133118(tx *sql.Tx) error {
	_, err := tx.Exec(`
	alter table import_jobs
	drop column error text;
	`)

	return err
}
