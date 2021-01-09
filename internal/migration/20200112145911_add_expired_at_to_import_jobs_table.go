package migration

import (
	"database/sql"

	"github.com/pressly/goose"
)

func init() {
	goose.AddMigration(Up20200112145911, Down20200112145911)
}

func Up20200112145911(tx *sql.Tx) error {
	_, err := tx.Exec(`
	alter table import_jobs
	add column expired_at timestamptz;
	`)

	return err
}

func Down20200112145911(tx *sql.Tx) error {
	_, err := tx.Exec(`
	alter table import_jobs
	drop column expired_at;
	`)

	return err
}
