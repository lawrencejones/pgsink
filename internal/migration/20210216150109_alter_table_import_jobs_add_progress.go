package migration

import (
	"database/sql"
	"github.com/pressly/goose"
)

func init() {
	goose.AddMigration(Up20210216150109, Down20210216150109)
}

func Up20210216150109(tx *sql.Tx) error {
	_, err := tx.Exec(`
	alter table import_jobs
	add column rows_processed_total bigint default 0 not null;
	`)

	return err
}

func Down20210216150109(tx *sql.Tx) error {
	_, err := tx.Exec(`
	alter table import_jobs
	drop column rows_processed_total;
	`)

	return err
}
