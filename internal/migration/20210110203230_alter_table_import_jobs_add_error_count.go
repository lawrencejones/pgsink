package migration

import (
	"database/sql"

	"github.com/pressly/goose"
)

func init() {
	goose.AddMigration(Up20210110203230, Down20210110203230)
}

func Up20210110203230(tx *sql.Tx) error {
	_, err := tx.Exec(`
	alter table import_jobs
	add column error_count bigint default 0 not null;
	`)

	return err
}

func Down20210110203230(tx *sql.Tx) error {
	_, err := tx.Exec(`
	alter table import_jobs
	drop column error_count;
	`)

	return err
}
