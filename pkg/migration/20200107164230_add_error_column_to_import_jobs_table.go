package migration

import (
	"database/sql"

	"github.com/pressly/goose"
)

func init() {
	goose.AddMigration(Up20200107164230, Down20200107164230)
}

func Up20200107164230(tx *sql.Tx) error {
	_, err := tx.Exec(`
	alter table pg2sink.import_jobs
	add column error text;
	`)

	return err
}

func Down20200107164230(tx *sql.Tx) error {
	_, err := tx.Exec(`
	alter table pg2sink.import_jobs
	drop column error;
	`)

	return err
}
