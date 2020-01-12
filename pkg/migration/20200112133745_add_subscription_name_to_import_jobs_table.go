package migration

import (
	"database/sql"

	"github.com/pressly/goose"
)

func init() {
	goose.AddMigration(Up20200112133745, Down20200112133745)
}

func Up20200112133745(tx *sql.Tx) error {
	_, err := tx.Exec(`
	alter table pg2sink.import_jobs
	add column subscription_name text default '' not null;
	`)

	return err
}

func Down20200112133745(tx *sql.Tx) error {
	_, err := tx.Exec(`
	alter table pg2sink.import_jobs
	drop column subscription_name;
	`)

	return err
}
