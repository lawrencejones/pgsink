package migration

import (
	"database/sql"

	"github.com/pressly/goose"
)

func init() {
	goose.AddMigration(Up20200525133118, Down20200525133118)
}

func Up20200525133118(tx *sql.Tx) error {
	return exec(tx, `
	alter table pgsink.import_jobs
	add column error text;
	`)
}

func Down20200525133118(tx *sql.Tx) error {
	return exec(tx, `
	alter table pgsink.import_jobs
	drop column error text;
	`)
}
