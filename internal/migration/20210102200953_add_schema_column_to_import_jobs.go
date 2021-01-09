package migration

import (
	"database/sql"

	"github.com/pressly/goose"
)

func init() {
	goose.AddMigration(Up20210102200953, Down20210102200953)
}

func Up20210102200953(tx *sql.Tx) error {
	_, err := tx.Exec(`
	alter table import_jobs
	add column schema text not null;
	`)

	return err
}

func Down20210102200953(tx *sql.Tx) error {
	_, err := tx.Exec(`
	alter table import_jobs
	drop column schema text;
	`)

	return err
}
