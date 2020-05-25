package migration

import (
	"database/sql"
	"errors"

	"github.com/pressly/goose"
)

func init() {
	goose.AddMigration(Up20200510142436, Down20200510142436)
}

// Remove the import jobs table in favour of restructuring. We're not at v1.0 yet, so no
// need to maintain compatibility.
func Up20200510142436(tx *sql.Tx) error {
	_, err := tx.Exec(`
	drop table pgsink.import_jobs;
	`)

	return err
}

func Down20200510142436(tx *sql.Tx) error {
	return errors.New("irreversible")
}
