package migration

import (
	"database/sql"

	"github.com/pressly/goose"
)

func init() {
	goose.AddMigration(Up20200510142527, Down20200510142527)
}

// Recreate import jobs with a cleaner structure.
func Up20200510142527(tx *sql.Tx) error {
	_, err := tx.Exec(`
	create table pgsink.import_jobs (
		id bigserial primary key,
		subscription_id text not null,
		table_name text not null,
		cursor text,
		completed_at timestamptz,
		expired_at timestamptz,
		updated_at timestamptz not null default now(),
		created_at timestamptz not null default now()
	);
	`)

	return err
}

func Down20200510142527(tx *sql.Tx) error {
	_, err := tx.Exec(`
	drop table pgsink.import_jobs;
	`)

	return err
}
