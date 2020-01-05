package migration

import (
	"database/sql"

	"github.com/pressly/goose"
)

func init() {
	goose.AddMigration(Up20191229145951, Down20191229145951)
}

func Up20191229145951(tx *sql.Tx) error {
	_, err := tx.Exec(`
	create table pg2sink.import_jobs (
		id bigserial primary key,
		publication_id text not null,
		table_name text not null,
		cursor text,
		completed_at timestamptz,
		created_at timestamptz not null default now()
	);
	`)

	return err
}

func Down20191229145951(tx *sql.Tx) error {
	_, err := tx.Exec(`
	drop table pg2sink.import_jobs;
	`)

	return err
}
