package imports

import (
	"context"
	"time"

	"github.com/jackc/pgx"
)

type Job struct {
	ID            int64
	PublicationID string
	TableName     string
	Cursor        *string
	CompletedAt   *time.Time
	CreatedAt     time.Time
}

// JobStore provides storage methods for import job records.
type JobStore struct {
	Connection
}

type Connection interface {
	QueryRowEx(ctx context.Context, sql string, options *pgx.QueryExOptions, args ...interface{}) *pgx.Row
	QueryEx(ctx context.Context, sql string, options *pgx.QueryExOptions, args ...interface{}) (*pgx.Rows, error)
	ExecEx(ctx context.Context, sql string, options *pgx.QueryExOptions, args ...interface{}) (pgx.CommandTag, error)
}

// We want all these connection constructs to satisfy Connection
var _ Connection = &pgx.ConnPool{}
var _ Connection = &pgx.Conn{}
var _ Connection = &pgx.Tx{}

func (i JobStore) Get(ctx context.Context, id int64) (*Job, error) {
	query := `
	select id, publication_id, table_name, cursor, completed_at, created_at
	from pg2sink.import_jobs
	where id = $1;
	`

	job := &Job{}
	return job, i.QueryRowEx(ctx, query, nil, id).Scan(
		job.ID, job.PublicationID, job.TableName, job.Cursor, job.CompletedAt, job.CreatedAt,
	)
}

func (i JobStore) Create(ctx context.Context, publicationID, tableName string) (*Job, error) {
	query := `
	insert into pg2sink.import_jobs (publication_id, table_name) values (
		$1, $2
	)
	returning id, publication_id, table_name, cursor, completed_at, created_at
	;`

	job := &Job{}
	return job, i.QueryRowEx(ctx, query, nil, publicationID, tableName).Scan(
		&job.ID, &job.PublicationID, &job.TableName, &job.Cursor, &job.CompletedAt, &job.CreatedAt,
	)
}

// GetImportedTables finds all tables that have a corresponding import job for this
// publication
func (i JobStore) GetImportedTables(ctx context.Context, publicationID string) ([]string, error) {
	query := `
	select table_name from pg2sink.import_jobs where publication_id = $1;
	`

	tableNames := []string{}
	rows, err := i.QueryEx(ctx, query, nil, publicationID)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}

		tableNames = append(tableNames, tableName)
	}

	return tableNames, nil
}

// Acquire will return a job that is locked for the duration of the transaction. If no
// jobs are available, we return nil. We prioritise import jobs that have not yet failed,
// to ensure we don't get blocked by any failing import job.
func (i JobStore) Acquire(ctx context.Context, tx *pgx.Tx, publicationID string) (*Job, error) {
	query := `
	select id, publication_id, table_name, cursor, completed_at, created_at
	from pg2sink.import_jobs
	where publication_id = $1
	and completed_at is null
	order by error is null desc
	for update skip locked
	limit 1
	;`

	rows, err := tx.QueryEx(ctx, query, nil, publicationID)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() { // there will be at most one
		job := &Job{}
		return job, rows.Scan(
			&job.ID, &job.PublicationID, &job.TableName, &job.Cursor, &job.CompletedAt, &job.CreatedAt,
		)
	}

	return nil, nil
}

func (i JobStore) UpdateCursor(ctx context.Context, id int64, cursor string) error {
	query := `
	update pg2sink.import_jobs
	   set cursor = $2
	 where id = $1
	;`

	_, err := i.ExecEx(ctx, query, nil, id, cursor)
	return err
}

func (i JobStore) Complete(ctx context.Context, id int64) (time.Time, error) {
	query := `
	update pg2sink.import_jobs
	   set completed_at = now()
	 where id = $1
	returning completed_at
	;`

	var completedAt time.Time
	return completedAt, i.QueryRowEx(ctx, query, nil, id).Scan(&completedAt)
}

func (i JobStore) SetError(ctx context.Context, id int64, jobErr error) error {
	query := `
	update pg2sink.import_jobs
	   set error = $2
	 where id = $1
	;`

	_, err := i.ExecEx(ctx, query, nil, id, jobErr.Error())
	return err
}
