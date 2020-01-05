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
	*pgx.ConnPool
}

func (i JobStore) Get(ctx context.Context, id int64) (*Job, error) {
	query := `
	select id, publication_id, table_name, cursor, completed_at, created_at
	from pg2pubsub.import_jobs
	where id = $1;
	`

	job := &Job{}
	return job, i.QueryRowEx(ctx, query, nil, id).Scan(
		job.ID, job.PublicationID, job.TableName, job.Cursor, job.CompletedAt, job.CreatedAt,
	)
}

func (i JobStore) Create(ctx context.Context, publicationID, tableName string) (*Job, error) {
	query := `
	insert into pg2pubsub.import_jobs (publication_id, table_name) values (
		$1, $2
	)
	returning id, publication_id, table_name, cursor, completed_at, created_at
	;`

	job := &Job{}
	return job, i.QueryRowEx(ctx, query, nil, publicationID, tableName).Scan(
		&job.ID, &job.PublicationID, &job.TableName, &job.Cursor, &job.CompletedAt, &job.CreatedAt,
	)
}

func (i JobStore) MarkAsComplete(ctx context.Context, job *Job) error {
	query := `
	update pg2pubsub.import_jobs
	   set completed_at = now()
	 where id = $1
	returning completed_at
	;`

	return i.QueryRowEx(ctx, query, nil, job.ID).Scan(&job.CompletedAt)
}

func (i JobStore) UpdateCursor(ctx context.Context, id int64, cursor interface{}) error {
	query := `
	update pg2pubsub.import_jobs
	   set cursor = $2
	 where id = $1
	;`

	_, err := i.ExecEx(ctx, query, nil, id, cursor)
	return err
}

// GetImportedTables finds all tables that have a corresponding import job for this
// publication
func (i JobStore) GetImportedTables(ctx context.Context, publicationID string) ([]string, error) {
	query := `
	select table_name from pg2pubsub.import_jobs where publication_id = $1;
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

// GetOutstandingJobs finds any incomplete jobs that match the given publication, that
// aren't currently in progress. Until all these import jobs have been processed, we won't
// have completely synced all data.
func (i JobStore) GetOutstandingJobs(ctx context.Context, publicationID string, inProgress []int64) ([]*Job, error) {
	query := `
	select id, publication_id, table_name, cursor, completed_at, created_at
	from pg2pubsub.import_jobs
	where publication_id = $1
	and completed_at is null
	and not id = any($2)
	;`

	jobs := []*Job{}
	rows, err := i.QueryEx(ctx, query, nil, publicationID, inProgress)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		job := &Job{}
		err := rows.Scan(&job.ID, &job.PublicationID, &job.TableName, &job.Cursor, &job.CompletedAt, &job.CreatedAt)
		if err != nil {
			return nil, err
		}

		jobs = append(jobs, job)
	}

	return jobs, nil
}
