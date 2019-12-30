package pg2pubsub

import (
	"context"
	"time"

	"github.com/jackc/pgx"
)

type ImportJob struct {
	ID            int64
	PublicationID string
	TableName     string
	Cursor        *string
	CompletedAt   *time.Time
	CreatedAt     time.Time
}

type ImportJobStore struct {
	*pgx.ConnPool
}

func (i ImportJobStore) Get(ctx context.Context, id int64) (*ImportJob, error) {
	query := `
	select id, publication_id, table_name, cursor, completed_at, created_at
	from pg2pubsub.import_jobs
	where id = $1;
	`

	job := &ImportJob{}
	return job, i.QueryRowEx(ctx, query, nil, id).Scan(
		job.ID, job.PublicationID, job.TableName, job.Cursor, job.CompletedAt, job.CreatedAt,
	)
}

func (i ImportJobStore) Create(ctx context.Context, publicationID, tableName string) (*ImportJob, error) {
	query := `
	insert into pg2pubsub.import_jobs (publication_id, table_name) values (
		$1, $2
	)
	returning id, publication_id, table_name, cursor, completed_at, created_at
	;`

	job := &ImportJob{}
	return job, i.QueryRowEx(ctx, query, nil, publicationID, tableName).Scan(
		&job.ID, &job.PublicationID, &job.TableName, &job.Cursor, &job.CompletedAt, &job.CreatedAt,
	)
}

func (i ImportJobStore) GetImportedTables(ctx context.Context, publicationID string) ([]string, error) {
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
func (i ImportJobStore) GetOutstandingJobs(ctx context.Context, publicationID string, inProgress []int64) ([]*ImportJob, error) {
	query := `
	select id, publication_id, table_name, cursor, completed_at, created_at
	from pg2pubsub.import_jobs
	where publication_id = $1
	and completed_at is null
	and id not in $2
	;`

	jobs := []*ImportJob{}
	rows, err := i.QueryEx(ctx, query, nil, publicationID, inProgress)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		job := &ImportJob{}
		err := rows.Scan(&job.ID, &job.PublicationID, &job.TableName, &job.Cursor, &job.CompletedAt, &job.CreatedAt)
		if err != nil {
			return nil, err
		}

		jobs = append(jobs, job)
	}

	return jobs, nil
}
