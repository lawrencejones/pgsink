package models

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v4"
)

type ImportJob struct {
	ID               int64      // primary key of import job
	PublicationID    string     // uuid assigned to publication
	SubscriptionName string     // subscription name, should match replication slot name
	TableName        string     // target table, fully qualified (includes schema)
	Cursor           *string    // optional cursor, updated as the import job is worked
	CompletedAt      *time.Time // set when we finish the import job
	CreatedAt        time.Time  // set when first created
	ExpiresAt        *time.Time // set when this import has expired
}

type ImportJobStore struct{ Connection }

func (s ImportJobStore) Columns() []string {
	return []string{
		"id", "publication_id", "subscription_name", "table_name", "cursor", "completed_at", "created_at", "expired_at",
	}
}

func (s ImportJobStore) buildSelect(query string) string {
	return fmt.Sprintf(query, strings.Join(s.Columns(), ", "))
}

func (s *ImportJobStore) Scan(row interface {
	Scan(...interface{}) error
}) (*ImportJob, error) {
	j := &ImportJob{}
	return j, row.Scan(
		&j.ID,
		&j.PublicationID,
		&j.SubscriptionName,
		&j.TableName,
		&j.Cursor,
		&j.CompletedAt,
		&j.CreatedAt,
		&j.ExpiresAt,
	)
}

func (s ImportJobStore) Create(ctx context.Context, publicationID, subscriptionName, tableName string) (*ImportJob, error) {
	query := s.buildSelect(`
	insert into pg2sink.import_jobs (publication_id, subscription_name, table_name) values (
		$1, $2, $3
	)
	returning %s
	;`)

	return s.Scan(s.QueryRow(ctx, query, publicationID, subscriptionName, tableName))
}

// Where executes a SQL query with the given clause against the import_jobs table.
func (s ImportJobStore) Where(ctx context.Context, clause string, args ...interface{}) ([]*ImportJob, error) {
	query := s.buildSelect(`
	select %s
	from pg2sink.import_jobs
	where `) + clause + ";"

	rows, err := s.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	jobs := []*ImportJob{}
	for rows.Next() {
		job, err := s.Scan(rows)
		if err != nil {
			return nil, err
		}

		jobs = append(jobs, job)
	}

	return jobs, nil
}

// GetImportedTables finds all tables that have a corresponding import job for this
// publication. If an import has expired, it doesn't count.
func (s ImportJobStore) GetImportedTables(ctx context.Context, publicationID, subscriptionName string) ([]string, error) {
	query := `
	select table_name
	from pg2sink.import_jobs
	where publication_id = $1
	and subscription_name = $2
	and expired_at is null
	;`

	tableNames := []string{}
	rows, err := s.Query(ctx, query, publicationID, subscriptionName)
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
func (s ImportJobStore) Acquire(ctx context.Context, tx pgx.Tx, publicationID, subscriptionName string) (*ImportJob, error) {
	query := s.buildSelect(`
	select %s
	from pg2sink.import_jobs
	where publication_id = $1
	and subscription_name = $2
	and completed_at is null
	and expired_at is null
	order by error is null desc
	for update skip locked
	limit 1
	;`)

	rows, err := tx.Query(ctx, query, publicationID, subscriptionName)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() { // there will be at most one
		return s.Scan(rows)
	}

	return nil, nil
}

func (s ImportJobStore) UpdateCursor(ctx context.Context, id int64, cursor string) error {
	query := `
	update pg2sink.import_jobs
	   set cursor = $2
	 where id = $1
	;`

	_, err := s.Exec(ctx, query, id, cursor)
	return err
}

func (s ImportJobStore) Complete(ctx context.Context, id int64) (time.Time, error) {
	query := `
	update pg2sink.import_jobs
	   set completed_at = now()
	 where id = $1
	returning completed_at
	;`

	var completedAt time.Time
	return completedAt, s.QueryRow(ctx, query, id).Scan(&completedAt)
}

func (s ImportJobStore) Expire(ctx context.Context, id int64) (time.Time, error) {
	query := `
	update pg2sink.import_jobs
	   set expired_at = now()
	 where id = $1
	returning expired_at
	;`

	var expiredAt time.Time
	return expiredAt, s.QueryRow(ctx, query, id).Scan(&expiredAt)
}

func (s ImportJobStore) SetError(ctx context.Context, id int64, jobErr error) error {
	query := `
	update pg2sink.import_jobs
	   set error = $2
	 where id = $1
	;`

	_, err := s.Exec(ctx, query, id, jobErr.Error())
	return err
}
