package pg2pubsub

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/davecgh/go-spew/spew"
	kitlog "github.com/go-kit/kit/log"
	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgtype"
)

type ImporterOptions struct {
	WorkerCount   int           // maximum parallel import workers
	PublicationID string        // identifier of the current publication
	PollInterval  time.Duration // interval to poll for new import jobs
}

func NewImporter(logger kitlog.Logger, pool *pgx.ConnPool, opts ImporterOptions) *Importer {
	return &Importer{
		logger: logger,
		pool:   pool,
		opts:   opts,
	}
}

type Importer struct {
	logger kitlog.Logger
	pool   *pgx.ConnPool
	opts   ImporterOptions
}

// Work starts WorkerCount workers to process jobs in the import jobs table. It returns a
// channel of Committed structs, generated from the on-going imports. It will track
// progress in the import jobs table.
func (i Importer) Work(ctx context.Context) <-chan Committed {
	output := make(chan Committed)
	jobs := make(chan *ImportJob, i.opts.WorkerCount)
	inProgress := []int64{}

	var wg sync.WaitGroup

	// Acquire and enqueue import jobs
	go func() {
		for {
			i.logger.Log("event", "poll")
			outstandingJobs, err := ImportJobStore{i.pool}.GetOutstandingJobs(ctx, i.opts.PublicationID, inProgress)
			if err != nil {
				i.logger.Log("error", err.Error(), "msg", "failed to fetch outstanding jobs")
			}

			for _, job := range outstandingJobs[0:i.opts.WorkerCount] {
				inProgress = append(inProgress, job.ID)
				jobs <- job
			}

			select {
			case <-ctx.Done():
				close(jobs)
				wg.Wait()
				close(output)
				return
			case <-time.After(i.opts.PollInterval):
				// continue
			}
		}
	}()

	// Work each job that appears in the channel
	{
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				if err := i.work(ctx, job); err != nil {
					i.logger.Log("error", err, "job_id", job.ID)
				}
			}
		}()
	}

	return output
}

func (i Importer) work(ctx context.Context, job *ImportJob) error {
	logger := kitlog.With(i.logger, "job_id", job.ID, "table", job.TableName)
	logger.Log("event", "work")

	primaryKey, err := i.getPrimaryKeyColumn(ctx, job.TableName)
	if err != nil {
		return err
	}

	i.logger.Log("event", "found_primary_key", "primary_key", primaryKey)
	spew.Dump(job) // TODO: Implement

	return nil
}

type multiplePrimaryKeysError []string

func (m multiplePrimaryKeysError) Error() string {
	return fmt.Sprintf("unsupported multiple primary keys: %s", strings.Join(m, ","))
}

func (i Importer) getPrimaryKeyColumn(ctx context.Context, tableName string) (string, error) {
	query := `
	select array_agg(pg_attribute.attname)
	from pg_index join pg_attribute
	on pg_attribute.attrelid = pg_index.indrelid and pg_attribute.attnum = ANY(pg_index.indkey)
	where pg_index.indrelid = $1::regclass
	and pg_index.indisprimary;
	`

	primaryKeysTextArray := pgtype.TextArray{}
	err := i.pool.QueryRowEx(ctx, query, nil, tableName).Scan(&primaryKeysTextArray)
	if err != nil {
		return "", err
	}

	var primaryKeys []string
	if err := primaryKeysTextArray.AssignTo(&primaryKeys); err != nil {
		return "", err
	}

	if len(primaryKeys) != 1 {
		return "", multiplePrimaryKeysError(primaryKeys)
	}

	return primaryKeys[0], nil
}
