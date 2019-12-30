package pg2pubsub

import (
	"context"
	"sync"
	"time"

	"github.com/davecgh/go-spew/spew"
	kitlog "github.com/go-kit/kit/log"
	"github.com/jackc/pgx"
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
	jobs := make(chan *ImportJob)
	inProgress := []int64{}

	var wg sync.WaitGroup

	// Acquire and enqueue import jobs
	go func() {
		select {
		case <-ctx.Done(): // shutdown
			close(jobs)
			wg.Wait()
			close(output)

		case <-time.After(i.opts.PollInterval): // every PollInterval
			outstandingJobs, err := ImportJobStore{i.pool}.GetOutstandingJobs(ctx, i.opts.PublicationID, inProgress)
			if err != nil {
				i.logger.Log("error", err.Error(), "msg", "failed to fetch outstanding jobs")
				break
			}

			for _, job := range outstandingJobs {
				inProgress = append(inProgress, job.ID)
				jobs <- job
			}
		}
	}()

	// Work each job that appears in the channel
	{
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				i.work(ctx, job)
			}
		}()
	}

	return output
}

func (i Importer) work(ctx context.Context, job *ImportJob) error {
	spew.Dump(job) // TODO: Implement
	return nil
}
