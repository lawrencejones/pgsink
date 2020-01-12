package imports

import (
	"context"
	"time"

	kitlog "github.com/go-kit/kit/log"
	"github.com/lawrencejones/pg2sink/pkg/models"
	"github.com/lawrencejones/pg2sink/pkg/publication"
	"github.com/lawrencejones/pg2sink/pkg/util"
	"github.com/pkg/errors"
)

type ManagerOptions struct {
	PublicationID    string        // identifier of the current publication
	SubscriptionName string        // name of subscription (should be the replication slot name)
	PollInterval     time.Duration // interval to poll for new import jobs
}

func NewManager(logger kitlog.Logger, conn models.Connection, opts ManagerOptions) *Manager {
	return &Manager{
		logger: logger,
		conn:   conn,
		opts:   opts,
	}
}

// Manager ensures we create import jobs for all tables that have not yet been imported.
type Manager struct {
	logger kitlog.Logger
	conn   models.Connection
	opts   ManagerOptions
}

// Work starts WorkerCount workers to process jobs in the import jobs table. It returns a
// channel of Committed structs, generated from the on-going imports. It will track
// progress in the import jobs table.

// Sync watches the publication to identify tables which have not yet been imported, and
// adds import jobs for those tables.
func (i Manager) Sync(ctx context.Context) error {
	logger := kitlog.With(i.logger, "publication_id", i.opts.PublicationID)
	jobStore := models.ImportJobStore{i.conn}

	for {
		logger.Log("event", "sync.poll")
		publishedTables, err := publication.GetPublishedTables(ctx, i.conn, i.opts.PublicationID)
		if err != nil {
			return errors.Wrap(err, "failed to query published tables")
		}

		importedTables, err := jobStore.GetImportedTables(ctx, i.opts.PublicationID, i.opts.SubscriptionName)
		if err != nil {
			return errors.Wrap(err, "failed to query imported tables")
		}

		notImportedTables := util.Diff(publishedTables, importedTables)
		for _, table := range notImportedTables {
			logger.Log("event", "import_job.create", "table", table)
			job, err := jobStore.Create(ctx, i.opts.PublicationID, i.opts.SubscriptionName, table)
			if err != nil {
				return errors.Wrap(err, "failed to create import job")
			}

			logger.Log("event", "import_job.created", "table", table, "job_id", job.ID)
		}

		select {
		case <-ctx.Done():
			logger.Log("event", "sync.finish", "msg", "context expired, finishing sync")
			return nil
		case <-time.After(i.opts.PollInterval):
			// continue
		}
	}
}
