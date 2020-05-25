package imports

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/lawrencejones/pgsink/pkg/dbschema/pgsink/model"
	. "github.com/lawrencejones/pgsink/pkg/dbschema/pgsink/table"
	"github.com/lawrencejones/pgsink/pkg/subscription"
	"github.com/lawrencejones/pgsink/pkg/util"

	"github.com/alecthomas/kingpin"
	. "github.com/go-jet/jet/postgres"
	kitlog "github.com/go-kit/kit/log"
)

type ManagerOptions struct {
	PollInterval time.Duration
}

func (opt *ManagerOptions) Bind(cmd *kingpin.CmdClause, prefix string) *ManagerOptions {
	cmd.Flag(fmt.Sprintf("%spoll-interval", prefix), "Interval to poll for newly subscribed tables").
		Default("30s").DurationVar(&opt.PollInterval)

	return opt
}

type Manager struct {
	logger   kitlog.Logger
	db       *sql.DB
	shutdown chan struct{}
	done     chan error
	opts     ManagerOptions
}

func NewManager(logger kitlog.Logger, db *sql.DB, opts ManagerOptions) *Manager {
	return &Manager{
		logger:   logger,
		db:       db,
		shutdown: make(chan struct{}),
		done:     make(chan error, 1), // buffered by 1, to ensure progress when reporting an error
		opts:     opts,
	}
}

func (m *Manager) Shutdown(ctx context.Context) error {
	close(m.shutdown)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-m.done:
		return err
	}
}

func (m *Manager) Manage(ctx context.Context, sub subscription.Subscription) error {
	defer func() {
		close(m.done)
	}()

	logger := kitlog.With(m.logger, "subscription_id", sub.GetID(), "publication_name", sub.Publication.Name)
	for {
		logger.Log("event", "reconcile_imports")
		jobs, err := m.Reconcile(ctx, sub)
		if err != nil {
			return err
		}

		for _, job := range jobs {
			logger.Log("event", "import_job.created",
				"import_job_id", job.ID,
				"import_job_table_name", job.TableName)
		}

		select {
		case <-ctx.Done():
			logger.Log("event", "finish", "msg", "context expired, finishing sync")
			return ctx.Err()
		case <-m.shutdown:
			logger.Log("event", "shutdown", "msg", "shutdown requested, exiting")
			return nil
		case <-time.After(m.opts.PollInterval):
			// continue
		}
	}
}

// Reconcile creates import jobs for tables registered in the subscription that have not
// yet been imported.
func (m *Manager) Reconcile(ctx context.Context, sub subscription.Subscription) ([]model.ImportJobs, error) {
	publishedTables, err := sub.GetTables(ctx, m.db)
	if err != nil {
		return nil, fmt.Errorf("failed to find already published tables: %w", err)
	}

	importedTables, err := m.getImportedTables(ctx, sub)
	if err != nil {
		return nil, fmt.Errorf("failed to find already imported tables: %w", err)
	}

	notImportedTables := util.Diff(publishedTables, importedTables)

	return m.create(ctx, sub, notImportedTables...)
}

func (m *Manager) getImportedTables(ctx context.Context, sub subscription.Subscription) ([]string, error) {
	stmt := SELECT(ImportJobs.TableName).
		FROM(ImportJobs).
		WHERE(
			ImportJobs.SubscriptionID.EQ(String(sub.GetID())).AND(
				// Filter out any jobs that have an expiry, as these imports are no longer valid
				ImportJobs.ExpiredAt.IS_NULL(),
			),
		)

	var tableNames []string
	if err := stmt.QueryContext(ctx, m.db, &tableNames); err != nil {
		return nil, err
	}

	return tableNames, nil
}

func (m *Manager) create(ctx context.Context, sub subscription.Subscription, tableNames ...string) ([]model.ImportJobs, error) {
	var jobs []model.ImportJobs
	if len(tableNames) == 0 {
		return jobs, nil
	}

	stmt := ImportJobs.
		INSERT(ImportJobs.SubscriptionID, ImportJobs.TableName).
		RETURNING(ImportJobs.AllColumns)

	for _, tableName := range tableNames {
		stmt = stmt.VALUES(sub.GetID(), tableName)
	}

	return jobs, stmt.QueryContext(ctx, m.db, &jobs)
}
