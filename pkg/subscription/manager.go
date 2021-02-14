package subscription

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lawrencejones/pgsink/internal/dbschema/information_schema/model"
	isv "github.com/lawrencejones/pgsink/internal/dbschema/information_schema/view"
	"github.com/lawrencejones/pgsink/internal/telem"
	"github.com/lawrencejones/pgsink/pkg/changelog"
	"go.opencensus.io/trace"

	"github.com/alecthomas/kingpin"
	pg "github.com/go-jet/jet/v2/postgres"
	kitlog "github.com/go-kit/kit/log"
)

type ManagerOptions struct {
	Schemas      []string      // list of schemas to watch
	Excludes     []string      // optional blacklist
	Includes     []string      // optional whitelist, combined with blacklist
	PollInterval time.Duration // interval to scan Postgres for new matching tables
}

func (opt *ManagerOptions) Bind(cmd *kingpin.CmdClause, prefix string) *ManagerOptions {
	cmd.Flag(fmt.Sprintf("%sschema", prefix), "Postgres schema to watch for changes").Default("public").StringsVar(&opt.Schemas)
	cmd.Flag(fmt.Sprintf("%sexclude", prefix), "Table name to exclude from changes").StringsVar(&opt.Excludes)
	cmd.Flag(fmt.Sprintf("%sinclude", prefix), "Table name to include from changes (activates whitelist)").StringsVar(&opt.Includes)
	cmd.Flag(fmt.Sprintf("%spoll-interval", prefix), "Interval to poll for new tables").Default("10s").DurationVar(&opt.PollInterval)

	return opt
}

// Manager supervises a subscription, adding and removing tables into the publication
// according to the white/blacklist.
type Manager struct {
	db       *sql.DB        // handle the the database, used query and modify the subscription resources
	shutdown chan struct{}  // closed when shutdown requested, allowing each component to terminate gracefully
	done     chan error     // closed when all components have shutdown, with an error if any failed
	opts     ManagerOptions // options provided when constructing the manager
}

func NewManager(db *sql.DB, opts ManagerOptions) *Manager {
	return &Manager{
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

// Manage begins syncing tables into publication using the rules configured on the manager
// options. It will run until the context expires.
func (m *Manager) Manage(ctx context.Context, logger kitlog.Logger, sub Subscription) (err error) {
	defer func() {
		m.done <- err
		close(m.done)
	}()

	logger = kitlog.With(logger, "subscription_id", sub.GetID(), "publication_name", sub.Publication.Name)
	for {
		if err := m.manage(ctx, logger, sub); err != nil {
			return err
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

// manage implements the Manage loop, allowing us to generate a new trace per iteration.
func (m *Manager) manage(ctx context.Context, logger kitlog.Logger, sub Subscription) error {
	ctx, span, logger := telem.StartSpan(ctx, "pkg/subscription.Manager.manage")
	defer span.End()

	logger.Log("event", "reconcile_tables")
	added, removed, err := m.Reconcile(ctx, sub)
	if err != nil {
		span.SetStatus(trace.Status{
			Code:    trace.StatusCodeUnknown,
			Message: err.Error(),
		})
		return err
	}

	for _, tableName := range added {
		logger.Log("event", "alter_publication.add", "table", tableName)
	}

	for _, tableName := range removed {
		logger.Log("event", "alter_publication.remove", "table", tableName)
	}

	return nil
}

// Reconcile ensures all watched tables are added to the subscription, through the
// Postgres publication.
func (m *Manager) Reconcile(ctx context.Context, sub Subscription) (watchedNotPublished changelog.Tables, publishedNotWatched changelog.Tables, err error) {
	err = sub.Begin(ctx, m.db, func(session PublicationSession) error {
		watched, err := m.getWatchedTables(ctx)
		if err != nil {
			return fmt.Errorf("failed to discover watched tables: %w", err)
		}

		published, err := session.GetTables(ctx, m.db)
		if err != nil {
			return fmt.Errorf("failed to query published tables: %w", err)
		}

		watchedNotPublished = watched.Diff(published)
		publishedNotWatched = published.Diff(watched)

		if (len(watchedNotPublished) > 0) || (len(publishedNotWatched) > 0) {
			if err := session.SetTables(ctx, m.db, watched...); err != nil {
				return fmt.Errorf("failed to alter publication: %w", err)
			}
		}

		return nil
	})

	return
}

// getWatchedTables scans the database for tables that match our watch conditions
func (m *Manager) getWatchedTables(ctx context.Context) (changelog.Tables, error) {
	stmt := isv.Tables.
		SELECT(
			isv.Tables.TableSchema.AS("table.schema"),
			isv.Tables.TableName.AS("table.table_name"),
		).
		WHERE(isv.Tables.TableType.EQ(pg.String("BASE TABLE")))

	var allTables []changelog.Table
	if err := stmt.QueryContext(ctx, m.db, &allTables); err != nil {
		return nil, err
	}

	var tables changelog.Tables
	for _, table := range allTables {
		for _, schema := range m.opts.Schemas {
			if table.Schema == schema {
				tables = append(tables, table)
			}
		}
	}

	var watchedTables changelog.Tables
forEachRow:
	for _, table := range tables {
		for _, excluded := range m.opts.Excludes {
			if excluded == table.String() {
				continue forEachRow
			}
		}

		// If we have an includes list, we only include our table if it appears in that list
		isIncluded := len(m.opts.Includes) == 0
		isIncluded = isIncluded || m.includes(table)

		if !isIncluded {
			continue forEachRow
		}

		watchedTables = append(watchedTables, table)
	}

	return watchedTables, nil
}

func (m *Manager) includes(table changelog.Table) bool {
	for _, included := range m.opts.Includes {
		if included == table.String() {
			return true
		}
	}

	return false
}
