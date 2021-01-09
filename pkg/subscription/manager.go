package subscription

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/lawrencejones/pgsink/pkg/changelog"
	_ "github.com/lawrencejones/pgsink/internal/dbschema/information_schema/model"
	isv "github.com/lawrencejones/pgsink/internal/dbschema/information_schema/view"

	"github.com/alecthomas/kingpin"
	pg "github.com/go-jet/jet/postgres"
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

// Manage begins syncing tables into publication using the rules configured on the manager
// options. It will run until the context expires.
func (m *Manager) Manage(ctx context.Context, sub Subscription) (err error) {
	defer func() {
		close(m.done)
	}()

	logger := kitlog.With(m.logger, "subscription_id", sub.GetID(), "publication_name", sub.Publication.Name)
	for {
		logger.Log("event", "reconcile_tables")
		added, removed, err := m.Reconcile(ctx, sub)
		if err != nil {
			return err
		}

		for _, tableName := range added {
			logger.Log("event", "alter_publication.add", "table", tableName)
		}

		for _, tableName := range removed {
			logger.Log("event", "alter_publication.remove", "table", tableName)
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

// Reconcile ensures all watched tables are added to the subscription, through the
// Postgres publication.
func (m *Manager) Reconcile(ctx context.Context, sub Subscription) (added changelog.Tables, removed changelog.Tables, err error) {
	watched, err := m.getWatchedTables(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to discover watched tables: %w", err)
	}

	published, err := sub.GetTables(ctx, m.db)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to query published tables: %w", err)
	}

	watchedNotPublished := watched.Diff(published)
	publishedNotWatched := published.Diff(watched)

	if (len(watchedNotPublished) > 0) || (len(publishedNotWatched) > 0) {
		if err := sub.SetTables(ctx, m.db, watched...); err != nil {
			return nil, nil, fmt.Errorf("failed to alter publication: %w", err)
		}
	}

	return watchedNotPublished, publishedNotWatched, nil
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
