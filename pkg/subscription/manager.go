package subscription

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/lawrencejones/pg2sink/pkg/util"

	"github.com/alecthomas/kingpin"
	kitlog "github.com/go-kit/kit/log"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4/stdlib"
	"github.com/pkg/errors"
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
	logger kitlog.Logger
	db     *sql.DB
	opts   ManagerOptions
}

func NewManager(logger kitlog.Logger, db *sql.DB, opts ManagerOptions) *Manager {
	return &Manager{
		logger: logger,
		db:     db,
		opts:   opts,
	}
}

// Manage begins syncing tables into publication using the rules configured on the manager
// options. It will run until the context expires.
func (m *Manager) Manage(ctx context.Context, publication Publication) (err error) {
	logger := kitlog.With(m.logger, "publication_name", publication.Name)
	for {
		logger.Log("event", "sync_published_tables")
		watched, err := m.getWatchedTables(ctx)
		if err != nil {
			return errors.Wrap(err, "failed to discover watched tables")
		}

		published, err := publication.GetTables(ctx, m.db)
		if err != nil {
			return errors.Wrap(err, "failed to query published tables")
		}

		watchedNotPublished := util.Diff(watched, published)
		publishedNotWatched := util.Diff(published, watched)

		if (len(watchedNotPublished) > 0) || (len(publishedNotWatched) > 0) {
			logger.Log("event", "alter_publication",
				"adding", strings.Join(watchedNotPublished, ","),
				"removing", strings.Join(publishedNotWatched, ","))

			if err := publication.SetTables(ctx, m.db, watched...); err != nil {
				return fmt.Errorf("failed to alter publication: %w", err)
			}
		}

		select {
		case <-ctx.Done():
			logger.Log("event", "finish", "msg", "context expired, finishing sync")
			return nil
		case <-time.After(m.opts.PollInterval):
			// continue
		}
	}
}

// getWatchedTables scans the database for tables that match our watch conditions
func (m *Manager) getWatchedTables(ctx context.Context) ([]string, error) {
	query := `
	select array_agg(table_schema || '.' || table_name)
	from information_schema.tables
	where table_schema = any($1::text[])
	and table_type = 'BASE TABLE';
	`

	// Drop into pgx to provide support for any, which is incompatible with db.SQL
	conn, err := stdlib.AcquireConn(m.db)
	if err != nil {
		return nil, err
	}
	defer stdlib.ReleaseConn(m.db, conn)

	tablesReceiver := pgtype.TextArray{}
	var tables []string
	if err := conn.QueryRow(ctx, query, m.opts.Schemas).Scan(&tablesReceiver); err != nil {
		return nil, err
	}

	if err := tablesReceiver.AssignTo(&tables); err != nil {
		return nil, err
	}

	watchedTables := []string{}
forEachRow:
	for _, table := range tables {
		if util.Includes(m.opts.Excludes, table) {
			continue forEachRow
		}

		// If we have an includes list, we only include our table if it appears in that list
		isIncluded := len(m.opts.Includes) == 0
		isIncluded = isIncluded || util.Includes(m.opts.Includes, table)

		if !isIncluded {
			continue forEachRow
		}

		watchedTables = append(watchedTables, table)
	}

	return watchedTables, nil
}