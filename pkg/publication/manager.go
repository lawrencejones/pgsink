package publication

import (
	"context"
	"fmt"
	"strings"
	"time"

	kitlog "github.com/go-kit/kit/log"
	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgtype"
	"github.com/lawrencejones/pg2sink/pkg/util"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

type ManagerOptions struct {
	Name         string        // name of the publication in Postgres
	Schemas      []string      // list of schemas to watch
	Excludes     []string      // optional blacklist
	Includes     []string      // optional whitelist, combined with blacklist
	PollInterval time.Duration // interval to scan Postgres for new matching tables
}

// CreateManager will create a new publication with no subscribed tables, or will no-op if
// a publication already exists with this name.
//
// The returned manager is configured to continually monitor the publication, adding or
// removing tables as appropriate.
func CreateManager(ctx context.Context, logger kitlog.Logger, pool *pgx.ConnPool, opts ManagerOptions) (*Manager, error) {
	logger = kitlog.With(logger, "publication", opts.Name)

	publicationID, err := getExistingPublicationID(ctx, pool, opts.Name)
	if err != nil {
		return nil, err
	}

	if publicationID == "" {
		// No publication exists, we need a new ID
		publicationID = uuid.NewV4().String()

		logger.Log("event", "publication.create", "publication_id", publicationID, "msg", "could not find publication, creating")
		if err := createPublication(ctx, pool, opts.Name, publicationID); err != nil {
			return nil, err
		}
	}

	logger = kitlog.With(logger, "publication_id", publicationID)
	logger.Log("event", "publication.init")

	pubmgr := &Manager{
		logger:        logger,
		pool:          pool,
		publicationID: publicationID,
		opts:          opts,
	}

	return pubmgr, nil
}

// getExistingPublicationID fetches the publicationID commented on the target publication, if it
// exists.
func getExistingPublicationID(ctx context.Context, pool *pgx.ConnPool, name string) (publicationID string, err error) {
	query := `
	select obj_description(oid, 'pg_publication') as id
	from pg_publication
	where pubname=$1
	`

	rows, err := pool.QueryEx(ctx, query, nil, name)
	if err != nil {
		return "", err
	}

	defer rows.Close()

	for rows.Next() {
		if err := rows.Scan(&publicationID); err != nil {
			return "", err
		}
	}

	return publicationID, err
}

// createPublication transactionally creates and comments on a new publication. The
// comment publicationID should be provided as a new UUID.
func createPublication(ctx context.Context, pool *pgx.ConnPool, name, publicationID string) error {
	createAndCommentQuery := fmt.Sprintf(`create publication %s;`, name) +
		fmt.Sprintf(`comment on publication %s is '%s';`, name, publicationID)

	txn, err := pool.Begin()
	if err != nil {
		return err
	}

	if _, err := txn.ExecEx(ctx, createAndCommentQuery, nil); err != nil {
		return err
	}

	if err := txn.Commit(); err != nil {
		return err
	}

	return nil
}

// Manager supervises a Postgres publication, adding and removing tables according to the
// white/blacklist.
type Manager struct {
	logger        kitlog.Logger
	pool          *pgx.ConnPool
	publicationID string
	opts          ManagerOptions
}

// GetPublicationID returns the publication ID. If empty, we haven't yet created our
// publication.
func (p *Manager) GetPublicationID() string {
	return p.publicationID
}

// Sync is intended to be run on an on-going basis, watching for new database tables in
// order to add them to the existing publication.
func (p *Manager) Sync(ctx context.Context) (err error) {
	p.logger.Log("event", "sync.start")
	for {
		select {
		case <-ctx.Done():
			p.logger.Log("event", "sync.finish", "msg", "context expired, finishing sync")
			return nil

		case <-time.After(p.opts.PollInterval):
			p.logger.Log("event", "sync.poll")
			watched, err := p.getWatchedTables(ctx)
			if err != nil {
				return errors.Wrap(err, "failed to discover watched tables")
			}

			published, err := GetPublishedTables(ctx, p.pool, p.publicationID)
			if err != nil {
				return errors.Wrap(err, "failed to query published tables")
			}

			watchedNotPublished := util.Diff(watched, published)
			publishedNotWatched := util.Diff(published, watched)

			if (len(watchedNotPublished) > 0) || (len(publishedNotWatched) > 0) {
				p.logger.Log("event", "alter_publication", "publication", p.opts.Name,
					"adding", strings.Join(watchedNotPublished, ","),
					"removing", strings.Join(publishedNotWatched, ","))

				if err := p.alter(ctx, watched); err != nil {
					return errors.Wrap(err, "failed to alter publication")
				}
			}
		}
	}
}

func (p *Manager) alter(ctx context.Context, tables []string) error {
	query := fmt.Sprintf(`alter publication %s set table %s;`, p.opts.Name, strings.Join(tables, ", "))
	_, err := p.pool.ExecEx(ctx, query, nil)

	return err
}

// getWatchedTables scans the database for tables that match our watch conditions
func (p *Manager) getWatchedTables(ctx context.Context) ([]string, error) {
	query := `
	select array_agg(table_schema || '.' || table_name)
	from information_schema.tables
	where table_schema = any($1::text[])
	and table_type = 'BASE TABLE';
	`

	tablesReceiver := pgtype.TextArray{}
	var tables []string

	if err := p.pool.QueryRowEx(ctx, query, nil, p.opts.Schemas).Scan(&tablesReceiver); err != nil {
		return nil, err
	}

	if err := tablesReceiver.AssignTo(&tables); err != nil {
		return nil, err
	}

	watchedTables := []string{}
forEachRow:
	for _, table := range tables {
		if util.Includes(p.opts.Excludes, table) {
			continue forEachRow
		}

		// If we have an includes list, we only include our table if it appears in that list
		isIncluded := len(p.opts.Includes) == 0
		isIncluded = isIncluded || util.Includes(p.opts.Includes, table)

		if !isIncluded {
			continue forEachRow
		}

		watchedTables = append(watchedTables, table)
	}

	return watchedTables, nil
}
