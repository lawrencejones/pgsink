package pg2pubsub

import (
	"context"
	"fmt"
	"strings"
	"time"

	kitlog "github.com/go-kit/kit/log"
	"github.com/jackc/pgx"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

type PublicationManagerOptions struct {
	Name         string        // name of the publication in Postgres
	Schemas      []string      // list of schemas to watch
	Excludes     []string      // optional blacklist
	Includes     []string      // optional whitelist, combined with blacklist
	PollInterval time.Duration // interval to scan Postgres for new matching tables
}

func NewPublicationManager(logger kitlog.Logger, pool *pgx.ConnPool, opts PublicationManagerOptions) *PublicationManager {
	return &PublicationManager{
		logger: logger,
		pool:   pool,
		opts:   opts,
	}
}

type PublicationManager struct {
	logger     kitlog.Logger
	pool       *pgx.ConnPool
	identifier string // publication identifier, set by create
	opts       PublicationManagerOptions
}

// Create will create a new publication with no subscribed tables, or will no-op if a
// publication already exists with this name. The returned string is the publication
// identifer, which is set to a uuid as a comment on the publication. Whenever a
// publication expires, it should be recreated with a different identifier.
func (p *PublicationManager) Create(ctx context.Context) (identifier string, err error) {
	logger := kitlog.With(p.logger, "publication", p.opts.Name)

	defer func() {
		// We've successfully created the publication, so initialise our manager
		if err == nil {
			logger.Log("event", "publication.initialise", "identifier", identifier)
			p.identifier = identifier
		}
	}()

	identifier, found, err := p.getExistingIdentifier(ctx)
	if err != nil {
		return "", err
	}

	if found {
		return identifier, nil
	}

	// No publication exists, we need a new identifier
	identifier = uuid.NewV4().String()
	logger.Log("event", "publication.create", "identifier", identifier, "msg", "could not find publication, creating")
	if err := p.createPublication(ctx, identifier); err != nil {
		return "", err
	}

	return identifier, nil
}

// createPublication transactionally creates and comments on a new publication. The
// comment identifier should be provided as a new UUID.
func (p *PublicationManager) createPublication(ctx context.Context, identifier string) error {
	createAndCommentQuery := fmt.Sprintf(`create publication %s;`, p.opts.Name) +
		fmt.Sprintf(`comment on publication %s is '%s';`, p.opts.Name, identifier)

	txn, err := p.pool.Begin()
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

// getExistingIdentifier fetches the identifier commented on the target publication, if it
// exists. Otherwise we return an empty string, which means a matching publication does
// not exist.
func (p *PublicationManager) getExistingIdentifier(ctx context.Context) (identifier string, found bool, err error) {
	matchingIdentifierQuery := `
	select obj_description(oid, 'pg_publication') as identifier
	from pg_publication
	where pubname=$1
	`

	rows, err := p.pool.QueryEx(ctx, matchingIdentifierQuery, nil, p.opts.Name)
	if err != nil {
		return "", false, err
	}

	defer rows.Close()

	for rows.Next() {
		found = true // any matching publication means we've found it
		if err := rows.Scan(&identifier); err != nil {
			return "", found, err
		}
	}

	return identifier, found, nil
}

// Sync is intended to be run on an on-going basis, watching for new database tables in
// order to add them to the existing publication.
func (p *PublicationManager) Sync(ctx context.Context) error {
	if p.identifier == "" {
		panic("called Sync() before Create()")
	}

	p.logger.Log("event", "sync_start")
	for {
		select {
		case <-ctx.Done():
			p.logger.Log("event", "sync_finish", "msg", "context expired, finishing sync")
			return nil

		case <-time.After(p.opts.PollInterval):
			p.logger.Log("event", "sync")
			watched, err := p.getWatchedTables(ctx)
			if err != nil {
				return err
			}

			published, err := p.GetPublishedTables(ctx)
			if err != nil {
				return err
			}

			importedTables, err := ImportJobStore{p.pool}.GetImportedTables(ctx, p.identifier)
			if err != nil {
				return err
			}

			watchedNotPublished := diff(watched, published)
			publishedNotWatched := diff(published, watched)
			publishedNotImported := diff(published, importedTables)

			for _, tableName := range publishedNotImported {
				p.logger.Log("event", "import.enqueue", "publication_id", p.identifier, "table", tableName)
				_, err := ImportJobStore{p.pool}.Create(ctx, p.identifier, tableName)
				if err != nil {
					return errors.Wrap(err, "failed to enqueue import job")
				}
			}

			if (len(watchedNotPublished) > 0) || (len(publishedNotWatched) > 0) {
				p.logger.Log("event", "alter_publication", "publication", p.opts.Name,
					"adding", strings.Join(watchedNotPublished, ","),
					"removing", strings.Join(publishedNotWatched, ","))

				if err := p.alter(ctx, watched); err != nil {
					return err
				}
			}
		}
	}
}

// GetIdentifier returns the publication ID. If empty, we haven't yet created our
// publication.
func (p *PublicationManager) GetIdentifier() string {
	return p.identifier
}

func (p *PublicationManager) alter(ctx context.Context, tables []string) error {
	query := fmt.Sprintf(`alter publication %s set table %s;`, p.opts.Name, strings.Join(tables, ", "))
	_, err := p.pool.ExecEx(ctx, query, nil)

	return err
}

// GetPublishedTables collects the tables that are already configured on our publication
func (p *PublicationManager) GetPublishedTables(ctx context.Context) ([]string, error) {
	query := `select schemaname, tablename from pg_publication_tables where pubname = $1;`
	rows, err := p.pool.QueryEx(ctx, query, nil, p.opts.Name)
	if err != nil {
		return nil, err
	}

	return p.scanTables(rows)
}

// getWatchedTables scans the database for tables that match our watched conditions
func (p *PublicationManager) getWatchedTables(ctx context.Context) ([]string, error) {
	schemaPattern := strings.Join(p.opts.Schemas, "|")
	query := `select table_schema, table_name
	from information_schema.tables
	where table_schema similar to $1
	and table_type = 'BASE TABLE';`

	rows, err := p.pool.QueryEx(ctx, query, nil, schemaPattern)
	if err != nil {
		return nil, err
	}

	return p.scanTables(rows)
}

// enqueueImport adds a new job to the import_jobs table. This will be picked up by the
// importer, and will backfill what was missed from this subscription.
func (p *PublicationManager) enqueueImport(ctx context.Context, table string) (*ImportJob, error) {
	return ImportJobStore{p.pool}.Create(ctx, p.identifier, table)
}

// scanTables collects table names in <schema>.<name> form from sql queries that return
// tuples of (schema, name)
func (p *PublicationManager) scanTables(rows *pgx.Rows) ([]string, error) {
	defer rows.Close()

	var tables = []string{}
forEachRow:
	for rows.Next() {
		var schema, name string
		if err := rows.Scan(&schema, &name); err != nil {
			return nil, err
		}

		// Filter the table if we intend to exclude it
		for _, excluded := range p.opts.Excludes {
			if excluded == name {
				continue forEachRow
			}
		}

		// If we have an includes list, we should only include our table if it appears in that
		// list
		isIncluded := len(p.opts.Includes) == 0
		for _, included := range p.opts.Includes {
			if included == name {
				isIncluded = true
			}
		}

		if isIncluded {
			tables = append(tables, fmt.Sprintf("%s.%s", schema, name))
		}
	}

	return tables, nil
}

func diff(s1 []string, s2 []string) []string {
	result := make([]string, 0)
	for _, s := range s1 {
		if !includes(s2, s) {
			result = append(result, s)
		}
	}

	return result
}

func includes(ss []string, s string) bool {
	for _, existing := range ss {
		if existing == s {
			return true
		}
	}

	return false
}
