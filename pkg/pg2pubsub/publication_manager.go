package pg2pubsub

import (
	"context"
	"fmt"
	"strings"
	"time"

	kitlog "github.com/go-kit/kit/log"
	"github.com/jackc/pgx"
)

type PublicationManagerOptions struct {
	Name         string
	Schemas      []string
	Excludes     []string
	PollInterval time.Duration
}

func NewPublicationManager(logger kitlog.Logger, conn *pgx.Conn, opts PublicationManagerOptions) *publicationManager {
	return &publicationManager{
		logger: logger,
		conn:   conn,
		opts:   opts,
	}
}

type publicationManager struct {
	logger kitlog.Logger
	conn   *pgx.Conn
	opts   PublicationManagerOptions
}

func (p *publicationManager) Create(ctx context.Context) error {
	rows, err := p.conn.QueryEx(ctx, `select exists(select * from pg_publication where pubname=$1);`, nil, p.opts.Name)
	if err != nil {
		return err
	}

	defer rows.Close()

	var exists bool
	for rows.Next() {
		if err := rows.Scan(&exists); err != nil {
			return err
		}
	}

	if exists {
		p.logger.Log("event", "publication_exists", "publication", p.opts.Name)
		return nil
	}

	p.logger.Log("event", "creating_publication", "publication", p.opts.Name,
		"msg", "could not find publication, creating")
	_, err = p.conn.ExecEx(ctx, fmt.Sprintf(`create publication %s;`, p.opts.Name), nil)

	return err
}

func (p *publicationManager) Sync(ctx context.Context) error {
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

			published, err := p.getPublishedTables(ctx)
			if err != nil {
				return err
			}

			watchedNotPublished := diff(watched, published)
			publishedNotWatched := diff(published, watched)

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

func (p *publicationManager) alter(ctx context.Context, tables []string) error {
	query := fmt.Sprintf(`alter publication %s set table %s;`, p.opts.Name, strings.Join(tables, ", "))
	_, err := p.conn.ExecEx(ctx, query, nil)
	return err
}

// getPublishedTables collects the tables that are already configured on our publication
func (p *publicationManager) getPublishedTables(ctx context.Context) ([]string, error) {
	query := `select schemaname, tablename from pg_publication_tables where pubname = $1;`
	rows, err := p.conn.QueryEx(ctx, query, nil, p.opts.Name)
	if err != nil {
		return nil, err
	}

	return p.scanTables(rows)
}

// getWatchedTables scans the database for tables that match our watched conditions
func (p *publicationManager) getWatchedTables(ctx context.Context) ([]string, error) {
	schemaPattern := strings.Join(p.opts.Schemas, "|")
	query := `select table_schema, table_name
	from information_schema.tables
	where table_schema similar to $1;`

	rows, err := p.conn.QueryEx(ctx, query, nil, schemaPattern)
	if err != nil {
		return nil, err
	}

	return p.scanTables(rows)
}

// scanTables collects table names in <schema>.<name> form from sql queries that return
// tuples of (schema, name)
func (p *publicationManager) scanTables(rows *pgx.Rows) ([]string, error) {
	defer rows.Close()

	var tables = []string{}
	for rows.Next() {
		var schema, name string
		if err := rows.Scan(&schema, &name); err != nil {
			return nil, err
		}

		tables = append(tables, fmt.Sprintf("%s.%s", schema, name))
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
