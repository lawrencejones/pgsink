package publication

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgtype"
)

// GetPublishedTables returns a slice of table names that exist against the given
// publication. It will error if the publication does not exist, or if the identifier does
// not match.
func GetPublishedTables(ctx context.Context, pool *pgx.ConnPool, identifier string) ([]string, error) {
	// Careful! This query has been constructed so that the scan will fail if there is no
	// matching publication. It's important we do this so we can detect if a publication has
	// disappeared under us, so don't go changing this without understanding the
	// implications.
	query := `
	select array_remove(array_agg(schemaname || '.' || tablename), NULL)
	from pg_publication left join pg_publication_tables on pg_publication.pubname=pg_publication_tables.pubname
	where obj_description(pg_publication.oid, 'pg_publication') = $1
	group by pg_publication.pubname;
	`

	tablesReceiver := pgtype.TextArray{}
	var tables []string

	if err := pool.QueryRowEx(ctx, query, nil, identifier).Scan(&tablesReceiver); err != nil {
		return nil, err
	}

	return tables, tablesReceiver.AssignTo(&tables)
}

// Publication wraps a publication name
type Publication string

// AddTable adds the specified table into the publication
func (p Publication) AddTable(ctx context.Context, pool *pgx.ConnPool, table string) error {
	query := fmt.Sprintf(`alter publication %s add table %s;`, string(p), table)
	_, err := pool.ExecEx(ctx, query, nil)
	return err
}

// SetTables resets the publication to include the given tables only
func (p Publication) SetTables(ctx context.Context, pool *pgx.ConnPool, tables ...string) error {
	query := fmt.Sprintf(`alter publication %s set table %s;`, string(p), strings.Join(tables, ", "))
	_, err := pool.ExecEx(ctx, query, nil)
	return err
}

// RemoveTable adds the specified table into the publication
func (p Publication) RemoveTable(ctx context.Context, pool *pgx.ConnPool, table string) error {
	query := fmt.Sprintf(`alter publication %s remove table %s;`, string(p), table)
	_, err := pool.ExecEx(ctx, query, nil)
	return err
}
