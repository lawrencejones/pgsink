package publication

import (
	"context"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgtype"
)

// GetPublishedTables returns a slice of table names that exist against the given
// publication
func GetPublishedTables(ctx context.Context, pool *pgx.ConnPool, name string) ([]string, error) {
	query := `
	select array_agg(schemaname || '.' || tablename)
	from pg_publication_tables
	where pubname = $1;
	`

	tablesReceiver := pgtype.TextArray{}
	var tables []string

	if err := pool.QueryRowEx(ctx, query, nil, name).Scan(&tablesReceiver); err != nil {
		return nil, err
	}

	return tables, tablesReceiver.AssignTo(&tables)
}
