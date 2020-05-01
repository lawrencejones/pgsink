package publication

import (
	"context"
	"fmt"
	"strings"

	"github.com/lawrencejones/pg2sink/pkg/models"

	"github.com/jackc/pgtype"
)

// Publication wraps a publication name
type Publication string

// GetIdentifier finds the comment value assigned to the publication. This is where we
// store a uuid to uniquely identify a publication, and to detect when a publication has
// been dropped and recreated.
func (p Publication) GetIdentifier(ctx context.Context, conn models.Connection) (string, error) {
	query := `
	select obj_description(pg_publication.oid, 'pg_publication') as id
	from pg_publication
	where pubname = $1
	;`

	var identifier string
	if err := conn.QueryRow(ctx, query, string(p)).Scan(&identifier); err != nil {
		return "", err
	}

	return identifier, nil
}

// GetPublishedTables returns a slice of table names that exist against the given
// publication. It will error if the publication does not exist, or if the identifier does
// not match.
func (p Publication) GetPublishedTables(ctx context.Context, conn models.Connection, identifier string) ([]string, error) {
	// Careful! This query has been constructed so that the scan will fail if there is no
	// matching publication. It's important we do this so we can detect if a publication has
	// disappeared under us, so don't go changing this without understanding the
	// implications.
	query := `
	select array_remove(array_agg(schemaname || '.' || tablename), NULL)
	from pg_publication left join pg_publication_tables on pg_publication.pubname=pg_publication_tables.pubname
	where pg_publication.pubname = $1
	and obj_description(pg_publication.oid, 'pg_publication') = $2
	group by pg_publication.pubname;
	`

	tablesReceiver := pgtype.TextArray{}
	var tables []string

	if err := conn.QueryRow(ctx, query, string(p), identifier).Scan(&tablesReceiver); err != nil {
		return nil, err
	}

	return tables, tablesReceiver.AssignTo(&tables)
}

// AddTable adds the specified table into the publication
func (p Publication) AddTable(ctx context.Context, conn models.Connection, table string) error {
	query := fmt.Sprintf(`alter publication %s add table %s;`, string(p), table)
	_, err := conn.Exec(ctx, query)
	return err
}

// SetTables resets the publication to include the given tables only
func (p Publication) SetTables(ctx context.Context, conn models.Connection, tables ...string) error {
	query := fmt.Sprintf(`alter publication %s set table %s;`, string(p), strings.Join(tables, ", "))
	_, err := conn.Exec(ctx, query)
	return err
}

// DropTable removes the given table from the publication
func (p Publication) DropTable(ctx context.Context, conn models.Connection, table string) error {
	query := fmt.Sprintf(`alter publication %s drop table %s;`, string(p), table)
	_, err := conn.Exec(ctx, query)
	return err
}
