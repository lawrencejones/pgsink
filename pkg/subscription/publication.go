package subscription

import (
	"context"
	"fmt"
	"strings"

	kitlog "github.com/go-kit/kit/log"
	"github.com/google/uuid"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
)

// Publication represents a Postgres publication, the publishing component of a
// subscription. It is coupled with a ReplicationSlot as a component of a Subscription.
type Publication struct {
	Name string
	ID   string
}

// GetReplicationSlotName generates the name of the replication slot associated with the
// publication. The ID is used to prove a slot was created against the existing
// publication, and to catch when publications have been dropped/recreated.
func (p Publication) GetReplicationSlotName() string {
	return fmt.Sprintf("%s_%s", p.Name, p.ID)
}

// String provides an easy printer for anything using publications
func (p Publication) String() string {
	return fmt.Sprintf("%s[%s]", p.Name, p.ID)
}

// GetTables returns a slice of table names that are included on the publication.
func (p Publication) GetTables(ctx context.Context, conn querier) ([]string, error) {
	// Careful! This query has been constructed so that the scan will fail if there is no
	// matching publication. It's important we do this so we can detect if a publication has
	// disappeared under us, so change this without care.
	query := `
	select array_remove(array_agg(schemaname || '.' || tablename), NULL)
	from pg_publication left join pg_publication_tables on pg_publication.pubname=pg_publication_tables.pubname
	where pg_publication.pubname = $1
	group by pg_publication.pubname;
	`

	tablesReceiver := pgtype.TextArray{}
	var tables []string
	if err := conn.QueryRow(ctx, query, p.Name).Scan(&tablesReceiver); err != nil {
		return nil, err
	}

	return tables, tablesReceiver.AssignTo(&tables)
}

// SetTables resets the publication to include the given tables only
func (p Publication) SetTables(ctx context.Context, conn querier, tables ...string) error {
	query := fmt.Sprintf(`alter publication %s set table %s;`, p.Name, strings.Join(tables, ", "))
	_, err := conn.Exec(ctx, query)
	return err
}

// querier allows use of various pgx constructs in place of a concrete type
type querier interface {
	Begin(ctx context.Context) (pgx.Tx, error)
	Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row
	Exec(ctx context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error)
}

// findOrCreatePublication will attempt to find an existing publication, or create from
// scratch with a fresh publication ID.
func findOrCreatePublication(ctx context.Context, logger kitlog.Logger, conn querier, name string) (*Publication, error) {
	logger = kitlog.With(logger, "publication_name", name)

	publication, err := getPublication(ctx, conn, name)
	if err != nil {
		return nil, fmt.Errorf("failed to find publication: %w", err)
	}

	if publication == nil {
		logger.Log("event", "publication.create", "msg", "could not find publication, creating")
		publication, err = createPublication(ctx, conn, name, newShortID())
		if err != nil {
			return nil, fmt.Errorf("failed to create publication: %w", err)
		}
	}

	logger.Log("event", "publication.found", "publication_id", publication.ID)
	return publication, err
}

// createPublication transactionally creates and comments on a new publication. The
// comment will be the unique subscription identifier.
func createPublication(ctx context.Context, conn querier, name, id string) (*Publication, error) {
	txn, err := conn.Begin(ctx)
	if err != nil {
		return nil, err
	}

	createQuery := fmt.Sprintf(`create publication %s;`, name)
	if _, err := txn.Exec(ctx, createQuery); err != nil {
		return nil, err
	}

	commentQuery := fmt.Sprintf(`comment on publication %s is '%s';`, name, id)
	if _, err := txn.Exec(ctx, commentQuery); err != nil {
		return nil, err
	}

	if err := txn.Commit(ctx); err != nil {
		return nil, err
	}

	return &Publication{Name: name, ID: id}, nil
}

func getPublication(ctx context.Context, conn querier, name string) (*Publication, error) {
	query := `
	select
		pubname,
		obj_description(oid, 'pg_publication')
	from pg_publication
	where pubname=$1
	limit 1
	`

	var pub Publication
	err := conn.QueryRow(ctx, query, name).Scan(&pub.Name, &pub.ID)
	if err == pgx.ErrNoRows {
		return nil, nil
	}

	return &pub, nil
}

// newShortID creates an abbreviated ID from a uuid, preferring the first component which
// is generated from time.
func newShortID() string {
	return strings.SplitN(uuid.New().String(), "-", 2)[0]
}
