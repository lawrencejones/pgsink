package subscription

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	kitlog "github.com/go-kit/kit/log"
	"github.com/google/uuid"
	"github.com/jackc/pgtype"
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
func (p Publication) GetTables(ctx context.Context, db *sql.DB) (tables []string, err error) {
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
	if err := db.QueryRowContext(ctx, query, p.Name).Scan(&tablesReceiver); err != nil {
		return nil, err
	}

	return tables, tablesReceiver.AssignTo(&tables)
}

// SetTables resets the publication to include the given tables only
func (p Publication) SetTables(ctx context.Context, db *sql.DB, tables ...string) error {
	query := fmt.Sprintf(`alter publication %s set table %s;`, p.Name, strings.Join(tables, ", "))
	_, err := db.ExecContext(ctx, query)
	return err
}

// findOrCreatePublication will attempt to find an existing publication, or create from
// scratch with a fresh publication ID.
func findOrCreatePublication(ctx context.Context, logger kitlog.Logger, db *sql.DB, name string) (*Publication, error) {
	logger = kitlog.With(logger, "publication_name", name)

	publication, err := getPublication(ctx, db, name)
	if err != nil {
		return nil, fmt.Errorf("failed to find publication: %w", err)
	}

	if publication == nil {
		logger.Log("event", "publication.create", "msg", "could not find publication, creating")
		publication, err = createPublication(ctx, db, name, newShortID())
		if err != nil {
			return nil, fmt.Errorf("failed to create publication: %w", err)
		}
	}

	logger.Log("event", "publication.found", "publication_id", publication.ID)
	return publication, err
}

// createPublication transactionally creates and comments on a new publication. The
// comment will be the unique subscription identifier.
func createPublication(ctx context.Context, db *sql.DB, name, id string) (*Publication, error) {
	txn, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	createQuery := fmt.Sprintf(`create publication %s;`, name)
	if _, err := txn.ExecContext(ctx, createQuery); err != nil {
		return nil, err
	}

	commentQuery := fmt.Sprintf(`comment on publication %s is '%s';`, name, id)
	if _, err := txn.ExecContext(ctx, commentQuery); err != nil {
		return nil, err
	}

	if err := txn.Commit(); err != nil {
		return nil, err
	}

	return &Publication{Name: name, ID: id}, nil
}

func getPublication(ctx context.Context, db *sql.DB, name string) (*Publication, error) {
	query := `
	select
		pubname,
		obj_description(oid, 'pg_publication')
	from pg_publication
	where pubname=$1
	limit 1
	`

	var pub Publication
	err := db.QueryRowContext(ctx, query, name).Scan(&pub.Name, &pub.ID)
	if err == sql.ErrNoRows {
		return nil, nil
	}

	return &pub, nil
}

// newShortID creates an abbreviated ID from a uuid, preferring the first component which
// is generated from time.
func newShortID() string {
	return strings.SplitN(uuid.New().String(), "-", 2)[0]
}
