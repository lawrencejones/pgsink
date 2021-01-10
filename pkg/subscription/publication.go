package subscription

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/lawrencejones/pgsink/pkg/changelog"
	_ "github.com/lawrencejones/pgsink/internal/dbschema/pg_catalog/model"
	. "github.com/lawrencejones/pgsink/internal/dbschema/pg_catalog/table"
	. "github.com/lawrencejones/pgsink/internal/dbschema/pg_catalog/view"

	. "github.com/go-jet/jet/v2/postgres"
	kitlog "github.com/go-kit/kit/log"
	"github.com/google/uuid"
)

// FindOrCreatePublication will attempt to find an existing publication, or create from
// scratch with a fresh publication ID.
func FindOrCreatePublication(ctx context.Context, logger kitlog.Logger, db *sql.DB, name string) (*Publication, error) {
	logger = kitlog.With(logger, "publication_name", name)

	publication, err := FindPublication(ctx, db, name)
	if err != nil {
		return nil, fmt.Errorf("failed to find publication: %w", err)
	}

	if publication == nil {
		logger.Log("event", "publication.create", "msg", "could not find publication, creating")
		publication, err = CreatePublication(ctx, db, name, newShortID())
		if err != nil {
			return nil, fmt.Errorf("failed to create publication: %w", err)
		}
	}

	logger.Log("event", "publication.found", "publication_id", publication.ID)
	return publication, err
}

// CreatePublication transactionally creates and comments on a new publication. The
// comment will be the unique subscription identifier.
func CreatePublication(ctx context.Context, db *sql.DB, name, id string) (*Publication, error) {
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

// FindPublication attempts to find an existing publication by the given name.
func FindPublication(ctx context.Context, db *sql.DB, name string) (*Publication, error) {
	query, args := PgPublication.
		SELECT(
			PgPublication.Pubname.AS("name"),
			Raw("obj_description(oid, 'pg_publication')").AS("id"),
		).
		WHERE(PgPublication.Pubname.EQ(String(name))).
		LIMIT(1).
		Sql()

	var pub Publication
	if err := db.QueryRowContext(ctx, query, args...).Scan(&pub.Name, &pub.ID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			err = nil
		}

		return nil, err
	}

	return &pub, nil
}

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
func (p Publication) GetTables(ctx context.Context, db *sql.DB) (tables changelog.Tables, err error) {
	stmt := PgPublication.
		INNER_JOIN(PgPublicationTables, PgPublicationTables.Pubname.EQ(PgPublication.Pubname)).
		SELECT(
			PgPublicationTables.Schemaname.AS("table.schema"),
			PgPublicationTables.Tablename.AS("table.table_name"),
		).
		WHERE(PgPublication.Pubname.EQ(String(p.Name)))

	if err := stmt.QueryContext(ctx, db, &tables); err != nil {
		return nil, err
	}

	return tables, nil
}

// SetTables resets the publication to include the given tables only
func (p Publication) SetTables(ctx context.Context, db *sql.DB, tables ...changelog.Table) error {
	var fullyQualifiedTableNames []string
	for _, table := range tables {
		fullyQualifiedTableNames = append(fullyQualifiedTableNames, table.String())
	}

	query := fmt.Sprintf(`alter publication %s set table %s;`, p.Name, strings.Join(fullyQualifiedTableNames, ", "))
	_, err := db.ExecContext(ctx, query)
	return err
}

// newShortID creates an abbreviated ID from a uuid, preferring the first component which
// is generated from time.
func newShortID() string {
	return strings.SplitN(uuid.New().String(), "-", 2)[0]
}
