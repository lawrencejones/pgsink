package subscription

import (
	"context"

	"github.com/lawrencejones/pg2sink/pkg/logical"

	_ "github.com/lawrencejones/pg2sink/pkg/dbschema/pg_catalog/model"
	_ "github.com/lawrencejones/pg2sink/pkg/dbschema/pg_catalog/table"
	. "github.com/lawrencejones/pg2sink/pkg/dbschema/pg_catalog/view"

	. "github.com/go-jet/jet/postgres"
	kitlog "github.com/go-kit/kit/log"
	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v4"
)

// ReplicationSlot represents a replication slot inside of Postgres, helping track changes
// on a publication. These slots are created with just a single output format, which is
// pg_output. They are permanent, not temporary.
type ReplicationSlot struct {
	Name string
}

func (s ReplicationSlot) GetConfirmedFlushLSN(ctx context.Context, conn *pgx.Conn) (lsn pglogrepl.LSN, err error) {
	query, queryArgs := PgReplicationSlots.
		SELECT(
			// The connection this method receives will often be a replication connection. If
			// so, it's unlikely to have performed the standard start-up procedure where oids
			// are shared, so the pg_lsn type will be unrecognised. Cast to text as this will
			// almost always work.
			CAST(PgReplicationSlots.ConfirmedFlushLsn).AS_TEXT(),
		).
		WHERE(PgReplicationSlots.SlotName.EQ(String(s.Name))).
		Sql()

	// The replication protocol does not support the extended protocol. Provide the sentinel
	// QuerySimpleProtocol setting to tell pgx to go simple for this query only, allowing
	// this method to be called regardless of whether replication is enabled.
	args := []interface{}{pgx.QuerySimpleProtocol(true)}
	args = append(args, queryArgs...)

	var lsnText string
	if err := conn.QueryRow(ctx, query, args...).Scan(&lsnText); err != nil {
		return lsn, err
	}

	return pglogrepl.ParseLSN(lsnText)
}

// findOrCreateReplicationSlot generates a replication slot for a publication. All we need
// to create a slot is the name, but we should only create slots once a publication has
// been initialised.
func findOrCreateReplicationSlot(ctx context.Context, logger kitlog.Logger, conn *pgconn.PgConn, publication Publication) (slot *ReplicationSlot, err error) {
	slot = &ReplicationSlot{Name: publication.GetReplicationSlotName()}
	logger = kitlog.With(logger, "event", "create_replication_slot", "slot", slot.Name)
	defer func() {
		logger.Log("event", "create_replication_slot", "error", err)
	}()

	_, err = pglogrepl.CreateReplicationSlot(ctx, conn, slot.Name, logical.PGOutput, pglogrepl.CreateReplicationSlotOptions{})
	if err != nil {
		if err, ok := err.(*pgconn.PgError); ok {
			if err.Code == "42710" {
				logger = kitlog.With(logger, "detail", "slot_already_exists")
				return slot, nil
			}
		}
	}

	return slot, err
}
