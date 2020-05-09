package subscription

import (
	"context"

	"github.com/lawrencejones/pg2sink/pkg/logical"

	kitlog "github.com/go-kit/kit/log"
	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
)

// ReplicationSlot represents a replication slot inside of Postgres, helping track changes
// on a publication. These slots are created with just a single output format, which is
// pg_output. They are permanent, not temporary.
type ReplicationSlot struct {
	Name string
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
