package subscription

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	kitlog "github.com/go-kit/kit/log"
	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v4"
)

type SubscriptionOptions struct {
	Name string // name of the publication, and prefix of replication slot
}

// Subscription is a wrapper around a Postgres publication and replication slot, coupled
// together via a unique identifier. Both replication slot and publication must be created
// before a subscription is used, to ensure Postgres retains unprocessed WAL from the
// moment the subscription is started.
//
// This implementation provides similar functionality to the CREATE SUBSCRIPTION command,
// in terms of managing the replication slot and providing a Start function that will
// subscribe to changes on the target publication.
//
// https://www.postgresql.org/docs/11/sql-createsubscription.html
type Subscription struct {
	Publication
	ReplicationSlot
	SubscriptionOptions
}

var NonReplicationConnection = errors.New("connection has not been created with replication=database")

// Create initialises a subscription once the publication and replication slot has been
// created. This is the only way to create a subscription, to ensure a replication slot
// exists before anyone can call Start().
func Create(ctx context.Context, logger kitlog.Logger, db *sql.DB, repconn *pgx.Conn, opts SubscriptionOptions) (*Subscription, error) {
	publication, err := FindOrCreatePublication(ctx, logger, db, opts.Name)
	if err != nil {
		return nil, err
	}

	// Validate the connection has activated replication mode, as the errors we get when
	// creating the replication slot are otherwise difficult to understand.
	//
	// https://www.postgresql.org/docs/current/protocol-replication.html
	if _, err := pglogrepl.IdentifySystem(ctx, repconn.PgConn()); err != nil {
		return nil, NonReplicationConnection
	}

	replicationSlot, err := findOrCreateReplicationSlot(ctx, logger, repconn.PgConn(), *publication)
	if err != nil {
		return nil, err
	}

	sub := &Subscription{
		Publication:         *publication,
		ReplicationSlot:     *replicationSlot,
		SubscriptionOptions: opts,
	}

	return sub, nil
}

// GetID is an easy accessor for code that needs the subscription ID. As we track and
// store the ID on the publication, it makes sense for the Publication struct to include
// it, but this is also the subscription ID.
func (s *Subscription) GetID() string {
	return s.Publication.ID
}

// Start begins replicating from our remote. We set our WAL position to whatever the
// server tells us our replication slot was last recorded at, then proceed to heartbeat
// and replicate our remote.
func (s *Subscription) Start(ctx context.Context, logger kitlog.Logger, conn *pgconn.PgConn, opts StreamOptions) (*Stream, error) {
	sysident, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil {
		return nil, err
	}

	logger.Log("event", "system_identification",
		"system_id", sysident.SystemID, "timeline", sysident.Timeline,
		"position", sysident.XLogPos, "database", sysident.DBName,
	)

	options := pglogrepl.StartReplicationOptions{
		Timeline: 0, // current server timeline
		Mode:     pglogrepl.LogicalReplication,
		PluginArgs: []string{
			`"proto_version" '1'`, fmt.Sprintf(`"publication_names" '%s'`, s.Publication.Name),
		},
	}

	logger.Log("event", "start_replication", "publication", s.Publication.Name, "slot", s.ReplicationSlot.Name)
	if err := pglogrepl.StartReplication(ctx, conn, s.ReplicationSlot.Name, sysident.XLogPos, options); err != nil {
		return nil, err
	}

	return stream(ctx, logger, conn, sysident, opts), nil
}
