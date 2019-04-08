package pg2pubsub

import (
	"context"
	"fmt"
	"time"

	"github.com/davecgh/go-spew/spew"
	kitlog "github.com/go-kit/kit/log"
	"github.com/jackc/pgx"
	"github.com/oklog/run"
)

// PGOutput is the Postgres recognised name of our desired encoding
const PGOutput = "pgoutput"

// Subscription provides the implementation of logical replication from a Postgres
// primary. It implements the functionality of CREATE SUBSCRIPTION in terms of managing
// the replication slot and provides a Start function that will subscribe to changes on
// the target publication.
//
// https://www.postgresql.org/docs/11/sql-createsubscription.html
type Subscription struct {
	logger kitlog.Logger
	conn   *pgx.ReplicationConn
	opts   SubscriptionOptions
}

func NewSubscription(logger kitlog.Logger, conn *pgx.ReplicationConn, opts SubscriptionOptions) *Subscription {
	return &Subscription{
		logger: logger,
		conn:   conn,
		opts:   opts,
	}
}

type SubscriptionOptions struct {
	Name            string
	Publication     string
	StatusHeartbeat time.Duration
}

func (s *Subscription) CreateReplicationSlot(ctx context.Context) error {
	logger := kitlog.With(s.logger, "slot", s.opts.Name)
	logger.Log("event", "create_replication_slot")

	if err := s.conn.CreateReplicationSlot(s.opts.Name, PGOutput); err != nil {
		if err, ok := err.(pgx.PgError); ok {
			if err.Code == "42710" {
				logger.Log("event", "slot_already_exists")
				return nil
			}
		}

		return err
	}

	return nil
}

type SubscriptionHandler func(Message) error

func (s *Subscription) Start(ctx context.Context, handler SubscriptionHandler) error {
	s.logger.Log("event", "start_replication", "publication", s.opts.Publication, "slot", s.opts.Name)
	pluginArguments := []string{
		`"proto_version" '1'`, fmt.Sprintf(`"publication_names" '%s'`, s.opts.Publication),
	}
	if err := s.conn.StartReplication(s.opts.Name, 0, -1, pluginArguments...); err != nil {
		return err
	}

	// Logical replication tracks three wal positions, flush apply and write. We don't have
	// the usual database semantics, so can probably get away with tracking just one
	// position, the minimum guaranteed-to-be-written LSN.
	var walPos uint64

	{
		s.logger.Log("event", "wait_for_heartbeat")

		ctx, cancel := context.WithTimeout(ctx, s.opts.StatusHeartbeat)
		defer cancel()

		for {
			msg, err := s.conn.WaitForReplicationMessage(ctx)
			if err != nil {
				return err
			}

			if msg.ServerHeartbeat != nil {
				walPos = msg.ServerHeartbeat.ServerWalEnd
				withPos(s.logger, walPos).Log("event", "initial_heartbeat_received")

				break
			}
		}
	}

	var g run.Group

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	g.Add(
		func() error { return s.startHeartbeats(ctx, &walPos) },
		func(error) { cancel() },
	)

	g.Add(
		func() error { return s.startReceiving(ctx, &walPos) },
		func(error) { cancel() },
	)

	return g.Run()
}

// startReceiving will receive messages until the context expires, or we receive an error
// from our connection.
func (s *Subscription) startReceiving(ctx context.Context, walPos *uint64) error {
	for {
		msg, err := s.conn.WaitForReplicationMessage(ctx)

		// If our context has expired then we want to quit now, as it will be the cause of
		// our error.
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		if err != nil {
			s.logger.Log("error", err, "msg", "failed to receive for replication message")
			return err
		}

		if msg.WalMessage != nil {
			if msg, err := DecodePGOutput(msg.WalMessage.WalData); err != nil {
				s.logger.Log("error", err)
			} else {
				spew.Dump(msg)
			}
		}
	}
}

// startHeartbeats sends standby statuses to our replication primary to confirm we've
// successfully applied wal and to keepalive our connection.
func (s *Subscription) startHeartbeats(ctx context.Context, walPos *uint64) error {
	ticker := time.NewTicker(s.opts.StatusHeartbeat)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.logger.Log("event", "status_heartbeat_stop", "msg", "context expired, finish heartbeating")
			err := s.sendStandbyStatus(*walPos)
			if err != nil { // send update before we quit
				s.logger.Log("error", err, "msg", "final heartbeat failed, expect to replay changes on next boot")
			}

			return err

		case <-ticker.C:
			if err := s.sendStandbyStatus(*walPos); err != nil {
				s.logger.Log("error", err, "msg", "failed to send status heartbeat")
			}
		}
	}
}

func (s *Subscription) sendStandbyStatus(walPos uint64) error {
	withPos(s.logger, walPos).Log("event", "send_standby_status", "msg", "heartbeating standby status")
	status, err := pgx.NewStandbyStatus(walPos)
	if err != nil {
		return err
	}

	return s.conn.SendStandbyStatus(status)
}

func withPos(logger kitlog.Logger, pos uint64) kitlog.Logger {
	return kitlog.With(logger, "position", pgx.FormatLSN(pos))
}
