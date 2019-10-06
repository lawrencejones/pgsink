package pg2pubsub

import (
	"context"
	"fmt"
	"time"

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
	logger   kitlog.Logger
	conn     *pgx.ReplicationConn
	walPos   uint64
	received chan interface{}
	opts     SubscriptionOptions
}

func NewSubscription(logger kitlog.Logger, conn *pgx.ReplicationConn, opts SubscriptionOptions) *Subscription {
	return &Subscription{
		logger:   logger,
		conn:     conn,
		walPos:   0,
		received: make(chan interface{}),
		opts:     opts,
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

// StartReplication begins replicating from our remote. We set our WAL position to
// whatever the server tells us our replication slot was last recorded at, then proceed to
// heartbeat and replicate our remote.
func (s *Subscription) StartReplication(ctx context.Context) error {
	s.logger.Log("event", "start_replication", "publication", s.opts.Publication, "slot", s.opts.Name)
	pluginArguments := []string{
		`"proto_version" '1'`, fmt.Sprintf(`"publication_names" '%s'`, s.opts.Publication),
	}
	if err := s.conn.StartReplication(s.opts.Name, 0, -1, pluginArguments...); err != nil {
		return err
	}

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
				s.walPos = msg.ServerHeartbeat.ServerWalEnd
				withPos(s.logger, s.walPos).Log("event", "initial_heartbeat_received")

				break
			}
		}
	}

	var g run.Group

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	g.Add(
		func() error { return s.startHeartbeats(ctx) },
		func(error) { cancel() },
	)

	g.Add(
		func() error { return s.startReceiving(ctx) },
		func(error) { cancel() },
	)

	return g.Run()
}

// Received provides the channel that our replication worker will send messages down. We
// set the channel to be receive only as the subscription should be the only entity
// capable of closing the channel, to avoid races.
func (s *Subscription) Received() <-chan interface{} {
	return s.received
}

// ConfirmReceived updates our WAL position, causing the next server heartbeat to update
// the replication slot position.
func (s *Subscription) ConfirmReceived(pos uint64) {
	if pos < s.walPos {
		panic("cannot confirm received position in the past")
	}

	s.walPos = pos
}

// startReceiving will receive messages until the context expires, or we receive an error
// from our connection. Received messages are sent down our received channel, which we
// assume someone is consuming from.
//
// Once we're done, we close the received channel so our consumers know we're done.
func (s *Subscription) startReceiving(ctx context.Context) error {
	defer close(s.received)

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
				s.received <- msg // send message down our received channel
			}
		}
	}
}

// startHeartbeats sends standby statuses to our replication primary to confirm we've
// successfully applied wal and to keepalive our connection.
func (s *Subscription) startHeartbeats(ctx context.Context) error {
	ticker := time.NewTicker(s.opts.StatusHeartbeat)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.logger.Log("event", "status_heartbeat_stop", "msg", "context expired, finish heartbeating")
			err := s.sendStandbyStatus()
			if err != nil {
				s.logger.Log("error", err, "msg", "final heartbeat failed, expect to replay changes on next boot")
			}

			return err

		case <-ticker.C:
			if err := s.sendStandbyStatus(); err != nil {
				s.logger.Log("error", err, "msg", "failed to send status heartbeat")
			}
		}
	}
}

func (s *Subscription) sendStandbyStatus() error {
	withPos(s.logger, s.walPos).Log("event", "send_standby_status", "msg", "heartbeating standby status")
	status, err := pgx.NewStandbyStatus(s.walPos)
	if err != nil {
		return err
	}

	return s.conn.SendStandbyStatus(status)
}

func withPos(logger kitlog.Logger, pos uint64) kitlog.Logger {
	return kitlog.With(logger, "position", pgx.FormatLSN(pos))
}
