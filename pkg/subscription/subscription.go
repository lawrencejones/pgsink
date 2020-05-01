package subscription

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/lawrencejones/pg2sink/pkg/logical"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	kitlog "github.com/go-kit/kit/log"
	level "github.com/go-kit/kit/log/level"
	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgproto3/v2"
	"github.com/pkg/errors"
)

type SubscriptionOptions struct {
	Name            string        // name of the subscription
	Publication     string        // name of the publication
	StatusHeartbeat time.Duration // heartbeat interval
}

// Create initialises a subscription once the replication slot has been created. This is
// the only way to create a subscription, to ensure a replication slot exists before
// anyone can call Start().
func Create(ctx context.Context, logger kitlog.Logger, conn *pgconn.PgConn, opts SubscriptionOptions) (*Subscription, error) {
	if err := createReplicationSlot(ctx, logger, conn, opts); err != nil {
		return nil, err
	}

	return &Subscription{conn: conn, opts: opts}, nil
}

func createReplicationSlot(ctx context.Context, logger kitlog.Logger, conn *pgconn.PgConn, opts SubscriptionOptions) (err error) {
	logger = kitlog.With(logger, "event", "create_replication_slot", "slot", opts.Name)
	defer func() {
		logger.Log("event", "create_replication_slot", "error", err)
	}()

	_, err = pglogrepl.CreateReplicationSlot(ctx, conn, opts.Name, logical.PGOutput, pglogrepl.CreateReplicationSlotOptions{})
	if err != nil {
		if err, ok := err.(*pgconn.PgError); ok {
			if err.Code == "42710" {
				logger = kitlog.With(logger, "detail", "slot_already_exists")
				return nil
			}
		}
	}

	return err
}

// Subscription provides the implementation of logical replication from a Postgres
// primary. It implements the functionality of CREATE SUBSCRIPTION in terms of managing
// the replication slot and provides a Start function that will subscribe to changes on
// the target publication.
//
// https://www.postgresql.org/docs/11/sql-createsubscription.html
type Subscription struct {
	conn *pgconn.PgConn
	opts SubscriptionOptions
}

// Start begins replicating from our remote. We set our WAL position to whatever the
// server tells us our replication slot was last recorded at, then proceed to heartbeat
// and replicate our remote.
func (s *Subscription) Start(ctx context.Context, logger kitlog.Logger) (*Stream, error) {
	sysident, err := pglogrepl.IdentifySystem(ctx, s.conn)
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
			`"proto_version" '1'`, fmt.Sprintf(`"publication_names" '%s'`, s.opts.Publication),
		},
	}

	logger.Log("event", "start_replication", "publication", s.opts.Publication, "slot", s.opts.Name)
	if err := pglogrepl.StartReplication(ctx, s.conn, s.opts.Name, sysident.XLogPos, options); err != nil {
		return nil, err
	}

	return s.stream(ctx, logger, sysident), nil
}

var (
	receiveMessageTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pg2sink_subscription_receive_message_total",
			Help: "Total number of logical messages received",
		},
		[]string{"type"},
	)
)

func (s *Subscription) stream(ctx context.Context, logger kitlog.Logger, sysident pglogrepl.IdentifySystemResult) *Stream {
	stream := &Stream{
		position: sysident.XLogPos,
		messages: make(chan interface{}),
		done:     make(chan error, 1), // buffered by 1, to ensure progress when reporting an error
	}

	go func() (err error) {
		defer func() {
			logger.Log("event", "closing_stream", "msg", "closing messages channel")
			close(stream.messages)
			stream.done <- err
			close(stream.done)
		}()

		logger := kitlog.With(logger, "stream_position", kitlog.Valuer(func() interface{} { return stream.position }))
		nextStandbyMessageDeadline := time.Now().Add(s.opts.StatusHeartbeat)

		for {
			select {
			case <-ctx.Done():
				logger.Log("event", "context_expired", "msg", "exiting")
				return
			default:
				// continue
			}

			if time.Now().After(nextStandbyMessageDeadline) {
				// Reset the counter for another heartbeat interval
				nextStandbyMessageDeadline = time.Now().Add(s.opts.StatusHeartbeat)

				position := stream.position
				err := pglogrepl.SendStandbyStatusUpdate(ctx, s.conn, pglogrepl.StandbyStatusUpdate{
					WALWritePosition: position,
				})

				if err != nil {
					return errors.Wrap(err, "failed to send standby update")
				}

				logger.Log("event", "standby_status_update", "position", position)
			}

			ctx, cancel := context.WithCancel(ctx)
			msg, err := s.conn.ReceiveMessage(ctx)
			cancel()

			if err != nil {
				if pgconn.Timeout(err) {
					logger.Log("event", "receive_timeout", "msg", "retrying")
					continue
				}

				return errors.Wrap(err, "failed to receive message")
			}

			switch msg := msg.(type) {
			case *pgproto3.CopyData:
				switch msg.Data[0] {
				case pglogrepl.PrimaryKeepaliveMessageByteID:
					pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
					if err != nil {
						return errors.Wrap(err, "failed to parse primary keepalive message")
					}

					if pkm.ReplyRequested {
						nextStandbyMessageDeadline = time.Now()
					}

					event := kitlog.With(logger,
						"event", "primary_keepalive_message",
						"position", pkm.ServerWALEnd,
						"server_time", pkm.ServerTime.Format(time.RFC3339),
						"reply_requested", pkm.ReplyRequested,
					)

					// If the server hasn't explicitly requested a reply, then this log should be
					// debug level only
					if !pkm.ReplyRequested {
						event = level.Debug(event)
					}

					event.Log()

				case pglogrepl.XLogDataByteID:
					xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
					if err != nil {
						return errors.Wrap(err, "failed to parse xlog data")
					}

					decoded, msgType, err := logical.DecodePGOutput(xld.WALData)
					if err != nil {
						return errors.Wrap(err, "failed to deocde wal")
					}

					receiveMessageTotal.WithLabelValues(msgType).Inc()
					stream.messages <- decoded // send message down our received channel
				}
			default:
				logger.Log("event", "unrecognised_message", "message", reflect.TypeOf(msg))
			}
		}
	}()

	return stream
}
