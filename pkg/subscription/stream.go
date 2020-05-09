package subscription

import (
	"context"
	"reflect"
	"time"

	"github.com/lawrencejones/pg2sink/pkg/logical"

	kitlog "github.com/go-kit/kit/log"
	level "github.com/go-kit/kit/log/level"
	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgproto3/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Stream represents an on-going replication stream, managed by a subscription. Consumers
// of the stream can acknowledge processing messages using the Confirm() method.
type Stream struct {
	position pglogrepl.LSN
	messages chan interface{}
	done     chan error
}

func (s *Stream) Messages() <-chan interface{} {
	return s.messages
}

func (s *Stream) Confirm(pos pglogrepl.LSN) {
	if pos < s.position {
		panic("cannot confirm received position in the past")
	}

	s.position = pos
}

func (s *Stream) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-s.done:
		return err
	}
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

func stream(ctx context.Context, logger kitlog.Logger, conn *pgconn.PgConn, sysident pglogrepl.IdentifySystemResult, heartbeatInterval time.Duration) *Stream {
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
		nextStandbyMessageDeadline := time.Now().Add(heartbeatInterval)

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
				nextStandbyMessageDeadline = time.Now().Add(heartbeatInterval)

				position := stream.position
				err := pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{
					WALWritePosition: position,
				})

				if err != nil {
					return errors.Wrap(err, "failed to send standby update")
				}

				logger.Log("event", "standby_status_update", "position", position)
			}

			ctx, cancel := context.WithCancel(ctx)
			msg, err := conn.ReceiveMessage(ctx)
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
