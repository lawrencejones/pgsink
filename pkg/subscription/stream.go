package subscription

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/alecthomas/kingpin"
	"github.com/lawrencejones/pgsink/pkg/logical"

	kitlog "github.com/go-kit/kit/log"
	level "github.com/go-kit/kit/log/level"
	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgproto3/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type StreamOptions struct {
	HeartbeatInterval time.Duration
}

func (opt *StreamOptions) Bind(cmd *kingpin.CmdClause, prefix string) *StreamOptions {
	cmd.Flag(fmt.Sprintf("%sheartbeat-interval", prefix), "Interval at which to heartbeat Postgres, must be < wal_sender_timeout").Default("30s").DurationVar(&opt.HeartbeatInterval)

	return opt
}

// Stream represents an on-going replication stream, managed by a subscription. Consumers
// of the stream can acknowledge processing messages using the Confirm() method.
type Stream struct {
	position   pglogrepl.LSN
	messages   chan interface{}
	heartbeats chan pglogrepl.LSN
	shutdown   chan struct{}
	done       chan error
}

func (s *Stream) Messages() <-chan interface{} {
	return s.messages
}

func (s *Stream) Confirm(pos pglogrepl.LSN) <-chan pglogrepl.LSN {
	if pos < s.position {
		panic("cannot confirm received position in the past")
	}

	s.position = pos

	return s.heartbeats
}

func (s *Stream) Shutdown(ctx context.Context) error {
	close(s.shutdown)

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
			Name: "pgsink_subscription_receive_message_total",
			Help: "Total number of logical messages received",
		},
		[]string{"type"},
	)
)

func stream(ctx context.Context, logger kitlog.Logger, conn *pgconn.PgConn, sysident pglogrepl.IdentifySystemResult, opts StreamOptions) *Stream {
	stream := &Stream{
		position:   sysident.XLogPos,
		messages:   make(chan interface{}),
		heartbeats: make(chan pglogrepl.LSN, 1),
		shutdown:   make(chan struct{}),
		done:       make(chan error, 1), // buffered by 1, to ensure progress when reporting an error
	}

	go func() (err error) {
		defer func() {
			logger.Log("event", "closing_stream", "msg", "closing messages channel")
			close(stream.messages)
			close(stream.heartbeats)
			stream.done <- err
			close(stream.done)
		}()

		logger := kitlog.With(logger, "stream_position", kitlog.Valuer(func() interface{} { return stream.position }))
		nextStandbyMessageDeadline := time.Now().Add(opts.HeartbeatInterval)

		for {
			select {
			case <-ctx.Done():
				logger.Log("event", "context_expired", "msg", "exiting")
				return ctx.Err()
			case <-stream.shutdown:
				logger.Log("event", "shutdown", "msg", "shutdown requested, exiting")
				return nil
			default:
				// continue
			}

			if time.Now().After(nextStandbyMessageDeadline) {
				// Reset the counter for another heartbeat interval
				nextStandbyMessageDeadline = time.Now().Add(opts.HeartbeatInterval)

				position := stream.position
				err := pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{
					WALWritePosition: position,
				})

				if err != nil {
					return errors.Wrap(err, "failed to send standby update")
				}

				logger.Log("event", "standby_status_update", "position", position)

				// If someone is listening for heartbeats, let them know. It's unlikely anyone is,
				// as we don't use this functionality in anything other than tests. I'm making an
				// assumption that non-blocking sends are fast, and we call this infrequently,
				// though this reasoning is based on a hunch.
				select {
				case stream.heartbeats <- position:
				default: // non-blocking send
				}
			}

			var msg pgproto3.BackendMessage

			{
				// Wait to receive a message, but only until we've received a shutdown request or
				// the next heartbeat is due
				ctx, cancel := context.WithDeadline(ctx, nextStandbyMessageDeadline)
				go func() {
					select {
					case <-stream.shutdown:
					case <-ctx.Done(): // no-op
					}

					cancel()
				}()

				msg, err = conn.ReceiveMessage(ctx)
				cancel()

				// If the heartbeat interval has exceeded while we're waiting for a message then
				// we're due to send a keepalive. Failure to do so will cause Postgres to
				// terminate the walsender process, resulting in a 'replication timeout' log. This
				// timeout is configured by wal_sender_timeout and defaults to 60s, so take care
				// to set the heartbeat interval to be more frequent that this.
				//
				// https://www.postgresql.org/docs/current/runtime-config-replication.html
				if ctx.Err() == context.DeadlineExceeded {
					level.Debug(logger).Log("event", "receive_message_timeout",
						"msg", "timed out receiving message, heartbeating")
					continue
				}
			}

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
