package subscription

import (
	"context"

	"github.com/jackc/pglogrepl"
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
