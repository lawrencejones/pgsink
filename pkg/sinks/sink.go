package sinks

import (
	"context"
	"os"

	"github.com/lawrencejones/pg2sink/pkg/changelog"
	"github.com/pkg/errors"
)

// AckCallback will acknowledge successful publication of up-to this WAL position
type AckCallback func(pos uint64)

// Sink is a generic sink destination for a changelog. It will consume entries until
// either an error, or the entries run out.
type Sink interface {
	Consume(context.Context, changelog.Changelog, AckCallback) error
}

var _ Sink = &File{}

func NewFile(opts FileOptions) (*File, error) {
	sink := &File{serializer: changelog.DefaultSerializer}

	var err error
	sink.schemas, err = os.OpenFile(opts.SchemasPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	sink.modifications, err = os.OpenFile(opts.ModificationsPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	return sink, nil
}

type FileOptions struct {
	SchemasPath       string
	ModificationsPath string
}

type File struct {
	schemas       *os.File
	modifications *os.File
	serializer    changelog.Serializer
}

func (s *File) Consume(_ context.Context, entries changelog.Changelog, ack AckCallback) error {
	for envelope := range entries {
		switch entry := envelope.Unwrap().(type) {
		case *changelog.Schema:
			if _, err := s.schemas.Write(append(s.serializer.Register(entry), '\n')); err != nil {
				return errors.Wrap(err, "failed to write schema")
			}
		case *changelog.Modification:
			bytes, err := s.serializer.Marshal(entry)
			if err != nil {
				return errors.Wrap(err, "failed to marshal modification")
			}

			if _, err := s.modifications.Write(append(bytes, '\n')); err != nil {
				return errors.Wrap(err, "failed to write modification")
			}

			if entry.LSN != nil && ack != nil {
				ack(*entry.LSN)
			}
		}
	}

	return nil
}
