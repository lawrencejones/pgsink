package sinks

import (
	"context"
	"os"
	"sync"

	"github.com/lawrencejones/pg2sink/pkg/changelog"
	"github.com/pkg/errors"
)

// AckCallback will acknowledge successful publication of up-to this message. It is not
// guaranteed to be called for any intermediate messages.
type AckCallback func(changelog.Entry)

// Sink is a generic sink destination for a changelog. It will consume entries until
// either an error, or the entries run out.
type Sink interface {
	Consume(context.Context, changelog.Changelog, AckCallback) error
}

var _ Sink = &File{}

func NewFile(opts FileOptions) (*File, error) {
	sink := &File{serializer: changelog.DefaultSerializer}

	var err error
	sink.schemas, err = openFile(opts.SchemasPath)
	if err != nil {
		return nil, err
	}

	sink.modifications, err = openFile(opts.ModificationsPath)
	if err != nil {
		return nil, err
	}

	return sink, nil
}

func openFile(path string) (*os.File, error) {
	switch path {
	case "/dev/stdout":
		return os.Stdout, nil
	case "/dev/stderr":
		return os.Stderr, nil
	}

	return os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
}

type FileOptions struct {
	SchemasPath       string
	ModificationsPath string
}

type File struct {
	schemas       *os.File
	modifications *os.File
	serializer    changelog.Serializer
	sync.Mutex
}

func (s *File) Consume(_ context.Context, entries changelog.Changelog, ack AckCallback) error {
	for envelope := range entries {
		switch entry := envelope.Unwrap().(type) {
		case *changelog.Schema:
			if _, err := s.write(s.schemas, append(s.serializer.Register(entry), '\n')); err != nil {
				return errors.Wrap(err, "failed to write schema")
			}
		case *changelog.Modification:
			bytes, err := s.serializer.Marshal(entry)
			if err != nil {
				return errors.Wrap(err, "failed to marshal modification")
			}

			if _, err := s.write(s.modifications, append(bytes, '\n')); err != nil {
				return errors.Wrap(err, "failed to write modification")
			}
		}

		if ack != nil {
			ack(envelope)
		}
	}

	return nil
}

// write wraps file modification in a lock, allowing this sink to be safe for concurrent
// use.
func (s *File) write(file *os.File, content []byte) (int, error) {
	s.Lock()
	defer s.Unlock()

	return file.Write(content)
}
