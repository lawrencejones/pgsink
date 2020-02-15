package file

import (
	"context"
	"os"
	"sync"

	"github.com/lawrencejones/pg2sink/pkg/changelog"
	"github.com/lawrencejones/pg2sink/pkg/changelog/serialize"
	"github.com/lawrencejones/pg2sink/pkg/sinks"
	"github.com/pkg/errors"
	"go.opencensus.io/trace"
)

func New(opts Options) (sinks.Sink, error) {
	sink := &Sink{serializer: serialize.DefaultSerializer}

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

type Sink struct {
	schemas       *os.File
	modifications *os.File
	serializer    serialize.Serializer
	sync.Mutex
}

type Options struct {
	SchemasPath       string
	ModificationsPath string
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

func (s *Sink) Consume(ctx context.Context, entries changelog.Changelog, ack sinks.AckCallback) error {
	ctx, span := trace.StartSpan(ctx, "pkg/sinks/file/Sink.Consume")
	defer span.End()

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
func (s *Sink) write(file *os.File, content []byte) (int, error) {
	s.Lock()
	defer s.Unlock()

	return file.Write(content)
}
