package file

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/alecthomas/kingpin"
	"github.com/lawrencejones/pg2sink/pkg/changelog"
	"github.com/lawrencejones/pg2sink/pkg/changelog/serialize"
	"github.com/lawrencejones/pg2sink/pkg/sinks/generic"

	kitlog "github.com/go-kit/kit/log"
	"github.com/pkg/errors"
)

type Options struct {
	SchemasPath       string
	ModificationsPath string
	BufferSize        int
	FlushInterval     time.Duration
}

func (opt *Options) Bind(cmd *kingpin.CmdClause, prefix string) *Options {
	cmd.Flag(fmt.Sprintf("%sschemas-path", prefix), "File path for schemas").Default("/dev/stdout").StringVar(&opt.SchemasPath)
	cmd.Flag(fmt.Sprintf("%smodifications-path", prefix), "File path for modifications").Default("/dev/stdout").StringVar(&opt.ModificationsPath)
	cmd.Flag(fmt.Sprintf("%sbuffer-size", prefix), "Number of modification to buffer before flushing").Default("5").IntVar(&opt.BufferSize)
	cmd.Flag(fmt.Sprintf("%sflush-interval", prefix), "Time period with which we periodically flush the sink").Default("5s").DurationVar(&opt.FlushInterval)

	return opt
}

func New(logger kitlog.Logger, opts Options) (generic.Sink, error) {
	schemas, err := openFile(opts.SchemasPath)
	if err != nil {
		return nil, err
	}

	modifications, err := openFile(opts.ModificationsPath)
	if err != nil {
		return nil, err
	}

	// TODO: We don't use the serialize package properly yet. Until we do, there's no point
	// paramterising it.
	serializer := serialize.DefaultSerializer

	sink := generic.SinkBuilder(
		logger,
		generic.SinkBuilder.WithBuffer(opts.BufferSize),
		generic.SinkBuilder.WithFlushInterval(opts.FlushInterval),
		generic.SinkBuilder.WithSyncer(
			generic.SyncAlwaysUpdateFunc(
				func(ctx context.Context, logger kitlog.Logger, schema *changelog.Schema) (changelog.Namespace, generic.Inserter, error) {
					if _, err := fmt.Fprintln(schemas, string(serializer.Register(schema))); err != nil {
						return "", nil, errors.Wrap(err, "failed to write schema")
					}

					return generic.RouterMatchAll, &inserter{file: modifications, serializer: serializer}, nil
				},
			),
		),
	)

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
