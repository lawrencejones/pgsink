package bigquery

import (
	"context"
	"fmt"
	"time"

	"github.com/lawrencejones/pg2sink/pkg/sinks/generic"

	bq "cloud.google.com/go/bigquery"
	"github.com/alecthomas/kingpin"
	kitlog "github.com/go-kit/kit/log"
	"google.golang.org/api/googleapi"
)

type Options struct {
	ProjectID     string
	Dataset       string
	Location      string
	BufferSize    int
	Instrument    bool
	FlushInterval time.Duration
}

func (opt *Options) Bind(cmd *kingpin.CmdClause, prefix string) *Options {
	cmd.Flag(fmt.Sprintf("%sproject-id", prefix), "Google Project ID").StringVar(&opt.ProjectID)
	cmd.Flag(fmt.Sprintf("%sdataset", prefix), "BigQuery dataset name").StringVar(&opt.Dataset)
	cmd.Flag(fmt.Sprintf("%slocation", prefix), "BigQuery dataset location").Default("EU").StringVar(&opt.Location)
	cmd.Flag(fmt.Sprintf("%sbuffer-size", prefix), "Number of modification to buffer before flushing").Default("250").IntVar(&opt.BufferSize)
	cmd.Flag(fmt.Sprintf("%sinstrument", prefix), "Enable instrumentation").Default("true").BoolVar(&opt.Instrument)
	cmd.Flag(fmt.Sprintf("%sflush-interval", prefix), "Time period with which we periodically flush the sink").Default("5s").DurationVar(&opt.FlushInterval)

	return opt
}

func New(ctx context.Context, logger kitlog.Logger, opts Options) (generic.Sink, error) {
	client, err := bq.NewClient(ctx, opts.ProjectID)
	if err != nil {
		return nil, err
	}

	kitlog.With(logger, "project", opts.ProjectID, "dataset", opts.Dataset, "location", opts.Location)

	dataset := client.Dataset(opts.Dataset)
	md, err := dataset.Metadata(ctx)
	if allowNotFound(err) != nil {
		return nil, err
	}

	if md == nil {
		logger.Log("event", "dataset.create", "msg", "dataset does not exist, creating")
		md = &bq.DatasetMetadata{
			Name:        opts.Dataset,
			Location:    opts.Location,
			Description: "Dataset created by pg2sink",
		}

		if err := dataset.Create(ctx, md); err != nil {
			return nil, err
		}
	}

	sink := generic.SinkBuilder(
		logger,
		generic.SinkBuilder.WithBuffer(opts.BufferSize),
		generic.SinkBuilder.WithInstrumentation(opts.Instrument),
		generic.SinkBuilder.WithFlushInterval(opts.FlushInterval),
		generic.SinkBuilder.WithSchemaHandler(
			generic.SchemaHandlerCacheOnFingerprint(
				newSchemaHandler(dataset),
			),
		),
	)

	return sink, nil
}

func allowNotFound(err error) error {
	if err, ok := err.(*googleapi.Error); ok && err.Code == 404 {
		return nil
	}

	return err
}