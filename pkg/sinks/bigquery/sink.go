package bigquery

import (
	"context"
	"fmt"
	"time"

	"github.com/lawrencejones/pgsink/internal/telem"
	"github.com/lawrencejones/pgsink/pkg/decode"
	"github.com/lawrencejones/pgsink/pkg/sinks/generic"

	bq "cloud.google.com/go/bigquery"
	"github.com/alecthomas/kingpin"
	"google.golang.org/api/googleapi"
)

type Options struct {
	ProjectID                     string
	Dataset                       string
	DatasetDefaultTableExpiration time.Duration
	Location                      string
	BufferSize                    int
	Instrument                    bool
	FlushInterval                 time.Duration
}

func (opt *Options) Bind(cmd *kingpin.CmdClause, prefix string) *Options {
	cmd.Flag(fmt.Sprintf("%sproject-id", prefix), "Google Project ID").StringVar(&opt.ProjectID)
	cmd.Flag(fmt.Sprintf("%sdataset", prefix), "BigQuery dataset name").StringVar(&opt.Dataset)
	cmd.Flag(fmt.Sprintf("%sdataset-default-table-expiration", prefix), "BigQuery dataset default table expiration, applied only if creating the dataset").
		DurationVar(&opt.DatasetDefaultTableExpiration)
	cmd.Flag(fmt.Sprintf("%slocation", prefix), "BigQuery dataset location").Default("EU").StringVar(&opt.Location)
	cmd.Flag(fmt.Sprintf("%sbuffer-size", prefix), "Number of modification to buffer before flushing").Default("250").IntVar(&opt.BufferSize)
	cmd.Flag(fmt.Sprintf("%sinstrument", prefix), "Enable instrumentation").Default("true").BoolVar(&opt.Instrument)
	cmd.Flag(fmt.Sprintf("%sflush-interval", prefix), "Time period with which we periodically flush the sink").Default("5s").DurationVar(&opt.FlushInterval)

	return opt
}

func New(ctx context.Context, decoder decode.Decoder, opts Options) (generic.Sink, error) {
	client, err := bq.NewClient(ctx, opts.ProjectID)
	if err != nil {
		return nil, err
	}

	dataset := client.Dataset(opts.Dataset)
	md, err := dataset.Metadata(ctx)
	if allowNotFound(err) != nil {
		return nil, err
	}

	logger := telem.LoggerFrom(ctx, "project", opts.ProjectID, "dataset", opts.Dataset, "location", opts.Location)
	if md == nil {
		logger.Log("event", "dataset.create", "msg", "dataset does not exist, creating")
		md = &bq.DatasetMetadata{
			Name:                   opts.Dataset,
			Location:               opts.Location,
			Description:            "Dataset created by pgsink",
			DefaultTableExpiration: opts.DatasetDefaultTableExpiration,
		}

		if err := dataset.Create(ctx, md); err != nil {
			return nil, err
		}
	}

	sink := generic.SinkBuilder(
		generic.SinkBuilder.WithBuffer(opts.BufferSize),
		generic.SinkBuilder.WithInstrumentation(opts.Instrument),
		generic.SinkBuilder.WithFlushInterval(opts.FlushInterval),
		generic.SinkBuilder.WithSchemaHandler(
			generic.SchemaHandlerCacheOnFingerprint(
				newSchemaHandler(dataset, decoder),
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
