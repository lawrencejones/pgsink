package bigquery

import (
	"context"
	"fmt"

	"go.opencensus.io/trace"

	"google.golang.org/api/googleapi"

	kitlog "github.com/go-kit/kit/log"
	"github.com/lawrencejones/pg2sink/pkg/changelog"
	"github.com/lawrencejones/pg2sink/pkg/sinks"
	"github.com/pkg/errors"

	bq "cloud.google.com/go/bigquery"
)

func New(ctx context.Context, logger kitlog.Logger, opts Options) (sinks.Sink, error) {
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

	return &Sink{logger: logger, dataset: dataset, opts: opts}, nil
}

type Sink struct {
	logger  kitlog.Logger
	dataset *bq.Dataset
	opts    Options
}

type Options struct {
	ProjectID string
	Dataset   string
	Location  string
}

func (s *Sink) Consume(ctx context.Context, entries changelog.Changelog, ack sinks.AckCallback) error {
	ctx, span := trace.StartSpan(ctx, "pkg/sinks.BigQuery.Consume")
	defer span.End()

	dataset := newDataset(s.dataset)
	for envelope := range entries {
		switch entry := envelope.Unwrap().(type) {
		case *changelog.Schema:
			if err := dataset.Sync(ctx, s.logger, entry); err != nil {
				return err
			}
		case *changelog.Modification:
			table := dataset.TableFor(entry)
			if table == nil {
				panic(fmt.Sprintf("no schema for %s, cannot proceed", entry.Namespace))
			}

			var payloadSchema bq.Schema
			for _, field := range table.rawMetadata.Schema {
				if field.Name == "payload" {
					payloadSchema = field.Schema
					break
				}
			}

			// When deletion, we'll update this row with the contents at delete
			row := entry.AfterOrBefore()
			values := []bq.Value{}
			for _, field := range payloadSchema {
				values = append(values, row[field.Name])
			}

			err := table.raw.Inserter().Put(ctx, &bq.ValuesSaver{
				Schema: table.rawMetadata.Schema,
				Row: []bq.Value{
					entry.Timestamp,
					entry.LSN,
					entry.Operation(),
					bq.Value(values),
				},
			})

			if err != nil {
				return errors.Wrapf(err, "failed to insert into table %s", table.raw.FullyQualifiedName())
			}
		}
	}

	return nil
}

func allowNotFound(err error) error {
	if err, ok := err.(*googleapi.Error); ok && err.Code == 404 {
		return nil
	}

	return err
}
