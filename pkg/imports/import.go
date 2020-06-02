package imports

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/lawrencejones/pgsink/pkg/dbschema/pgsink/model"
	"github.com/lawrencejones/pgsink/pkg/sinks/generic"

	"github.com/alecthomas/kingpin"
	kitlog "github.com/go-kit/kit/log"
)

type ImporterOptions struct {
	SnapshotTimeout time.Duration
	BatchLimit      int
	BufferSize      int
}

func (opt *ImporterOptions) Bind(cmd *kingpin.CmdClause, prefix string) *ImporterOptions {
	cmd.Flag(fmt.Sprintf("%ssnapshot-timeout", prefix), "Hold snapshots for no longer than this").Default("1m").DurationVar(&opt.SnapshotTimeout)
	cmd.Flag(fmt.Sprintf("%sbatch-limit", prefix), "Max rows to pull from database per import iteration").Default("5000").IntVar(&opt.BatchLimit)
	cmd.Flag(fmt.Sprintf("%sbuffer-size", prefix), "Channel buffer between Postgres and the sink").Default("5000").IntVar(&opt.BufferSize)

	return opt
}

type Importer interface {
	Do(ctx context.Context, logger kitlog.Logger, tx pgx.Tx, job model.ImportJobs) error
}

func NewImporter(sink generic.Sink, opts ImporterOptions) Importer {
	return &importer{
		sink: sink,
		opts: opts,
	}
}

type importer struct {
	sink generic.Sink
	opts ImporterOptions
}

// Import works an import job.
func (i importer) Do(ctx context.Context, logger kitlog.Logger, tx pgx.Tx, job model.ImportJobs) error {
	return nil // TODO
}

// Import is built for each job in the database, having resolved contextual information
// that can help run the job from the database whenever the job was enqueued.
type Import struct {
	TableName  string
	PrimaryKey string
	// PrimaryKeyScanner logical.ValueScanner
	// Relation          *logical.Relation
	Scanners []interface{}
	Cursor   interface{}
}
