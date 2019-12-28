package pg2pubsub

import (
	"time"

	kitlog "github.com/go-kit/kit/log"
	"github.com/jackc/pgx"
)

type ImporterOptions struct {
	WorkerCount   int    // maximum parallel import workers
	TableName     string // name of table containing in-progress imports
	PublicationID string // identifier of the current publication
}

func NewImporter(logger kitlog.Logger, pool *pgx.ConnPool, opts ImporterOptions) *Importer {
	if pool.Stat().MaxConnections < opts.WorkerCount {
		panic("connection pool is too small to support importer workers")
	}

	return &Importer{
		logger: logger,
		pool:   pool,
		opts:   opts,
	}
}

type Importer struct {
	logger kitlog.Logger
	pool   *pgx.ConnPool
	opts   ImporterOptions
}

type ImportJob struct {
	ID            int64
	PublicationID string
	Table         string
	Cursor        string
	CompletedAt   time.Time
}
