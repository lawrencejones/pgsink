package main

import (
	"context"
	"fmt"
	stdlog "log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/alecthomas/kingpin"
	"github.com/davecgh/go-spew/spew"
	kitlog "github.com/go-kit/kit/log"
	level "github.com/go-kit/kit/log/level"
	"github.com/jackc/pgx"
	"github.com/lawrencejones/pg2sink/pkg/changelog"
	"github.com/lawrencejones/pg2sink/pkg/imports"
	"github.com/lawrencejones/pg2sink/pkg/migration"
	"github.com/lawrencejones/pg2sink/pkg/models"
	"github.com/lawrencejones/pg2sink/pkg/publication"
	"github.com/lawrencejones/pg2sink/pkg/sinks"
	"github.com/lawrencejones/pg2sink/pkg/subscription"
	"github.com/lawrencejones/pg2sink/pkg/util"
	"github.com/oklog/run"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var logger kitlog.Logger

var (
	app = kingpin.New("pg2sink", "Publish Postgres changes to pubsub").Version(versionStanza())

	// Global flags
	debug = app.Flag("debug", "Enable debug logging").Default("false").Bool()

	// Database connection paramters
	host     = app.Flag("host", "Postgres host").Envar("PGHOST").Default("127.0.0.1").String()
	port     = app.Flag("port", "Postgres port").Envar("PGPORT").Default("5432").Uint16()
	database = app.Flag("database", "Postgres database name").Envar("PGDATABASE").Default("postgres").String()
	user     = app.Flag("user", "Postgres user").Envar("PGUSER").Default("postgres").String()

	// Each stream is uniquely identified by a (publication_name, subscription_name)
	publicationName = app.Flag("publication-name", "Publication name").Default("pg2sink").String()
	slotName        = app.Flag("slot-name", "Replication slot name").Default("pg2sink").String()

	add      = app.Command("add", "Add table to existing publication")
	addTable = add.Flag("table", "Table to add to publication, e.g. public.example").Required().String()

	resync      = app.Command("resync", "Resync table for a given subscription")
	resyncTable = resync.Flag("table", "Table to mark for resync").Required().String()

	remove      = app.Command("remove", "Remove table from existing publication")
	removeTable = remove.Flag("table", "Table to remove from publication, e.g. public.example").Required().String()

	stream                          = app.Command("stream", "Stream changes into sink")
	streamMetricsAddress            = stream.Flag("metrics-address", "Address to bind HTTP metrics listener").Default("127.0.0.1").String()
	streamMetricsPort               = stream.Flag("metrics-port", "Port to bind HTTP metrics listener").Default("9525").Uint16()
	streamManagePublication         = stream.Flag("manage-publication", "Auto-manage tables in publication").Default("false").Bool()
	streamSchemas                   = stream.Flag("schema", "Postgres schema to watch for changes").Default("public").Strings()
	streamExcludes                  = stream.Flag("exclude", "Table name to exclude from changes").Strings()
	streamIncludes                  = stream.Flag("include", "Table name to include from changes (activates whitelist)").Strings()
	streamPollInterval              = stream.Flag("poll-interval", "Interval to poll for new tables").Default("10s").Duration()
	streamStatusHeartbeat           = stream.Flag("status-heartbeat", "Interval to heartbeat replication primary").Default("10s").Duration()
	streamDecodeOnly                = stream.Flag("decode-only", "Interval to heartbeat replication primary").Default("false").Bool()
	streamModificationWorkerCount   = stream.Flag("modification-worker-count", "Workers for building modifications").Default("1").Int()
	streamImportManagerPollInterval = stream.Flag("import-manager-poll-interval", "Interval to poll for newly published tables").Default("10s").Duration()
	streamImporterPollInterval      = stream.Flag("importer-poll-interval", "Interval to poll for new import jobs").Default("10s").Duration()
	streamImporterWorkerCount       = stream.Flag("importer-worker-count", "Workers for processing imports").Default("1").Int()
	streamImporterSnapshotTimeout   = stream.Flag("importer-snapshot-timeout", "Maximum time to hold Postgres snapshots").Default("1m").Duration()
	streamImporterBatchLimit        = stream.Flag("importer-batch-limit", "Maximum rows to pull per import job pagination").Default("100000").Int()
	streamImporterBufferSize        = stream.Flag("importer-buffer-size", "Buffer between pulling data from Postgres and sink").Default("100000").Int()

	streamSinkType        = stream.Flag("sink", "Type of sink target").Default("file").String()
	streamSinkFileOptions = sinks.FileOptions{}
	_                     = func() (err error) {
		stream.Flag("sink.file.schemas-path", "File path for schemas").Default("/dev/stdout").StringVar(&streamSinkFileOptions.SchemasPath)
		stream.Flag("sink.file.modifications-path", "File path for modifications").Default("/dev/stdout").StringVar(&streamSinkFileOptions.ModificationsPath)
		return
	}()
)

func main() {
	command := kingpin.MustParse(app.Parse(os.Args[1:]))
	logger = kitlog.NewLogfmtLogger(kitlog.NewSyncWriter(os.Stderr))

	if *debug {
		logger = level.NewFilter(logger, level.AllowDebug())
	} else {
		logger = level.NewFilter(logger, level.AllowInfo())
	}

	logger = kitlog.With(logger, "ts", kitlog.DefaultTimestampUTC, "caller", kitlog.DefaultCaller)
	stdlog.SetOutput(kitlog.NewStdlibAdapter(logger))

	ctx, cancel := setupSignalHandler()
	defer cancel()

	logger.Log("event", "database_config", "host", *host, "port", *port, "database", *database, "user", user)
	cfg := pgx.ConnConfig{Host: *host, Port: *port, Database: *database, User: *user}

	mustConnectionPool := func(size int) *pgx.ConnPool {
		pool, err := pgx.NewConnPool(
			pgx.ConnPoolConfig{
				ConnConfig:     cfg,
				MaxConnections: size,
			},
		)

		if err != nil {
			kingpin.Fatalf("failed to create connection pool: %v", err)
		}

		return pool
	}

	handleError := func(logger kitlog.Logger) func(error) {
		return func(err error) {
			if err != nil {
				logger.Log("error", err.Error(), "msg", "received error, cancelling context")
			}
			cancel()
		}
	}

	if err := migration.Migrate(ctx, logger, cfg); err != nil {
		kingpin.Fatalf("failed to migrate database: %v", err)
	}

	switch command {
	case add.FullCommand():
		logger := kitlog.With(logger, "table", *addTable, "publication", *publicationName)
		if err := publication.Publication(*publicationName).AddTable(ctx, mustConnectionPool(1), *addTable); err != nil {
			kingpin.Fatalf("failed to add table to publication: %v", err)
		}

		logger.Log("event", "added_table")

	case resync.FullCommand():
		pool := mustConnectionPool(1)
		pub := publication.Publication(*publicationName)
		publicationID, err := pub.GetIdentifier(ctx, pool)
		if err != nil {
			kingpin.Fatalf("failed to find publication: %v", err)
		}

		published, err := pub.GetPublishedTables(ctx, pool, publicationID)
		if err != nil {
			kingpin.Fatalf("failed to list published tables: %v", err)
		}

		if !util.Includes(published, *resyncTable) {
			kingpin.Fatalf("table %s is not in publication %s", *resyncTable, publicationName)
		}

		tx, err := pool.Begin()
		if err != nil {
			kingpin.Fatalf("failed to open transaction: %v", err)
		}

		jobStore := models.ImportJobStore{tx}
		jobs, err := jobStore.Where(ctx,
			`publication_id = $1 and subscription_name = $2 and table_name = $3`,
			publicationID, *slotName, *resyncTable)
		if err != nil {
			kingpin.Fatalf("failed to find import jobs for table: %v", err)
		}

		for _, job := range jobs {
			logger.Log("event", "expiring_import_job", "job_id", job.ID, "subscription_name",
				job.SubscriptionName, "completed_at", job.CompletedAt)
			if _, err := jobStore.Expire(ctx, job.ID); err != nil {
				kingpin.Fatalf("failed to mark job as expired: %v", err)
			}
		}

		if err := tx.Commit(); err != nil {
			kingpin.Fatalf("failed to commit transaction: %v", err)
		}

		logger.Log("event", "resync_triggered")

	case remove.FullCommand():
		pool := mustConnectionPool(1)
		tx, err := pool.Begin()
		if err != nil {
			kingpin.Fatalf("failed to open transaction: %v", err)
		}

		publicationID, err := publication.Publication(*publicationName).GetIdentifier(ctx, tx)
		if err != nil {
			kingpin.Fatalf("failed to find matching publication: %v", err)
		}

		logger := kitlog.With(logger, "table", *removeTable, "publication", *publicationName)
		if err := publication.Publication(*publicationName).DropTable(ctx, tx, *removeTable); err != nil {
			kingpin.Fatalf("failed to remove table from publication: %v", err)
		}

		jobStore := models.ImportJobStore{tx}
		jobs, err := jobStore.Where(ctx, `publication_id = $1 and table_name = $2`, publicationID, *removeTable)
		if err != nil {
			kingpin.Fatalf("failed to find import jobs for table: %v", err)
		}

		for _, job := range jobs {
			logger.Log("event", "expiring_import_job", "job_id", job.ID, "subscription_name",
				job.SubscriptionName, "completed_at", job.CompletedAt)
			if _, err := jobStore.Expire(ctx, job.ID); err != nil {
				kingpin.Fatalf("failed to mark job as expired: %v", err)
			}
		}

		if err := tx.Commit(); err != nil {
			kingpin.Fatalf("failed to commit transaction: %v", err)
		}

		logger.Log("event", "removed_table")

	case stream.FullCommand():
		var (
			sink   sinks.Sink
			sub    *subscription.Subscription
			pubmgr *publication.Manager
		)

		mustSink := func(sink sinks.Sink, err error) sinks.Sink {
			if err != nil {
				kingpin.Fatalf("failed to create file sink: %v", err)
			}

			return sink
		}

		switch *streamSinkType {
		case "file":
			sink = mustSink(sinks.NewFile(streamSinkFileOptions))
		default:
			kingpin.Fatalf("unsupported sink type: %s", *streamSinkType)
		}

		var g run.Group

		logger.Log("event", "metrics.listen", "address", *streamMetricsAddress, "port", *streamMetricsPort)
		http.Handle("/metrics", promhttp.Handler())
		go http.ListenAndServe(fmt.Sprintf("%s:%v", *streamMetricsAddress, *streamMetricsPort), nil)

		{
			logger := kitlog.With(logger, "component", "publication")

			var err error
			pubmgr, err = publication.CreateManager(
				ctx,
				logger,
				mustConnectionPool(2),
				publication.ManagerOptions{
					Name:         *publicationName,
					Schemas:      *streamSchemas,
					Excludes:     *streamExcludes,
					Includes:     *streamIncludes,
					PollInterval: *streamPollInterval,
				},
			)

			if err != nil {
				kingpin.Fatalf("failed to create publication: %v", err)
			}

			if *streamManagePublication {
				g.Add(
					func() error {
						return pubmgr.Sync(ctx)
					},
					handleError(logger),
				)
			}
		}

		{
			logger := kitlog.With(logger, "component", "import_manager")

			manager := imports.NewManager(
				logger,
				mustConnectionPool(1),
				imports.ManagerOptions{
					PublicationName:  *publicationName,
					PublicationID:    pubmgr.GetPublicationID(),
					SubscriptionName: *slotName,
					PollInterval:     *streamImportManagerPollInterval,
				},
			)

			g.Add(
				func() error {
					return manager.Sync(ctx)
				},
				handleError(logger),
			)
		}

		{
			logger := kitlog.With(logger, "component", "importer")

			importer := imports.NewImporter(
				logger,
				mustConnectionPool(*streamImporterWorkerCount),
				sink,
				imports.ImporterOptions{
					WorkerCount:      *streamImporterWorkerCount,
					PublicationID:    pubmgr.GetPublicationID(),
					SubscriptionName: *slotName,
					PollInterval:     *streamImporterPollInterval,
					SnapshotTimeout:  *streamImporterSnapshotTimeout,
					BatchLimit:       *streamImporterBatchLimit,
					BufferSize:       *streamImporterBufferSize,
				},
			)

			if *streamImporterWorkerCount > 0 {
				g.Add(
					func() error {
						importer.Run(ctx)
						return nil
					},
					handleError(logger),
				)
			}
		}

		{
			logger := kitlog.With(logger, "component", "subscription")

			conn, err := pgx.ReplicationConnect(cfg)
			if err != nil {
				kingpin.Fatalf("failed to connect to Postgres: %v", err)
			}

			sub = subscription.NewSubscription(
				logger,
				conn,
				subscription.SubscriptionOptions{
					Name:            *slotName,
					Publication:     *publicationName,
					StatusHeartbeat: *streamStatusHeartbeat,
				},
			)

			if sub.CreateReplicationSlot(ctx); err != nil {
				kingpin.Fatalf("failed to create replication slot: %v", err)
			}

			g.Add(
				func() error {
					return sub.StartReplication(ctx)
				},
				handleError(logger),
			)
		}

		{
			logger := kitlog.With(logger, "component", "consumer")

			g.Add(
				func() error {
					if *streamDecodeOnly {
						for msg := range sub.Receive() {
							spew.Dump(msg)
						}

						return nil
					}

					entries := subscription.BuildChangelog(logger, sub.Receive())
					return sink.Consume(ctx, entries, func(entry changelog.Entry) {
						if entry.Modification != nil && entry.Modification.LSN != nil {
							sub.ConfirmReceived(*entry.Modification.LSN)
						}
					})
				},
				handleError(logger),
			)
		}

		if err := g.Run(); err != nil {
			logger.Log("error", err.Error(), "msg", "exiting with error")
		}

	default:
		panic("unsupported command")
	}
}

// Set by goreleaser
var (
	Version   = "dev"
	Commit    = "none"
	Date      = "unknown"
	GoVersion = runtime.Version()
)

func versionStanza() string {
	return fmt.Sprintf(
		"pg2sink Version: %v\nGit SHA: %v\nGo Version: %v\nGo OS/Arch: %v/%v\nBuilt at: %v",
		Version, Commit, GoVersion, runtime.GOOS, runtime.GOARCH, Date,
	)
}

// setupSignalHandler is similar to the community provided functions, but follows a more
// modern pattern using contexts. If the caller desires a channel that will be closed on
// completion, they can simply use ctx.Done()
func setupSignalHandler() (context.Context, func()) {
	ctx, cancel := context.WithCancel(context.Background())

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)

	go func() {
		<-sigc
		cancel()
		<-sigc
		panic("received second signal, exiting immediately")
	}()

	return ctx, cancel
}
