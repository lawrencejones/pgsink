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

	"github.com/lawrencejones/pg2sink/pkg/changelog"
	"github.com/lawrencejones/pg2sink/pkg/imports"
	"github.com/lawrencejones/pg2sink/pkg/migration"
	"github.com/lawrencejones/pg2sink/pkg/publication"
	sinkbigquery "github.com/lawrencejones/pg2sink/pkg/sinks/bigquery"
	sinkfile "github.com/lawrencejones/pg2sink/pkg/sinks/file"
	"github.com/lawrencejones/pg2sink/pkg/sinks/generic"
	"github.com/lawrencejones/pg2sink/pkg/subscription"

	"contrib.go.opencensus.io/exporter/jaeger"
	"github.com/alecthomas/kingpin"
	"github.com/davecgh/go-spew/spew"
	kitlog "github.com/go-kit/kit/log"
	level "github.com/go-kit/kit/log/level"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/oklog/run"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opencensus.io/trace"
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

	stream                          = app.Command("stream", "Stream changes into sink")
	streamMetricsAddress            = stream.Flag("metrics-address", "Address to bind HTTP metrics listener").Default("127.0.0.1").String()
	streamMetricsPort               = stream.Flag("metrics-port", "Port to bind HTTP metrics listener").Default("9525").Uint16()
	streamJaegerAgentEndpoint       = stream.Flag("jaeger-agent-endpoint", "Endpoint for Jaeger agent").Default("localhost:6831").String()
	streamManagePublication         = stream.Flag("manage-publication", "Auto-manage tables in publication").Default("false").Bool()
	streamSchema                    = stream.Flag("schema", "Postgres schema to watch for changes").Default("public").String()
	streamExcludes                  = stream.Flag("exclude", "Table name to exclude from changes").Strings()
	streamIncludes                  = stream.Flag("include", "Table name to include from changes (activates whitelist)").Strings()
	streamPollInterval              = stream.Flag("poll-interval", "Interval to poll for new tables").Default("10s").Duration()
	streamStatusHeartbeat           = stream.Flag("status-heartbeat", "Interval to heartbeat replication primary").Default("10s").Duration()
	streamDecodeOnly                = stream.Flag("decode-only", "Interval to heartbeat replication primary").Default("false").Bool()
	streamImportManagerPollInterval = stream.Flag("import-manager-poll-interval", "Interval to poll for newly published tables").Default("10s").Duration()
	streamImporterPollInterval      = stream.Flag("importer-poll-interval", "Interval to poll for new import jobs").Default("10s").Duration()
	streamImporterWorkerCount       = stream.Flag("importer-worker-count", "Workers for processing imports").Default("0").Int()
	streamImporterSnapshotTimeout   = stream.Flag("importer-snapshot-timeout", "Maximum time to hold Postgres snapshots").Default("1m").Duration()
	streamImporterBatchLimit        = stream.Flag("importer-batch-limit", "Maximum rows to pull per import job pagination").Default("100000").Int()
	streamImporterBufferSize        = stream.Flag("importer-buffer-size", "Buffer between pulling data from Postgres and sink").Default("100000").Int()

	streamSinkType            = stream.Flag("sink", "Type of sink target").Required().String()
	streamSinkFileOptions     = new(sinkfile.Options).Bind(stream, "sink.file.")
	streamSinkBigQueryOptions = new(sinkbigquery.Options).Bind(stream, "sink.bigquery.")
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

	// pgx makes it difficult to use the raw pgconn.Config struct without going via the
	// ParseConfig method. We compromise by rendering a connection string for our overrides
	// and relying on ParseConfig to identify additional Postgres parameters from libpq
	// compatible environment variables.
	cfg, err := pgxpool.ParseConfig(fmt.Sprintf("host=%s port=%d database=%s user=%s", *host, *port, *database, *user))
	if err != nil {
		kingpin.Fatalf("invalid postgres configuration: %v", err.Error())
	}

	logger.Log("event", "database_config",
		"host", cfg.ConnConfig.Host,
		"port", cfg.ConnConfig.Port,
		"database", cfg.ConnConfig.Database,
		"user", cfg.ConnConfig.User,
	)

	mustConnectionPool := func(size int) *pgxpool.Pool {
		cfg := *cfg
		cfg.MaxConns = int32(size)

		pool, err := pgxpool.ConnectConfig(ctx, &cfg)

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

	if err := migration.Migrate(ctx, logger, *cfg); err != nil {
		kingpin.Fatalf("failed to migrate database: %v", err)
	}

	switch command {
	case stream.FullCommand():
		var (
			sink   generic.Sink
			stream *subscription.Stream
			pubmgr *publication.Manager
		)

		mustSink := func(sink generic.Sink, err error) generic.Sink {
			if err != nil {
				kingpin.Fatalf("failed to create file sink: %v", err)
			}

			return sink
		}

		switch *streamSinkType {
		case "file":
			sink = mustSink(sinkfile.New(logger, *streamSinkFileOptions))
		case "bigquery":
			sink = mustSink(sinkbigquery.New(ctx, logger, *streamSinkBigQueryOptions))
		default:
			kingpin.Fatalf("unsupported sink type: %s", *streamSinkType)
		}

		var g run.Group

		logger.Log("event", "metrics.listen", "address", *streamMetricsAddress, "port", *streamMetricsPort)
		http.Handle("/metrics", promhttp.Handler())
		go http.ListenAndServe(fmt.Sprintf("%s:%v", *streamMetricsAddress, *streamMetricsPort), nil)

		jexporter, err := jaeger.NewExporter(jaeger.Options{
			AgentEndpoint: *streamJaegerAgentEndpoint,
			Process: jaeger.Process{
				ServiceName: "pg2sink",
			},
		})

		if err != nil {
			kingpin.Fatalf("failed to configure jaeger: %v", err)
		}

		trace.RegisterExporter(jexporter)
		trace.ApplyConfig(trace.Config{DefaultSampler: trace.AlwaysSample()})

		{
			logger := kitlog.With(logger, "component", "publication")

			var err error
			pubmgr, err = publication.CreateManager(
				ctx,
				logger,
				mustConnectionPool(2),
				publication.ManagerOptions{
					Name:         *publicationName,
					Schemas:      []string{*streamSchema},
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

			if *streamImporterWorkerCount > 0 {
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

			params := map[string]string{"replication": "database"}
			for key, value := range cfg.ConnConfig.RuntimeParams {
				params[key] = value
			}

			replicationCfg := *cfg.ConnConfig
			replicationCfg.RuntimeParams = params

			conn, err := pgx.ConnectConfig(ctx, &replicationCfg)
			if err != nil {
				kingpin.Fatalf("failed to connect to Postgres: %v", err)
			}

			sub, err := subscription.Create(
				ctx,
				logger,
				conn.PgConn(),
				subscription.SubscriptionOptions{
					Name:            *slotName,
					Publication:     *publicationName,
					StatusHeartbeat: *streamStatusHeartbeat,
				},
			)

			if err != nil {
				kingpin.Fatalf("failed to create subscription: %v", err)
			}

			stream, err = sub.Start(ctx, logger)
			if err != nil {
				kingpin.Fatalf("failed to start subscription: %v", err)
			}

			g.Add(
				func() error {
					return stream.Wait(context.Background())
				},
				handleError(logger),
			)
		}

		{
			logger := kitlog.With(logger, "component", "consumer")

			g.Add(
				func() error {
					if *streamDecodeOnly {
						for msg := range stream.Messages() {
							spew.Dump(msg)
						}

						return nil
					}

					entries := subscription.BuildChangelog(logger, stream.Messages())
					return sink.Consume(ctx, entries, func(entry changelog.Entry) {
						if entry.Modification != nil && entry.Modification.LSN != nil {
							stream.Confirm(pglogrepl.LSN(*entry.Modification.LSN))
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
