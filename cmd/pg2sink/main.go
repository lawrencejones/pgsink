package main

import (
	"context"
	"database/sql"
	"fmt"
	stdlog "log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/lawrencejones/pg2sink/pkg/changelog"
	"github.com/lawrencejones/pg2sink/pkg/migration"
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
	"github.com/jackc/pgx/v4/stdlib"
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

	// Each subscription has a name and a unique identifier
	subscriptionName = app.Flag("subscription-name", "Subscription name, matches Postgres publication").Default("pg2sink").String()

	stream                    = app.Command("stream", "Stream changes into sink")
	streamMetricsAddress      = stream.Flag("metrics-address", "Address to bind HTTP metrics listener").Default("127.0.0.1").String()
	streamMetricsPort         = stream.Flag("metrics-port", "Port to bind HTTP metrics listener").Default("9525").Uint16()
	streamJaegerAgentEndpoint = stream.Flag("jaeger-agent-endpoint", "Endpoint for Jaeger agent").Default("localhost:6831").String()
	streamDecodeOnly          = stream.Flag("decode-only", "Print messages only, ignoring sink").Default("false").Bool()

	streamOptions = new(subscription.StreamOptions).Bind(stream, "")

	streamSubscriptionManager        = stream.Flag("subscription-manager", "Auto-manage tables into the subscription, modifying the PG publication").Default("false").Bool()
	streamSubscriptionManagerOptions = new(subscription.ManagerOptions).Bind(stream, "subscription-manager.")

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

	db, cfg, repCfg, err := buildDBConfig(fmt.Sprintf("host=%s port=%d database=%s user=%s", *host, *port, *database, *user))
	if err != nil {
		kingpin.Fatalf("invalid postgres configuration: %v", err.Error())
	}

	logger.Log("event", "database_config",
		"host", cfg.Host,
		"port", cfg.Port,
		"database", cfg.Database,
		"user", cfg.User,
	)

	handleError := func(logger kitlog.Logger) func(error) {
		return func(err error) {
			if err != nil {
				logger.Log("error", err.Error(), "msg", "received error, cancelling context")
			}
			cancel()
		}
	}

	if err := migration.Migrate(ctx, logger, db); err != nil {
		kingpin.Fatalf("failed to migrate database: %v", err)
	}

	switch command {
	case stream.FullCommand():
		var (
			sub    *subscription.Subscription
			stream *subscription.Stream
			sink   generic.Sink
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

		http.Handle("/metrics", promhttp.Handler())
		go func() {
			logger.Log("event", "metrics.listen", "address", *streamMetricsAddress, "port", *streamMetricsPort)
			http.ListenAndServe(fmt.Sprintf("%s:%v", *streamMetricsAddress, *streamMetricsPort), nil)
		}()

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
			logger := kitlog.With(logger, "component", "subscription")

			repconn, err := pgx.ConnectConfig(ctx, repCfg)
			if err != nil {
				kingpin.Fatalf("failed to open replication connection: %v", err)
			}

			// Initialise our subscription, a process that requires both a replication and a
			// standard connection. We'll reuse the replication connection to power the stream.
			sub, err = subscription.Create(
				ctx, logger, db, repconn, subscription.SubscriptionOptions{
					Name: *subscriptionName,
				})

			if err != nil {
				kingpin.Fatalf("failed to create subscription: %v", err)
			}

			stream, err = sub.Start(ctx, logger, repconn, *streamOptions)
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
			logger := kitlog.With(logger, "component", "subscription_consumer")

			g.Add(
				func() error {
					if *streamDecodeOnly {
						for msg := range stream.Messages() {
							spew.Dump(msg)
						}

						return nil
					}

					entries := subscription.BuildChangelog(logger, stream)
					return sink.Consume(ctx, entries, func(entry changelog.Entry) {
						if entry.Modification != nil && entry.Modification.LSN != nil {
							stream.Confirm(pglogrepl.LSN(*entry.Modification.LSN))
						}
					})
				},
				handleError(logger),
			)
		}

		if *streamSubscriptionManager {
			logger := kitlog.With(logger, "component", "subscription_manager")

			manager := subscription.NewManager(logger, db, *streamSubscriptionManagerOptions)

			g.Add(
				func() error {
					return manager.Manage(ctx, *sub)
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

func buildDBConfig(base string) (db *sql.DB, cfg *pgx.ConnConfig, repCfg *pgx.ConnConfig, err error) {
	// pgx makes it difficult to use the raw pgconn.Config struct without going via the
	// ParseConfig method. We compromise by rendering a connection string for our overrides
	// and relying on ParseConfig to identify additional Postgres parameters from libpq
	// compatible environment variables.
	cfg, err = pgx.ParseConfig(base)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("invalid database configuration: %w", err)
	}

	// In addition to standard connections, we'll also need a replication connection.
	repCfg, err = pgx.ParseConfig(fmt.Sprintf("%s replication=database", base))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("invalid replication database configuration: %w", err)
	}

	// When interacting with standard database objects, it's easier to use sql.DB. This
	// allows compatibility with a wide variety of other libraries, while allowing us to
	// drop into a raw pgx connection when required like so:
	//
	//   var conn *pgx.Conn
	//   conn, _ = stdlib.AcquireConn(db)
	//   defer stdlib.ReleaseConn(db, conn)
	//
	// An exception is necessary for the replication connection, as this requires a
	// different startup paramter (replication=database).
	connStr := stdlib.RegisterConnConfig(cfg)
	db, err = sql.Open("pgx", connStr)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to initialise db.SQL: %w", err)
	}

	return
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
