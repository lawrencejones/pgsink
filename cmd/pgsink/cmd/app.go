package cmd

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	stdlog "log"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lawrencejones/pgsink/pkg/changelog"
	"github.com/lawrencejones/pgsink/pkg/imports"
	"github.com/lawrencejones/pgsink/pkg/migration"
	sinkbigquery "github.com/lawrencejones/pgsink/pkg/sinks/bigquery"
	sinkfile "github.com/lawrencejones/pgsink/pkg/sinks/file"
	"github.com/lawrencejones/pgsink/pkg/sinks/generic"
	"github.com/lawrencejones/pgsink/pkg/subscription"

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
	app = kingpin.New("pgsink", "Publish Postgres changes to pubsub").Version(versionStanza())

	// Global flags
	debug               = app.Flag("debug", "Enable debug logging").Default("false").Bool()
	metricsAddress      = app.Flag("metrics-address", "Address to bind HTTP metrics listener").Default("127.0.0.1").String()
	metricsPort         = app.Flag("metrics-port", "Port to bind HTTP metrics listener").Default("9525").Uint16()
	jaegerAgentEndpoint = app.Flag("jaeger-agent-endpoint", "Endpoint for Jaeger agent").Default("localhost:6831").String()

	// Database connection paramters
	host     = app.Flag("host", "Postgres host").Envar("PGHOST").Default("127.0.0.1").String()
	port     = app.Flag("port", "Postgres port").Envar("PGPORT").Default("5432").Uint16()
	database = app.Flag("database", "Postgres database name").Envar("PGDATABASE").Default("postgres").String()
	user     = app.Flag("user", "Postgres user").Envar("PGUSER").Default("postgres").String()

	// Each subscription has a name and a unique identifier
	subscriptionName = app.Flag("subscription-name", "Subscription name, matches Postgres publication").Default("pgsink").String()

	stream           = app.Command("stream", "Stream changes into sink")
	streamDecodeOnly = stream.Flag("decode-only", "Print messages only, ignoring sink").Default("false").Bool()

	streamOptions = new(subscription.StreamOptions).Bind(stream, "")

	streamSubscriptionManager        = stream.Flag("subscription-manager", "Auto-manage tables into the subscription, modifying the PG publication").Default("false").Bool()
	streamSubscriptionManagerOptions = new(subscription.ManagerOptions).Bind(stream, "subscription-manager.")

	streamSinkType            = stream.Flag("sink", "Type of sink target").Required().String()
	streamSinkFileOptions     = new(sinkfile.Options).Bind(stream, "sink.file.")
	streamSinkBigQueryOptions = new(sinkbigquery.Options).Bind(stream, "sink.bigquery.")

	streamImportManager        = stream.Flag("import-manager", "Schedule imports for subscribed tables that aren't yet imported").Default("true").Bool()
	streamImportManagerOptions = new(imports.ManagerOptions).Bind(stream, "import-manager.")

	streamImportWorkerCount   = stream.Flag("import-worker-count", "Number of concurrent import workers").Default("1").Int()
	streamImportWorkerOptions = new(imports.WorkerOptions).Bind(stream, "import-worker.")
)

// SilentError should be returned when the command wants to skip all logging of the error
// it has encountered. It wraps no error content as we should never inspect it.
var SilentError = errors.New("silent error")

type UsageError struct {
	error
}

func Run() (err error) {
	command := kingpin.MustParse(app.Parse(os.Args[1:]))

	logger = kitlog.NewLogfmtLogger(kitlog.NewSyncWriter(os.Stderr))
	logger = level.NewFilter(logger, level.AllowInfo())
	if *debug {
		logger = level.NewFilter(logger, level.AllowDebug())
	}
	logger = kitlog.With(logger, "ts", kitlog.DefaultTimestampUTC, "caller", kitlog.DefaultCaller)
	stdlog.SetOutput(kitlog.NewStdlibAdapter(logger))

	// Setup an error handler to log and print usage
	defer func() {
		var usageErr UsageError
		switch {
		// Do nothing if no error
		case err == nil:
			return
		// Suppress silent errors
		case errors.Is(err, SilentError):
			return
		// If we're a usage error, unwrap it and print out usage before returning
		case errors.As(err, &usageErr):
			context, _ := app.ParseContext(os.Args[1:])
			app.UsageForContext(context)
			fmt.Fprintf(os.Stderr, "error: %s\n", usageErr.Error())

			err = usageErr.error
			return
		// Otherwise we probably want to log our error
		default:
			logger.Log("event", "error", "error", err, "msg", "exiting with error")
		}
	}()

	// This is the root context for the application. Once terminated, everything we have
	// started should also finish.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Stage our shutdown to first request termination, then cancel contexts if downstream
	// workers haven't responded.
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)
	shutdown := make(chan struct{})

	go func() {
		<-sigc
		close(shutdown)
		select {
		case <-time.After(30 * time.Second):
		case <-sigc:
		}
		cancel()
	}()

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

	if err := migration.Migrate(ctx, logger, db); err != nil {
		return fmt.Errorf("failed to migrate database: %w", err)
	}

	var g run.Group

	{
		logger := kitlog.With(logger, "component", "shutdown_handler")

		ctx, cancel := context.WithCancel(ctx)

		// If we're asked to shutdown, we use the rungroup to trigger interrupts for every
		// component
		g.Add(
			func() error {
				select {
				case <-shutdown:
					logger.Log("event", "requesting_shutdown", "msg", "received signal, requesting shutdown")
				case <-ctx.Done():
				}

				return nil
			},
			func(error) {
				cancel() // end the shutdown select
			},
		)
	}

	{
		logger := kitlog.With(logger, "component", "metrics")

		// Metrics and debug endpoints
		mux := http.NewServeMux()

		mux.Handle("/metrics", promhttp.Handler())
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

		srv := &http.Server{Addr: fmt.Sprintf("%s:%d", *metricsAddress, *metricsPort)}

		g.Add(
			func() error {
				logger.Log("event", "listen", "address", *metricsAddress, "port", *metricsPort)
				if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
					return err
				}

				return nil
			},
			func(error) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				srv.Shutdown(ctx)
			},
		)
	}

	{
		// Tracing with jaeger
		jexporter, err := jaeger.NewExporter(jaeger.Options{
			AgentEndpoint: *jaegerAgentEndpoint,
			Process: jaeger.Process{
				ServiceName: "pgsink",
			},
		})

		if err != nil {
			return UsageError{err}
		}

		trace.RegisterExporter(jexporter)
		trace.ApplyConfig(trace.Config{DefaultSampler: trace.AlwaysSample()})
	}

	switch command {
	case stream.FullCommand():
		var (
			sub    *subscription.Subscription
			stream *subscription.Stream
			sink   generic.Sink
		)

		switch *streamSinkType {
		case "file":
			sink, err = sinkfile.New(logger, *streamSinkFileOptions)
		case "bigquery":
			sink, err = sinkbigquery.New(ctx, logger, *streamSinkBigQueryOptions)
		default:
			return UsageError{fmt.Errorf("unsupported sink type: %s", *streamSinkType)}
		}

		if err != nil {
			return err
		}

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
				return fmt.Errorf("failed to create subscription: %w", err)
			}

			stream, err = sub.Start(ctx, logger, repconn, *streamOptions)
			if err != nil {
				return fmt.Errorf("failed to start subscription: %w", err)
			}

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
				func(error) {
					stream.Shutdown(ctx)
				},
			)
		}

		if *streamSubscriptionManager {
			logger := kitlog.With(logger, "component", "subscription_manager")

			manager := subscription.NewManager(logger, db, *streamSubscriptionManagerOptions)

			g.Add(
				func() error {
					return manager.Manage(ctx, *sub)
				},
				func(error) {
					manager.Shutdown(ctx)
				},
			)
		}

		if *streamImportManager {
			logger := kitlog.With(logger, "component", "import_manager")

			manager := imports.NewManager(logger, db, *streamImportManagerOptions)

			g.Add(
				func() error {
					return manager.Manage(ctx, *sub)
				},
				func(error) {
					manager.Shutdown(ctx)
				},
			)
		}

		for idx := 0; idx < *streamImportWorkerCount; idx++ {
			logger := kitlog.With(logger, "component", "import_worker", "worker_id", idx)

			worker := imports.NewWorker(logger, db, sink, *streamImportWorkerOptions)

			g.Add(
				func() error {
					return worker.Start(ctx)
				},
				func(error) {
					worker.Shutdown(ctx)
				},
			)
		}

		return g.Run()
	}

	return UsageError{fmt.Errorf("unsupported command")}
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
