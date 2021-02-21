package cmd

import (
	"context"
	"database/sql"
	"fmt"
	stdlog "log"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lawrencejones/pgsink/api"
	"github.com/lawrencejones/pgsink/api/gen/health"
	apiimports "github.com/lawrencejones/pgsink/api/gen/imports"
	apisubscriptions "github.com/lawrencejones/pgsink/api/gen/subscriptions"
	"github.com/lawrencejones/pgsink/api/gen/tables"
	middleware "github.com/lawrencejones/pgsink/internal/middleware"
	"github.com/lawrencejones/pgsink/internal/migration"
	"github.com/lawrencejones/pgsink/internal/telem"
	"github.com/lawrencejones/pgsink/pkg/changelog"
	"github.com/lawrencejones/pgsink/pkg/decode"
	"github.com/lawrencejones/pgsink/pkg/decode/gen/mappings"
	"github.com/lawrencejones/pgsink/pkg/imports"
	sinkbigquery "github.com/lawrencejones/pgsink/pkg/sinks/bigquery"
	sinkfile "github.com/lawrencejones/pgsink/pkg/sinks/file"
	"github.com/lawrencejones/pgsink/pkg/sinks/generic"
	"github.com/lawrencejones/pgsink/pkg/subscription"

	"contrib.go.opencensus.io/exporter/jaeger"
	"contrib.go.opencensus.io/integrations/ocsql"
	"github.com/alecthomas/kingpin"
	"github.com/davecgh/go-spew/spew"
	"github.com/getsentry/sentry-go"
	kitlog "github.com/go-kit/kit/log"
	level "github.com/go-kit/kit/log/level"
	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/stdlib"
	"github.com/oklog/run"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opencensus.io/trace"
	goa "goa.design/goa/v3/pkg"
)

var logger kitlog.Logger

var (
	app = kingpin.New("pgsink", "Logically replicate data out of Postgres into sinks (files, Google BigQuery, etc)").Version(versionStanza())

	// Global flags
	debug               = app.Flag("debug", "Enable debug logging").Default("false").Bool()
	metricsAddress      = app.Flag("metrics-address", "Address to bind HTTP metrics listener").Default("127.0.0.1").String()
	metricsPort         = app.Flag("metrics-port", "Port to bind HTTP metrics listener").Default("9525").Uint16()
	jaegerEnabled = app.Flag("jaeger-enabled", "Whether we should export spans to Jaeger").Default("false").Bool()
	jaegerAgentEndpoint = app.Flag("jaeger-agent-endpoint", "Endpoint for Jaeger agent").Default("localhost:6831").String()
	sentryDSN           = app.Flag("sentry-dsn", "DSN key for the Sentry exceptions project").Envar("PGSINK_SENTRY_DSN").String()

	// Each subscription has a name and a unique identifier
	subscriptionName = app.Flag("subscription-name", "Subscription name, matches Postgres publication").Default("pgsink").String()
	schemaName       = app.Flag("schema", "Postgres schema name, in which we store pgsink resources").Default("pgsink").String()

	// Control-plane APIs are best served independently from the workload they measure, so
	// provide a separate serve command for those who want to boot it separately. The same
	// API is served in stream, if --serve=true is provided.
	serveCmd     = app.Command("serve", "Serve the API control-server")
	serveAddress = app.Flag("address", "Listen on this address").Default("localhost:8080").String()

	stream           = app.Command("stream", "Stream changes into sink")
	streamConsume    = stream.Flag("consume", "Consume messages from the subscription").Default("true").Bool()
	streamDecodeOnly = stream.Flag("decode-only", "Print messages only, ignoring sink").Default("false").Bool()

	streamOptions = new(subscription.StreamOptions).Bind(stream, "")

	streamServe = stream.Flag("serve", "Enable the API control-server").Default("true").Bool()
	streamServeAddress = stream.Flag("serve.address", "Listen on this address").Default("localhost:8080").String()

	streamSubscriptionManager        = stream.Flag("subscription-manager", "Auto-manage tables into the subscription, modifying the PG publication").Default("false").Bool()
	streamSubscriptionManagerOptions = new(subscription.ManagerOptions).Bind(stream, "subscription-manager.")

	streamSinkType            = stream.Flag("sink", "Type of sink target").Required().String()
	streamSinkFileOptions     = new(sinkfile.Options).Bind(stream, "sink.file.")
	streamSinkBigQueryOptions = new(sinkbigquery.Options).Bind(stream, "sink.bigquery.")

	streamImportManager        = stream.Flag("import-manager", "Schedule imports for subscribed tables that aren't yet imported").Default("true").Bool()
	streamImportManagerOptions = new(imports.ManagerOptions).Bind(stream, "import-manager.")

	streamImportWorkerCount   = stream.Flag("import-worker-count", "Number of concurrent import workers").Default("1").Int()
	streamImportWorkerOptions = new(imports.WorkerOptions).Bind(stream, "import-worker.")
	streamImporterOptions     = new(imports.ImporterOptions).Bind(stream, "import-worker.")
)

func Run() (err error) {
	command := kingpin.MustParse(app.Parse(os.Args[1:]))

	logger = kitlog.NewLogfmtLogger(kitlog.NewSyncWriter(os.Stderr))
	if *debug {
		logger = level.NewFilter(logger, level.AllowDebug())
	} else {
		logger = level.NewFilter(logger, level.AllowInfo())
	}
	logger = kitlog.With(logger, "ts", kitlog.DefaultTimestampUTC, "caller", kitlog.DefaultCaller)
	stdlog.SetOutput(kitlog.NewStdlibAdapter(logger))

	// This is the root context for the application. Once terminated, everything we have
	// started should also finish.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// We stash loggers into the context parameter of each function. This allows logs to be
	// annotated with operation-specific information, so that we may associate logs with
	// their original cause.
	//
	// This means we need to stash a logger onto our root context, so methods can reliably
	// fetch it even if we haven't set it for the current operation.
	ctx, logger = telem.WithLogger(ctx, logger)

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

	// Don't allow us to use the default search path, as we'd rather fail-fast than
	// accidentally misreference a table and find it in the public schema.
	db, cfg, repCfg, err := buildDBConfig(fmt.Sprintf("search_path=%s", *schemaName), buildDBLogger(debug))
	if err != nil {
		app.FatalUsage("invalid postgres configuration: %v", err.Error())
	}

	logger.Log("event", "database_config",
		"host", cfg.Host,
		"port", cfg.Port,
		"database", cfg.Database,
		"user", cfg.User,
		"schema", *schemaName,
	)

	// Auto-migrate. We might want to prevent automatic install before GA, but it's much
	// smoother to automatically handle this for now.
	if err := migration.Migrate(ctx, logger, db, *schemaName); err != nil {
		return errors.Wrap(err, "failed to run migrations")
	}

	var g run.Group

	{
		logger := kitlog.With(logger, "component", "shutdown_handler")

		ctx, cancel := context.WithCancel(ctx)

		// If we're asked to shutdown, we use the rungroup to trigger interrupts for every
		// component
		g.Add(
			reportCompletion(logger, func() error {
				select {
				case <-shutdown:
					logger.Log("event", "requesting_shutdown", "msg", "received signal, requesting shutdown")
				case <-ctx.Done():
				}

				return nil
			}),
			func(error) {
				cancel() // end the shutdown select
			},
		)
	}

	// If we're given a Sentry DSN, we'll want to initialise the client. Sentry clients can
	// be acquired via the sentry.Hub helpers, which manage a collection of clients on
	// behalf of many concurrent processes.
	//
	// https://pkg.go.dev/github.com/getsentry/sentry-go
	if *sentryDSN != "" {
		// Log anything Sentry related if we're in debug mode too
		if *debug {
			sentry.Logger.SetOutput(kitlog.NewStdlibAdapter(logger))
		}

		err := sentry.Init(sentry.ClientOptions{
			Dsn:              *sentryDSN,
			AttachStacktrace: true,
		})
		if err != nil {
			return fmt.Errorf("failed to initialise Sentry: %w", err)
		}

		defer func() {
			logger.Log("msg", "flushing any pending Sentry exceptions")
			sentry.Flush(5 * time.Second)
		}()
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
		srv.Handler = mux

		g.Add(
			reportCompletion(logger, func() error {
				logger.Log("event", "listen", "address", *metricsAddress, "port", *metricsPort)
				if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
					return err
				}

				return nil
			}),
			func(error) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				srv.Shutdown(ctx)
			},
		)
	}

	if *jaegerEnabled {
		// Tracing with jaeger
		jexporter, err := jaeger.NewExporter(jaeger.Options{
			AgentEndpoint: *jaegerAgentEndpoint,
			Process: jaeger.Process{
				ServiceName: "pgsink",
			},
		})
		if err != nil {
			return err
		}

		defer func() {
			done := make(chan struct{})
			go func() {
				jexporter.Flush()
				close(done)
			}()

			select {
			case <-time.After(3 * time.Second):
				logger.Log("event", "jaeger_timeout", "msg", "timed out waiting to flush jaeger exporter")
			case <-done:
			}
		}()

		trace.RegisterExporter(jexporter)
		trace.ApplyConfig(trace.Config{DefaultSampler: trace.AlwaysSample()})
	}

	switch command {
	case serveCmd.FullCommand():
		if err := addAPIServer(ctx, logger, g, db, *serveAddress, *subscriptionName); err != nil {
			return err
		}

		return g.Run()

	case stream.FullCommand():
		var (
			sub     *subscription.Subscription
			stream  *subscription.Stream
			sink    generic.Sink
			decoder = decode.NewDecoder(mappings.Mappings)
		)

		switch *streamSinkType {
		case "file":
			sink, err = sinkfile.New(*streamSinkFileOptions)
		case "bigquery":
			sink, err = sinkbigquery.New(ctx, decoder, *streamSinkBigQueryOptions)
		default:
			app.FatalUsage(fmt.Sprintf("unsupported sink type: %s", *streamSinkType))
		}

		if err != nil {
			return err
		}

		repconn, err := pgx.ConnectConfig(ctx, repCfg)
		if err != nil {
			app.FatalUsage("failed to open replication connection: %v", err)
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

		if *streamServe {
			if err := addAPIServer(ctx, logger, g, db, *streamServeAddress, *subscriptionName); err != nil {
				return err
			}
		}

		if *streamConsume {
			ctx, logger := telem.WithLogger(ctx, logger, "component", "subscription")

			stream, err = sub.Start(ctx, logger, repconn, *streamOptions)
			if err != nil {
				return fmt.Errorf("failed to start subscription: %w", err)
			}

			g.Add(
				reportCompletion(logger, func() error {
					if *streamDecodeOnly {
						for msg := range stream.Messages() {
							spew.Dump(msg)
						}

						return nil
					}

					entries := subscription.BuildChangelog(logger, decoder, stream)
					return sink.Consume(ctx, entries, func(entry changelog.Entry) {
						if entry.Modification != nil && entry.Modification.LSN != nil {
							stream.Confirm(pglogrepl.LSN(*entry.Modification.LSN))
						}
					})
				}),
				func(error) {
					stream.Shutdown(ctx)
				},
			)
		}

		if *streamSubscriptionManager {
			ctx, logger := telem.WithLogger(ctx, logger, "component", "subscription_manager")
			manager := subscription.NewManager(db, *streamSubscriptionManagerOptions)

			g.Add(
				reportCompletion(logger, func() error {
					return manager.Manage(ctx, logger, *sub)
				}),
				func(error) {
					manager.Shutdown(ctx)
				},
			)
		}

		if *streamImportManager {
			ctx, logger := telem.WithLogger(ctx, logger, "component", "import_manager")
			manager := imports.NewManager(logger, db, *streamImportManagerOptions)

			g.Add(
				reportCompletion(logger, func() error {
					return manager.Manage(ctx, *sub)
				}),
				func(error) {
					manager.Shutdown(ctx)
				},
			)
		}

		for idx := 0; idx < *streamImportWorkerCount; idx++ {
			ctx, logger := telem.WithLogger(ctx, logger, "component", "import_worker", "worker_id", idx)

			// Assign the subscription ID from what we generated on boot
			streamImportWorkerOptions.SubscriptionID = sub.ID

			importer := imports.NewImporter(sink, decoder, *streamImporterOptions)
			worker := imports.NewWorker(logger, db, *streamImportWorkerOptions)

			g.Add(
				reportCompletion(logger, func() error {
					return worker.Start(ctx, importer)
				}),
				func(error) {
					worker.Shutdown(ctx)
				},
			)
		}

		return g.Run()
	}

	app.FatalUsage(fmt.Sprintf("unsupported command: %s", command))
	panic("unreachable")
}

// addAPIServer registers an API server onto the run.Group, allowing us to register it
// for multiple commands.
func addAPIServer(ctx context.Context, logger kitlog.Logger, g run.Group, db *sql.DB, serveAddress, subscriptionName string) error {
	pub, err := subscription.FindPublication(ctx, db, subscriptionName)
	if err != nil {
		return fmt.Errorf(
			"failed to find publication for subscription name '%s': %w", subscriptionName, err)
	}
	if pub == nil {
		return fmt.Errorf(
			"no publication for subscription name '%s', have you started a stream against this database?", subscriptionName)
	}

	// Initialise services
	var (
		tablesService        = api.NewTables(db, pub)
		importsService       = api.NewImports(db, pub)
		subscriptionsService = api.NewSubscriptions(db, pub)
		healthService        = api.NewHealth()
	)

	// Wrap services in endpoints, a calling abstraction that is transport independent
	var (
		tablesEndpoints        = tables.NewEndpoints(tablesService)
		importsEndpoints       = apiimports.NewEndpoints(importsService)
		subscriptionsEndpoints = apisubscriptions.NewEndpoints(subscriptionsService)
		healthEndpoints        = health.NewEndpoints(healthService)
	)

	// Apply application middlewares to all the endpoint groups
	{
		for _, endpoints := range []interface {
			Use(m func(goa.Endpoint) goa.Endpoint)
		}{
			tablesEndpoints,
			importsEndpoints,
			subscriptionsEndpoints,
			healthEndpoints,
		} {
			endpoints.Use(middleware.Observe())
		}
	}

	{
		logger := kitlog.With(logger, "component", "http")
		srv := buildHTTPServer(logger, serveAddress,
			tablesEndpoints,
			importsEndpoints,
			subscriptionsEndpoints,
			healthEndpoints,
			*debug)

		g.Add(
			reportCompletion(logger, func() error {
				logger.Log("event", "listen", "address", serveAddress)
				if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
					return err
				}

				return nil
			}),
			func(error) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				srv.Shutdown(ctx)
			},
		)
	}

	return nil
}

// reportCompletion wraps a standard action function with a log message that triggers on
// completion. Add actions added to a RunGroup should be wrapped thusly, to ensure
// termination can be traced back to a specific action.
func reportCompletion(logger kitlog.Logger, action func() error) func() error {
	return func() error {
		err := action()
		if err != nil {
			logger.Log("event", "finish_with_error", "error", err)
		} else {
			logger.Log("event", "finish")
		}

		return err
	}
}

// pgxLoggerFunc wraps a function to satisfy the pgx logger interface
type pgxLoggerFunc func(ctx context.Context, level pgx.LogLevel, msg string, data map[string]interface{})

func (f pgxLoggerFunc) Log(ctx context.Context, level pgx.LogLevel, msg string, data map[string]interface{}) {
	f(ctx, level, msg, data)
}

// buildDBLogger produces a pgx.Logger that can trace SQL operations at the connection
// level, which is important when the ocsql package can only support commands going via
// sql.DB. It logs using the context logger, meaning the logs appear with the right
// correlation IDs.
//
// We also log queries, in debug mode, helping to locally reproduce issues.
func buildDBLogger(enableLogs *bool) pgx.Logger {
	return pgxLoggerFunc(func(ctx context.Context, level pgx.LogLevel, msg string, data map[string]interface{}) {
		queryPID, _ := data["pid"].(uint32)
		queryCommandTag, _ := data["commandTag"].(pgconn.CommandTag)
		querySQL, _ := data["sql"].(string)
		queryArgs, _ := data["args"].([]string)

		// Fetch the logger from the current context
		logger := telem.LoggerFrom(ctx)

		span := trace.FromContext(ctx)
		if span != nil {
			logger = kitlog.With(logger, "trace_id", span.SpanContext().TraceID)
			span.Annotate([]trace.Attribute{
				trace.StringAttribute("query_pid", fmt.Sprintf("%v", queryPID)),
				trace.StringAttribute("query_command_tag", queryCommandTag.String()),
				trace.StringAttribute("query_sql", querySQL),
			}, msg)
		}

		// Only log if the enabled flag is set to true. This allows an operator to toggle
		// database logging in realtime.
		if !*enableLogs {
			return
		}

		logger.Log("event", "pgx", "msg", msg,
			"query_pid", queryPID,
			"query_command_tag", queryCommandTag,
			"query_sql", querySQL,
			"query_args", spew.Sprint(queryArgs))
	})
}

func buildDBConfig(base string, logger pgx.Logger) (db *sql.DB, cfg *pgx.ConnConfig, repCfg *pgx.ConnConfig, err error) {
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

	// Ensure both connections are assigned the logger, which powers tracing and logs
	cfg.Logger = logger
	repCfg.Logger = logger

	// Register the pgx driver via an ocsql (OpenCensus) SQL tracer. The driverName can then
	// be used to open a *sql.DB in the normal way.
	driverName, err := ocsql.Register("pgx",
		ocsql.WithAllTraceOptions(),
		// Turn off rows.next, as we make many of these calls
		ocsql.WithRowsNext(false),
		ocsql.WithInstanceName("db"))
	if err != nil {
		return nil, nil, nil, err
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
	db, err = sql.Open(driverName, connStr)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to initialise db.SQL: %w", err)
	}

	return
}
