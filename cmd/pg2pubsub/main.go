package main

import (
	"context"
	"encoding/json"
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
	"github.com/lawrencejones/pg2pubsub/pkg/imports"
	"github.com/lawrencejones/pg2pubsub/pkg/migration"
	"github.com/lawrencejones/pg2pubsub/pkg/publication"
	"github.com/lawrencejones/pg2pubsub/pkg/subscription"
	"github.com/oklog/run"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var logger kitlog.Logger

var (
	app = kingpin.New("pg2pubsub", "Publish Postgres changes to pubsub").Version(versionStanza())

	// Global flags applying to every command
	debug          = app.Flag("debug", "Enable debug logging").Default("false").Bool()
	metricsAddress = app.Flag("metrics-address", "Address to bind HTTP metrics listener").Default("127.0.0.1").String()
	metricsPort    = app.Flag("metrics-port", "Port to bind HTTP metrics listener").Default("9525").Uint16()

	host                      = app.Flag("host", "Postgres host").Envar("PGHOST").Default("127.0.0.1").String()
	port                      = app.Flag("port", "Postgres port").Envar("PGPORT").Default("5432").Uint16()
	database                  = app.Flag("database", "Postgres database name").Envar("PGDATABASE").Default("postgres").String()
	user                      = app.Flag("user", "Postgres user").Envar("PGUSER").Default("postgres").String()
	name                      = app.Flag("name", "Publication name").Default("pg2pubsub").String()
	slotName                  = app.Flag("slot-name", "Replication slot name").Default("pg2pubsub").String()
	schemas                   = app.Flag("schema", "Postgres schema to watch for changes").Default("public").Strings()
	excludes                  = app.Flag("exclude", "Table name to exclude from changes").Strings()
	includes                  = app.Flag("include", "Table name to include from changes (activates whitelist)").Strings()
	pollInterval              = app.Flag("poll-interval", "Interval to poll for new tables").Default("10s").Duration()
	statusHeartbeat           = app.Flag("status-heartbeat", "Interval to heartbeat replication primary").Default("10s").Duration()
	decodeOnly                = app.Flag("decode-only", "Interval to heartbeat replication primary").Default("false").Bool()
	modificationWorkerCount   = app.Flag("modification-worker-count", "Workers for building modifications").Default("1").Int()
	importManagerPollInterval = app.Flag("import-manager-poll-interval", "Interval to poll for newly published tables").Default("10s").Duration()
	importerPollInterval      = app.Flag("importer-poll-interval", "Interval to poll for new import jobs").Default("10s").Duration()
	importerWorkerCount       = app.Flag("importer-worker-count", "Workers for processing imports").Default("1").Int()
)

func main() {
	_ = kingpin.MustParse(app.Parse(os.Args[1:]))

	logger = kitlog.NewLogfmtLogger(kitlog.NewSyncWriter(os.Stderr))

	if *debug {
		logger = level.NewFilter(logger, level.AllowDebug())
	} else {
		logger = level.NewFilter(logger, level.AllowInfo())
	}

	logger = kitlog.With(logger, "ts", kitlog.DefaultTimestampUTC, "caller", kitlog.DefaultCaller)
	stdlog.SetOutput(kitlog.NewStdlibAdapter(logger))

	go func() {
		logger.Log("event", "metrics.listen", "address", *metricsAddress, "port", *metricsPort)
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(fmt.Sprintf("%s:%v", *metricsAddress, *metricsPort), nil)
	}()

	ctx, cancel := setupSignalHandler()
	defer cancel()

	logger.Log("event", "database_config", "host", *host, "port", *port, "database", *database, "user", user)
	cfg := pgx.ConnConfig{
		Host:     *host,
		Port:     *port,
		Database: *database,
		User:     *user,
	}

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

	// TODO: This is useful only while developing
	outputMessages := make(chan interface{})
	defer close(outputMessages)

	go func() {
		for msg := range outputMessages {
			bytes, err := json.MarshalIndent(msg, "", "  ")
			if err != nil {
				panic(err)
			}

			fmt.Println(string(bytes))
		}
	}()

	var g run.Group
	var sub *subscription.Subscription
	var pubmgr *publication.PublicationManager

	{
		logger := kitlog.With(logger, "component", "publication")

		var err error
		pubmgr, err = publication.CreatePublicationManager(
			ctx,
			logger,
			mustConnectionPool(2),
			publication.PublicationManagerOptions{
				Name:         *name,
				Schemas:      *schemas,
				Excludes:     *excludes,
				Includes:     *includes,
				PollInterval: *pollInterval,
			},
		)

		if err != nil {
			kingpin.Fatalf("failed to create publication: %v", err)
		}

		g.Add(
			func() error {
				return pubmgr.Sync(ctx)
			},
			handleError(logger),
		)
	}

	{
		logger := kitlog.With(logger, "component", "import_manager")

		manager := imports.NewManager(
			logger,
			mustConnectionPool(1),
			imports.ManagerOptions{
				PublicationID: pubmgr.GetPublicationID(),
				PollInterval:  *importManagerPollInterval,
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
			mustConnectionPool(*importerWorkerCount),
			imports.ImporterOptions{
				WorkerCount:   *importerWorkerCount,
				PollInterval:  *importerPollInterval,
				PublicationID: pubmgr.GetPublicationID(),
			},
		)

		entries := importer.Work(ctx)

		g.Add(
			func() error {
				for entry := range entries {
					outputMessages <- entry.Unwrap()
				}

				return nil
			},
			handleError(logger),
		)
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
				Publication:     *name,
				StatusHeartbeat: *statusHeartbeat,
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
				if *decodeOnly {
					for msg := range sub.Receive() {
						spew.Dump(msg)
					}

					return nil
				}

				for entry := range subscription.BuildChangelog(logger, sub.Receive()) {
					outputMessages <- entry.Unwrap()

					if entry.Modification != nil {
						sub.ConfirmReceived(*entry.Modification.LSN)
					}
				}

				return nil
			},
			handleError(logger),
		)
	}

	if err := g.Run(); err != nil {
		logger.Log("error", err.Error(), "msg", "exiting with error")
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
		"pg2pubsub Version: %v\nGit SHA: %v\nGo Version: %v\nGo OS/Arch: %v/%v\nBuilt at: %v",
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
