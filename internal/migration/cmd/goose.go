package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	// Load migrations, so the goose command can discover them
	_ "github.com/lawrencejones/pgsink/internal/migration"

	"github.com/alecthomas/kingpin"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/pressly/goose"
)

var (
	app     = kingpin.New("goose", "Manage pgsink database migrations")
	install = app.Flag("install", "Install the Postgres schema, only required for first run").Default("false").Bool()
	schema  = app.Flag("schema", "Schema for pgsink resources").Default("pgsink").String()
	dir     = app.Flag("dir", "Directory containing migrations").Default("internal/migration").String()
	command = app.Arg("command", "Command to pass to goose").Required().String()
	args    = app.Arg("args", "Arguments to goose command").Strings()
)

func main() {
	app.UsageTemplate(kingpin.DefaultUsageTemplate + `

Commands:
	up                   Migrate the DB to the most recent version available
	up-to VERSION        Migrate the DB to a specific VERSION
	down                 Roll back the version by 1
	down-to VERSION      Roll back to a specific VERSION
	redo                 Re-run the latest migration
	reset                Roll back all migrations
	status               Dump the migration status for the current DB
	version              Print the current version of the database
	create NAME [sql|go] Creates new migration file with the current timestamp
	fix                  Apply sequential ordering to migrations
`)

	if _, err := app.Parse(os.Args[1:]); err != nil {
		app.FatalUsage(err.Error())
	}

	db, err := sql.Open("pgx", fmt.Sprintf("search_path=%s,public", *schema))
	if err != nil {
		app.Fatalf("failed to open DB: %v\n", err)
	}

	defer func() {
		if err := db.Close(); err != nil {
			app.Fatalf("failed to close DB: %v\n", err)
		}
	}()

	if *install {
		log.Printf("requested --install, so creating schema '%s'", *schema)
		if _, err := db.Exec(fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s;`, *schema)); err != nil {
			log.Fatalf("failed to create schema: %v", err)
		}
	}

	// Goose migrations should exist within the schema, too
	goose.SetTableName(fmt.Sprintf("%s.schema_migrations", *schema))

	if err := goose.Run(*command, db, *dir, *args...); err != nil {
		app.Fatalf(err.Error())
	}
}
