package main

import (
	"context"
	"database/sql"
	"os"

	"github.com/lawrencejones/pgsink/pkg/migration"

	kitlog "github.com/go-kit/kit/log"
	_ "github.com/jackc/pgx/v4/stdlib"
)

func main() {
	if err := run(); err != nil {
		panic(err.Error())
	}
}

func run() error {
	db, err := sql.Open("pgx", "")
	if err != nil {
		return err
	}

	return migration.Migrate(
		context.Background(), kitlog.NewLogfmtLogger(kitlog.NewSyncWriter(os.Stderr)), db)
}
