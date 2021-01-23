package api

import (
	"context"
	"database/sql"
	"strings"

	pg "github.com/go-jet/jet/v2/postgres"
	"github.com/lawrencejones/pgsink/api/gen/tables"
	isv "github.com/lawrencejones/pgsink/internal/dbschema/information_schema/view"
)

type tablesService struct {
	db *sql.DB
}

func NewTables(db *sql.DB) tables.Service {
	return &tablesService{db}
}

func (s *tablesService) List(ctx context.Context, payload *tables.ListPayload) (tables []*tables.Table, err error) {
	stmt := isv.Tables.
		SELECT(
			isv.Tables.TableSchema.AS("table.schema"),
			isv.Tables.TableName.AS("table.name"),
		).
		WHERE(
			isv.Tables.TableType.EQ(pg.String("BASE TABLE")).AND(
				isv.Tables.TableSchema.IN((func() (expr []pg.Expression) {
					for _, schema := range strings.Split(payload.Schema, ",") {
						expr = append(expr, pg.String(schema))
					}

					return
				})()...),
			),
		)

	if err := stmt.QueryContext(ctx, s.db, &tables); err != nil {
		return nil, err
	}

	return
}
