package api

import (
	"context"
	"database/sql"
	"strings"

	pg "github.com/go-jet/jet/v2/postgres"
	"github.com/lawrencejones/pgsink/api/gen/tables"
	isvmodel "github.com/lawrencejones/pgsink/internal/dbschema/information_schema/model"
	isv "github.com/lawrencejones/pgsink/internal/dbschema/information_schema/view"
	"github.com/lawrencejones/pgsink/internal/dbschema/pgsink/model"
	. "github.com/lawrencejones/pgsink/internal/dbschema/pgsink/table"
	"github.com/lawrencejones/pgsink/pkg/subscription"
)

type tablesService struct {
	db  *sql.DB
	pub *subscription.Publication
}

func NewTables(db *sql.DB, pub *subscription.Publication) tables.Service {
	return &tablesService{db, pub}
}

func (s *tablesService) List(ctx context.Context, payload *tables.ListPayload) ([]*tables.Table, error) {
	// Join information_schema.tables onto imports, so we can compute the import_status.
	// LEFT JOIN because we want all tables, even if we haven't imported them.
	stmt := isv.Tables.
		LEFT_JOIN(ImportJobs, isv.Tables.TableSchema.EQ(ImportJobs.Schema).
			AND(isv.Tables.TableName.EQ(ImportJobs.TableName))).
		SELECT(
			isv.Tables.TableSchema,
			isv.Tables.TableName,
			ImportJobs.AllColumns,
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

	var tableWithImports []struct {
		*isvmodel.Tables
		ImportJobs []struct {
			*model.ImportJobs
		}
	}
	if err := stmt.QueryContext(ctx, s.db, &tableWithImports); err != nil {
		return nil, err
	}

	// We can calculate the publication status in the SQL query, but it's easier to reuse
	// the publication logic for now.
	publishedTables, err := s.pub.GetTables(ctx, s.db)
	if err != nil {
		return nil, err
	}

	var results []*tables.Table
	for _, row := range tableWithImports {
		table := &tables.Table{
			Schema:            *row.TableSchema,
			Name:              *row.TableName,
			PublicationStatus: "inactive",
			ImportStatus:      "inactive",
		}

		for _, publishedTable := range publishedTables {
			if table.Name == publishedTable.TableName && table.Schema == publishedTable.Schema {
				table.PublicationStatus = "active"
			}
		}

		var lastImportJob *model.ImportJobs
		for _, job := range row.ImportJobs {
			if lastImportJob == nil {
				lastImportJob = job.ImportJobs
			}
			if lastImportJob.CreatedAt.Before(job.CreatedAt) {
				lastImportJob = job.ImportJobs
			}
		}

		if lastImportJob != nil {
			if lastImportJob.ExpiredAt != nil {
				table.ImportStatus = "expired"
			} else if lastImportJob.CompletedAt != nil {
				table.ImportStatus = "complete"
			} else if lastImportJob.Error != nil {
				table.ImportStatus = "error"
			} else if lastImportJob.Cursor == nil {
				table.ImportStatus = "scheduled"
			} else {
				table.ImportStatus = "unknown" // we shouldn't get here
			}
		}

		results = append(results, table)
	}

	return results, nil
}
