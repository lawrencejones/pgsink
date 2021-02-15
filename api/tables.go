package api

import (
	"context"
	"database/sql"
	"fmt"
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
	tablesWithImports, err := s.getTablesWithImports(ctx, strings.Split(payload.Schema, ","))
	if err != nil {
		return nil, err
	}

	// We can calculate the publication status in the SQL query, but it's easier to reuse
	// the publication logic for now.
	publishedTables, err := s.pub.GetTables(ctx, s.db)
	if err != nil {
		return nil, err
	}

	var results []*tables.Table
	for _, row := range tablesWithImports {
		table := &tables.Table{
			Schema:            *row.Table.TableSchema,
			Name:              *row.Table.TableName,
			PublicationStatus: "inactive",
			ImportStatus:      "inactive",
		}

		// Publication is active if we can find it in our published tables
		for _, publishedTable := range publishedTables {
			if table.Name == publishedTable.TableName && table.Schema == publishedTable.Schema {
				table.PublicationStatus = "active"
			}
		}

		// Find the most recent import job for this table
		var lastImportJob *model.ImportJobs
		for _, job := range row.Imports {
			if lastImportJob == nil {
				lastImportJob = job
			}
			if lastImportJob.CreatedAt.Before(job.CreatedAt) {
				lastImportJob = job
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
			} else if lastImportJob.Cursor != nil {
				table.ImportStatus = "in_progress"
			} else {
				table.ImportStatus = "unknown" // we shouldn't get here
			}
		}

		results = append(results, table)
	}

	return results, nil
}

type tableWithImports struct {
	Table   *isvmodel.Tables
	Imports []*model.ImportJobs
}

func (s *tablesService) getTablesWithImports(ctx context.Context, schemas []string) (tablesWithImports []*tableWithImports, err error) {
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
					for _, schema := range schemas {
						expr = append(expr, pg.String(schema))
					}

					return
				})()...),
			),
		)

	// Jet can group by the primary key of a table, but the information scheme views don't
	// have any primary key. This forces us to pull the rows individually, then group them
	// ourselves.
	var rows []struct {
		*isvmodel.Tables
		ImportJobs []struct {
			*model.ImportJobs
		}
	}
	if err := stmt.QueryContext(ctx, s.db, &rows); err != nil {
		return nil, err
	}

	// Here is where we group by the 'primary key' of tables, which is <schema>.<table>
	aggregate := map[string]*tableWithImports{}
	for _, row := range rows {
		key := fmt.Sprintf("%s.%s", *row.Tables.TableSchema, *row.Tables.TableName)
		result, ok := aggregate[key]
		if !ok {
			result = &tableWithImports{
				Table:   row.Tables,
				Imports: []*model.ImportJobs{},
			}
		}

		if len(row.ImportJobs) > 0 {
			result.Imports = append(result.Imports, row.ImportJobs[0].ImportJobs)
		}

		aggregate[key] = result
	}

	for _, tableWithImports := range aggregate {
		tablesWithImports = append(tablesWithImports, tableWithImports)
	}

	return
}
