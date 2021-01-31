package api

import (
	"context"
	"database/sql"

	pg "github.com/go-jet/jet/v2/postgres"
	apiimports "github.com/lawrencejones/pgsink/api/gen/imports"
	"github.com/lawrencejones/pgsink/internal/dbschema/pgsink/model"
	. "github.com/lawrencejones/pgsink/internal/dbschema/pgsink/table"
)

type importsService struct {
	db             *sql.DB
	subscriptionID string
}

func NewImports(db *sql.DB, subscriptionID string) apiimports.Service {
	return &importsService{db, subscriptionID}
}

func (s *importsService) List(ctx context.Context) (imports []*apiimports.Import, err error) {
	stmt := ImportJobs.
		SELECT(ImportJobs.AllColumns).
		WHERE(
			ImportJobs.SubscriptionID.EQ(pg.String(s.subscriptionID)),
		)

	var rows []model.ImportJobs
	if err := stmt.QueryContext(ctx, s.db, &rows); err != nil {
		return nil, err
	}

	for _, row := range rows {
		imports = append(imports, s.serialize(row))
	}

	return imports, nil
}

func (s *importsService) serialize(job model.ImportJobs) *apiimports.Import {
	return &apiimports.Import{
		ID:             int(job.ID),
		SubscriptionID: job.SubscriptionID,
		Schema:         job.Schema,
		TableName:      job.TableName,
		CompletedAt:    FormatDateTimePointer(job.CompletedAt),
		CreatedAt:      FormatDateTime(job.CreatedAt),
		UpdatedAt:      FormatDateTime(job.UpdatedAt),
		ExpiredAt:      FormatDateTimePointer(job.ExpiredAt),
		Error:          job.Error,
		ErrorCount:     int(job.ErrorCount),
		LastErrorAt:    FormatDateTimePointer(job.LastErrorAt),
	}
}
