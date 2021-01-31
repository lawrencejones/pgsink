package api

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/lawrencejones/pgsink/api/gen/subscriptions"
	apisubscriptions "github.com/lawrencejones/pgsink/api/gen/subscriptions"
	"github.com/lawrencejones/pgsink/internal/dbschema/pgsink/model"
	. "github.com/lawrencejones/pgsink/internal/dbschema/pgsink/table"
	"github.com/lawrencejones/pgsink/internal/middleware"
	"github.com/lawrencejones/pgsink/pkg/changelog"
	"github.com/lawrencejones/pgsink/pkg/subscription"

	pg "github.com/go-jet/jet/v2/postgres"
	kitlog "github.com/go-kit/kit/log"
)

type subscriptionsService struct {
	db  *sql.DB
	pub *subscription.Publication
	sync.Mutex
}

func NewSubscriptions(db *sql.DB, pub *subscription.Publication) apisubscriptions.Service {
	return &subscriptionsService{db: db, pub: pub}
}

func (s *subscriptionsService) Get(ctx context.Context) (*apisubscriptions.Subscription, error) {
	publishedTables, err := s.pub.GetTables(ctx, s.db)
	if err != nil {
		return nil, err
	}

	sub := &apisubscriptions.Subscription{
		ID:              s.pub.ID,
		PublishedTables: make([]*apisubscriptions.SubscriptionPublishedTable, 0, len(publishedTables)),
	}
	for _, table := range publishedTables {
		sub.PublishedTables = append(sub.PublishedTables, &apisubscriptions.SubscriptionPublishedTable{
			Schema: table.Schema,
			Name:   table.TableName,
		})
	}

	return sub, nil
}

func (s *subscriptionsService) AddTable(ctx context.Context, table *subscriptions.SubscriptionPublishedTable) (*subscriptions.Subscription, error) {
	logger := middleware.LoggerFrom(ctx)
	logger = kitlog.With(logger, "table_name", table.Name, "table_schema", table.Schema)

	// Take an application lock to prevent multiple requests from modifying the publication
	// simultaneously. This doesn't solve any races that come from anywhere other than this
	// app, and is best implemented as a database lock in future.
	s.Lock()
	defer s.Unlock()

	logger.Log("msg", "finding already published tables")
	publishedTables, err := s.pub.GetTables(ctx, s.db)
	if err != nil {
		return nil, err
	}

	for _, publishedTable := range publishedTables {
		if publishedTable.Schema == table.Schema && publishedTable.TableName == table.Name {
			logger.Log("msg", "already published")
			return s.Get(ctx)
		}
	}

	logger.Log("msg", "table isn't already published, configuring publication")
	if err := s.pub.SetTables(ctx, s.db, append(publishedTables, changelog.Table{Schema: table.Schema, TableName: table.Name})...); err != nil {
		return nil, err
	}

	return s.Get(ctx)
}

func (s *subscriptionsService) StopTable(ctx context.Context, table *subscriptions.SubscriptionPublishedTable) (*subscriptions.Subscription, error) {
	logger := middleware.LoggerFrom(ctx)
	logger = kitlog.With(logger, "table_name", table.Name, "table_schema", table.Schema)

	// As with AddTable, lock. It's not foolproof, but will do for now.
	s.Lock()
	defer s.Unlock()

	{
		// Expire any active import jobs first, as this might block against any outstanding
		// import. Better to block and fail here than remove from publication, which can't be
		// rolled back, and die.
		logger.Log("msg", "expiring any outstanding import jobs")
		stmt := ImportJobs.
			UPDATE().
			SET(
				ImportJobs.ExpiredAt.SET(pg.TimestampzExp(pg.Raw("now()"))),
				ImportJobs.UpdatedAt.SET(pg.TimestampzExp(pg.Raw("now()"))),
			).
			WHERE(
				ImportJobs.Schema.EQ(pg.String(table.Schema)).AND(
					ImportJobs.TableName.EQ(pg.String(table.Name)),
				).AND(
					ImportJobs.ExpiredAt.IS_NULL(),
				),
			).
			RETURNING(ImportJobs.AllColumns)

		var jobs []model.ImportJobs
		if err := stmt.QueryContext(ctx, s.db, &jobs); err != nil {
			return nil, fmt.Errorf("failed to expire import jobs: %w", err)
		}
		for _, job := range jobs {
			logger.Log("event", "import_expired", "import_job_id", job.ID)
		}
	}

	{
		logger.Log("msg", "finding already published tables")
		publishedTables, err := s.pub.GetTables(ctx, s.db)
		if err != nil {
			return nil, err
		}

		var found bool
		for _, publishedTable := range publishedTables {
			if publishedTable.Schema == table.Schema && publishedTable.TableName == table.Name {
				found = true
			}
		}

		if !found {
			logger.Log("msg", "table isn't in subscription, continuing")
			return s.Get(ctx)
		}

		logger.Log("msg", "table isn't already published, configuring publication")
		publishedWithoutStop := publishedTables.Diff([]changelog.Table{{Schema: table.Schema, TableName: table.Name}})
		if err := s.pub.SetTables(ctx, s.db, publishedWithoutStop...); err != nil {
			return nil, err
		}
	}

	return s.Get(ctx)
}
