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

	err := s.pub.Begin(ctx, s.db, func(session subscription.PublicationSession) error {
		logger.Log("msg", "finding already published tables")
		publishedTables, err := session.GetTables(ctx, s.db)
		if err != nil {
			return err
		}

		for _, publishedTable := range publishedTables {
			if publishedTable.Schema == table.Schema && publishedTable.TableName == table.Name {
				logger.Log("msg", "already published")
				return nil
			}
		}

		logger.Log("msg", "table isn't already published, configuring publication")
		alreadyPublishedWithAdded := append(publishedTables,
			changelog.Table{Schema: table.Schema, TableName: table.Name})
		return session.SetTables(ctx, s.db, alreadyPublishedWithAdded...)
	})
	if err != nil {
		return nil, err
	}

	logger.Log("msg", "added table, reloading subscription")
	return s.Get(ctx)
}

func (s *subscriptionsService) StopTable(ctx context.Context, table *subscriptions.SubscriptionPublishedTable) (*subscriptions.Subscription, error) {
	logger := middleware.LoggerFrom(ctx)
	logger = kitlog.With(logger, "table_name", table.Name, "table_schema", table.Schema)

	err := s.pub.Begin(ctx, s.db, func(session subscription.PublicationSession) error {
		// Expire any active import jobs first, as this might block against any outstanding
		// import. Better to block and fail here than remove from publication, which can't be
		// rolled back, and die.
		logger.Log("msg", "expiring any outstanding import jobs")
		expiredJobs, err := s.expireOutstandingImports(ctx, table)
		if err != nil {
			return err
		}
		for _, job := range expiredJobs {
			logger.Log("event", "import_expired", "import_job_id", job.ID)
		}

		logger.Log("msg", "finding already published tables")
		publishedTables, err := session.GetTables(ctx, s.db)
		if err != nil {
			return err
		}

		var found bool
		for _, publishedTable := range publishedTables {
			if publishedTable.Schema == table.Schema && publishedTable.TableName == table.Name {
				found = true
			}
		}

		if !found {
			logger.Log("msg", "table isn't in subscription, continuing")
			return nil
		}

		logger.Log("msg", "table is published, removing it from publication")
		publishedWithoutStop := publishedTables.Diff([]changelog.Table{{Schema: table.Schema, TableName: table.Name}})
		return session.SetTables(ctx, s.db, publishedWithoutStop...)
	})
	if err != nil {
		return nil, err
	}

	return s.Get(ctx)
}

func (s *subscriptionsService) expireOutstandingImports(ctx context.Context, table *subscriptions.SubscriptionPublishedTable) ([]*model.ImportJobs, error) {
	// Expire any active import jobs first, as this might block against any outstanding
	// import. Better to block and fail here than remove from publication, which can't be
	// rolled back, and die.
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

	var jobs []*model.ImportJobs
	if err := stmt.QueryContext(ctx, s.db, &jobs); err != nil {
		return nil, fmt.Errorf("failed to expire import jobs: %w", err)
	}

	return jobs, nil
}
