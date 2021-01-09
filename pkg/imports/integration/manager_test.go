package integration

import (
	"context"
	"database/sql"
	"time"

	"github.com/lawrencejones/pgsink/pkg/changelog"
	"github.com/lawrencejones/pgsink/internal/dbschema/pgsink/model"
	. "github.com/lawrencejones/pgsink/internal/dbschema/pgsink/table"
	"github.com/lawrencejones/pgsink/internal/dbtest"
	"github.com/lawrencejones/pgsink/pkg/imports"
	"github.com/lawrencejones/pgsink/pkg/subscription"

	. "github.com/go-jet/jet/postgres"
	_ "github.com/jackc/pgx/v4/stdlib"

	. "github.com/onsi/ginkgo"
	_ "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gstruct"
)

var _ = Describe("Manager", func() {
	var (
		ctx     context.Context
		cancel  func()
		sub     subscription.Subscription
		manager *imports.Manager
	)

	var (
		schema         = "imports_manager_integration_test"
		subscriptionID = "uniqueness"
		tableOneName   = "one"
		tableOne       = changelog.Table{Schema: schema, TableName: tableOneName}
		tableTwoName   = "two"
	)

	db := dbtest.Configure(
		dbtest.WithSchema(schema),
		dbtest.WithPublication(schema),
		dbtest.WithTable(schema, tableOneName, "id bigserial primary key", "message text"),
		dbtest.WithTable(schema, tableTwoName, "id bigserial primary key", "message text"),
		dbtest.WithLifecycle(
			nil, // no creation, this table should already be here
			func(ctx context.Context, db, _ *sql.DB) (sql.Result, error) {
				return db.ExecContext(ctx, "truncate pgsink.import_jobs;")
			},
		),
	)

	JustBeforeEach(func() {
		manager = imports.NewManager(logger, db.GetDB(), imports.ManagerOptions{})
	})

	BeforeEach(func() {
		ctx, cancel = db.Setup(context.Background(), 10*time.Second)

		sub = subscription.Subscription{
			Publication: subscription.Publication{
				Name: schema,
				ID:   subscriptionID,
			},
		}
	})

	AfterEach(func() {
		cancel()
	})

	Describe("Reconcile()", func() {
		var (
			jobs []model.ImportJobs
			err  error
		)

		JustBeforeEach(func() {
			jobs, err = manager.Reconcile(ctx, sub)
		})

		Context("for published table", func() {
			BeforeEach(func() {
				Expect(sub.SetTables(ctx, db.GetDB(), tableOne)).To(Succeed())
			})

			Context("with no previous import", func() {
				It("creates import job", func() {
					Expect(err).NotTo(HaveOccurred())
					Expect(jobs).To(ContainElement(
						MatchFields(IgnoreExtras, Fields{
							"Schema": Equal(schema), "TableName": Equal(tableOneName)}),
					))
				})
			})

			Context("with previous import", func() {
				BeforeEach(func() {
					query, args := ImportJobs.
						INSERT(ImportJobs.SubscriptionID, ImportJobs.Schema, ImportJobs.TableName).
						VALUES(sub.GetID(), schema, tableOneName).
						Sql()

					db.MustExec(ctx, query, args...)
				})

				It("does not create an additional import", func() {
					Expect(err).NotTo(HaveOccurred())
					Expect(jobs).To(BeEmpty())
				})
			})

			Context("with previous expired import", func() {
				BeforeEach(func() {
					query, args := ImportJobs.
						INSERT(ImportJobs.SubscriptionID, ImportJobs.Schema, ImportJobs.TableName, ImportJobs.ExpiredAt).
						VALUES(sub.GetID(), schema, tableOneName, Raw("now()")).
						Sql()

					db.MustExec(ctx, query, args...)
				})

				It("creates import job", func() {
					Expect(err).NotTo(HaveOccurred())
					Expect(jobs).To(ContainElement(
						MatchFields(IgnoreExtras, Fields{
							"Schema": Equal(schema), "TableName": Equal(tableOneName)}),
					))
				})
			})
		})
	})
})
