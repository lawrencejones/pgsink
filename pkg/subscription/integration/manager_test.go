package integration

import (
	"context"
	"time"

	"github.com/lawrencejones/pgsink/pkg/changelog"
	"github.com/lawrencejones/pgsink/pkg/dbtest"
	"github.com/lawrencejones/pgsink/pkg/subscription"

	_ "github.com/jackc/pgx/v4/stdlib"

	. "github.com/onsi/ginkgo"
	_ "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
	_ "github.com/onsi/gomega/gstruct"
)

var _ = Describe("Manager", func() {
	var (
		ctx     context.Context
		cancel  func()
		sub     subscription.Subscription
		opts    *subscription.ManagerOptions
		manager *subscription.Manager
	)

	var (
		schema         = "subscription_manager_integration_test"
		subscriptionID = "uniqueness"
		tableOneName   = "one"
		tableOne       = changelog.Table{Schema: schema, TableName: tableOneName}
		tableTwoName   = "two"
		tableTwo       = changelog.Table{Schema: schema, TableName: tableTwoName}
	)

	db := dbtest.Configure(
		dbtest.WithSchema(schema),
		dbtest.WithPublication(schema),
		dbtest.WithTable(schema, tableOneName, "id bigserial primary key", "message text"),
		dbtest.WithTable(schema, tableTwoName, "id bigserial primary key", "message text"),
	)

	JustBeforeEach(func() {
		manager = subscription.NewManager(logger, db.GetDB(), *opts)
	})

	BeforeEach(func() {
		ctx, cancel = db.Setup(context.Background(), 10*time.Second)

		sub = subscription.Subscription{
			Publication: subscription.Publication{
				Name: schema,
				ID:   subscriptionID,
			},
		}

		opts = &subscription.ManagerOptions{
			Schemas:  []string{schema},
			Excludes: []string{},
			Includes: []string{},
		}
	})

	AfterEach(func() {
		cancel()
	})

	Describe("Reconcile()", func() {
		var (
			added   changelog.Tables
			removed changelog.Tables
			err     error
		)

		JustBeforeEach(func() {
			added, removed, err = manager.Reconcile(ctx, sub)
			Expect(err).NotTo(HaveOccurred(), "reconcile isn't expected to error")
		})

		It("adds watched tables", func() {
			Expect(added).To(ConsistOf(tableOne, tableTwo))
		})

		Context("when tables are already added", func() {
			BeforeEach(func() {
				Expect(sub.SetTables(ctx, db.GetDB(), tableOne, tableTwo)).To(Succeed())
			})

			It("adds and removes nothing", func() {
				Expect(added).To(BeEmpty())
				Expect(removed).To(BeEmpty())
			})

			Context("but they should be ignored", func() {
				BeforeEach(func() {
					opts.Excludes = []string{tableOne.String()}
				})

				It("removes them", func() {
					Expect(removed).To(ConsistOf(tableOne))
				})
			})
		})
	})
})
