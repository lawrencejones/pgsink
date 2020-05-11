package integration

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/lawrencejones/pg2sink/pkg/subscription"

	_ "github.com/jackc/pgx/v4/stdlib"

	. "github.com/onsi/ginkgo"
	_ "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
	_ "github.com/onsi/gomega/gstruct"
)

const (
	schemaName      = "subscription_manager_integration_test"
	publicationName = "subscription_manager_integration_test"
	subscriptionID  = "uniqueness"
)

var (
	tableOneName   = fmt.Sprintf("%s.%s", schemaName, "one")
	tableTwoName   = fmt.Sprintf("%s.%s", schemaName, "two")
	tableThreeName = fmt.Sprintf("%s.%s", schemaName, "three")
)

var _ = Describe("Manager", func() {
	var (
		ctx     context.Context
		cancel  func()
		db      *sql.DB
		sub     subscription.Subscription
		opts    *subscription.ManagerOptions
		manager *subscription.Manager
		err     error
	)

	mustExec := func(sql string, args []interface{}, message ...interface{}) {
		_, err := db.ExecContext(ctx, fmt.Sprintf(sql, args...))
		Expect(err).To(BeNil(), message...)
	}

	JustBeforeEach(func() {
		manager = subscription.NewManager(logger, db, *opts)
	})

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)

		db, err = sql.Open("pgx", "")
		Expect(err).NotTo(HaveOccurred(), "failed to connect to database")

		// Remove any artifacts from previous runs
		db.ExecContext(ctx, fmt.Sprintf(`drop schema if exists %s cascade`, schemaName))
		db.ExecContext(ctx, fmt.Sprintf(`drop publication if exists %s`, publicationName))

		// Recreate global state
		db.ExecContext(ctx, fmt.Sprintf(`create schema %s`, schemaName))
		db.ExecContext(ctx, fmt.Sprintf(`create publication %s`, publicationName))

		sub = subscription.Subscription{
			Publication: subscription.Publication{
				ID:   subscriptionID,
				Name: publicationName,
			},
		}

		opts = &subscription.ManagerOptions{
			Schemas:  []string{schemaName},
			Excludes: []string{},
			Includes: []string{},
		}
	})

	AfterEach(func() {
		cancel()
	})

	Describe("Reconcile()", func() {
		var (
			added   []string
			removed []string
			err     error
		)

		JustBeforeEach(func() {
			added, removed, err = manager.Reconcile(ctx, sub)
			Expect(err).NotTo(HaveOccurred(), "reconcile isn't expected to error")
		})

		BeforeEach(func() {
			mustExec(fmt.Sprintf(`create table %s (id bigserial primary key);`, tableOneName), nil)
			mustExec(fmt.Sprintf(`create table %s (id bigserial primary key);`, tableTwoName), nil)
		})

		It("adds watched tables", func() {
			Expect(added).To(ConsistOf(tableOneName, tableTwoName))
		})

		Context("when tables are already added", func() {
			BeforeEach(func() {
				Expect(sub.SetTables(ctx, db, tableOneName, tableTwoName)).To(Succeed())
			})

			It("adds and removes nothing", func() {
				Expect(added).To(BeEmpty())
				Expect(removed).To(BeEmpty())
			})

			Context("but they should be ignored", func() {
				BeforeEach(func() {
					opts.Excludes = []string{tableOneName}
				})

				It("removes them", func() {
					Expect(removed).To(ConsistOf(tableOneName))
				})
			})
		})
	})
})
