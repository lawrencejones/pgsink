package integration

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/lawrencejones/pg2sink/pkg/dbschema/model"
	. "github.com/lawrencejones/pg2sink/pkg/dbschema/table"
	"github.com/lawrencejones/pg2sink/pkg/imports"
	"github.com/lawrencejones/pg2sink/pkg/subscription"

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
		db      *sql.DB
		sub     subscription.Subscription
		manager *imports.Manager
		err     error
	)

	var (
		schemaName      = "imports_manager_integration_test"
		publicationName = "imports_manager_integration_test"
		subscriptionID  = "uniqueness"

		tableOneName = fmt.Sprintf("%s.%s", schemaName, "one")
		tableTwoName = fmt.Sprintf("%s.%s", schemaName, "two")
	)

	mustExec := func(sql string, args []interface{}, message ...interface{}) {
		_, err := db.ExecContext(ctx, fmt.Sprintf(sql, args...))
		Expect(err).To(BeNil(), message...)
	}

	JustBeforeEach(func() {
		manager = imports.NewManager(logger, db, imports.ManagerOptions{})
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

		BeforeEach(func() {
			mustExec(fmt.Sprintf(`create table %s (id bigserial primary key);`, tableOneName), nil)
			mustExec(fmt.Sprintf(`create table %s (id bigserial primary key);`, tableTwoName), nil)
		})

		Context("for published table", func() {
			BeforeEach(func() {
				Expect(sub.SetTables(ctx, db, tableOneName)).To(Succeed())
			})

			Context("with no previous import", func() {
				It("creates import job", func() {
					Expect(err).NotTo(HaveOccurred())
					Expect(jobs).To(ContainElement(
						MatchFields(IgnoreExtras, Fields{"TableName": Equal(tableOneName)}),
					))
				})
			})

			Context("with previous import", func() {
				BeforeEach(func() {
					stmt := ImportJobs.
						INSERT(ImportJobs.SubscriptionID, ImportJobs.TableName).
						VALUES(sub.GetID(), tableOneName)

					_, err := stmt.ExecContext(ctx, db)
					Expect(err).NotTo(HaveOccurred())
				})

				It("does not create an additional import", func() {
					Expect(err).NotTo(HaveOccurred())
					Expect(jobs).To(BeEmpty())
				})
			})

			Context("with previous expired import", func() {
				BeforeEach(func() {
					stmt := ImportJobs.
						INSERT(ImportJobs.SubscriptionID, ImportJobs.TableName, ImportJobs.ExpiredAt).
						VALUES(sub.GetID(), tableOneName, Raw("now()"))

					_, err := stmt.ExecContext(ctx, db)
					Expect(err).NotTo(HaveOccurred())
				})

				It("creates import job", func() {
					Expect(err).NotTo(HaveOccurred())
					Expect(jobs).To(ContainElement(
						MatchFields(IgnoreExtras, Fields{"TableName": Equal(tableOneName)}),
					))
				})
			})
		})
	})
})
