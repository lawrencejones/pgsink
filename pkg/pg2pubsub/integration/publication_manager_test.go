package integration

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx"
	"github.com/lawrencejones/pg2pubsub/pkg/pg2pubsub"

	. "github.com/onsi/ginkgo"
	_ "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
	_ "github.com/onsi/gomega/gstruct"
)

var _ = Describe("PublicationManager", func() {
	var (
		ctx    context.Context
		cancel func()
		conn   *pgx.Conn
		pubmgr *pg2pubsub.PublicationManager
		opts   *pg2pubsub.PublicationManagerOptions

		name          = "pubmgr_integration"
		existingTable = "public.pubmgr_integration_test_existing"
		newTable      = "public.pubmgr_integration_test_new"
		ignoredTable  = "public.pubmgr_integration_test_ignored"
	)

	getPublishedTables := func() []string {
		tables, _ := pubmgr.GetPublishedTables(ctx)
		return tables
	}

	mustExec := func(sql string, args []interface{}, message string) {
		_, err := conn.ExecEx(ctx, fmt.Sprintf(sql, args...), nil)
		Expect(err).To(BeNil(), message)
	}

	JustBeforeEach(func() {
		pubmgr = pg2pubsub.NewPublicationManager(logger, mustConnect(), *opts)
	})

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
		conn = mustConnect()

		// If a publication exists from previous test runs, we should tear it down
		conn.ExecEx(ctx, fmt.Sprintf(`drop publication if exists %s`, name), nil)

		// Remove all tables from previous test runs
		for _, table := range []string{existingTable, ignoredTable, newTable} {
			mustExec(`drop table if exists %s;`, []interface{}{table}, "failed to drop pre-existing table")
		}

		// Create the existing table, as this is intended to be here before we ever start our
		// publication manager
		mustExec(`create table %s (id bigserial primary key);`, []interface{}{existingTable}, "failed to create sync table")

		opts = &pg2pubsub.PublicationManagerOptions{
			Name:         name,
			Schemas:      []string{"public"},
			Excludes:     []string{ignoredTable},
			PollInterval: 100 * time.Millisecond,
		}
	})

	AfterEach(func() {
		cancel()
	})

	Describe("Create()", func() {
		var (
			err error
		)

		JustBeforeEach(func() {
			err = pubmgr.Create(context.Background())
		})

		It("creates a publication of the given name", func() {
			Expect(err).To(BeNil(), "failed to create publication")

			var foundName string
			err := conn.QueryRowEx(ctx, `select pubname from pg_publication where pubname = $1;`, nil, name).
				Scan(&foundName)
			Expect(err).To(BeNil(), "failed to find publication")
			Expect(foundName).To(Equal(name))
		})

		Context("when publication already exists", func() {
			BeforeEach(func() {
				mustExec(`create publication %s`, []interface{}{name}, "failed to create publication")
			})

			It("no-ops", func() {
				Expect(err).To(BeNil(), "failed when calling create on existing publication")
			})
		})
	})

	Describe("Sync()", func() {
		var (
			errChan chan error
		)

		JustBeforeEach(func() {
			go func() {
				errChan <- pubmgr.Sync(ctx)
				close(errChan)
			}()
		})

		BeforeEach(func() {
			// Provide a channel that can receive the sync error once it's concluded
			errChan = make(chan error)

			// We need a publication, as that is what Sync() will be managing
			mustExec(`create publication %s;`, []interface{}{name}, "failed to create publication")
		})

		AfterEach(func() {
			cancel()
			Eventually(errChan).Should(BeClosed())
		})

		It("adds tables to an existing publication", func() {
			Eventually(getPublishedTables, 5*time.Second).Should(ContainElement(existingTable))
		})

		Context("when tables should no longer be watched", func() {
			It("removes them from the publication", func() {
				Eventually(getPublishedTables, 5*time.Second).ShouldNot(ContainElement(ignoredTable))
			})
		})

		Context("when new table is created", func() {
			It("adds the table to the publication", func() {
				// Wait for a full sync to add the existing table, so we know we're not just
				// testing start-up logic
				Eventually(getPublishedTables, 5*time.Second).Should(ContainElement(existingTable))

				// Now create a new table
				mustExec(`create table %s (id bigserial primary key);`, []interface{}{newTable}, "failed to create new table")

				// Verify we eventually detect the new table and add it to the publication
				Eventually(getPublishedTables, 5*time.Second).Should(ContainElement(newTable))
			})
		})
	})
})
