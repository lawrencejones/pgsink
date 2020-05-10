package integration

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/lawrencejones/pg2sink/pkg/subscription"

	"github.com/jackc/pgx/v4/pgxpool"
	uuid "github.com/satori/go.uuid"

	. "github.com/onsi/ginkgo"
	_ "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
	_ "github.com/onsi/gomega/gstruct"
)

// randomSuffix provides the first component of a uuid to help construct test-local random
// identifiers. An example result is "9ed17482".
func randomSuffix() string {
	return strings.SplitN(uuid.NewV4().String(), "-", 2)[0]
}

var _ = Describe("Manager", func() {
	var (
		ctx     context.Context
		cancel  func()
		pool    *pgxpool.Pool
		manager *subscription.Manager
		opts    *subscription.ManagerOptions
		err     error

		name          = "manager_integration"
		existingTable = "public.pubmgr_integration_test_existing"
		newTable      = "public.pubmgr_integration_test_new"
		ignoredTable  = "public.pubmgr_integration_test_ignored"
	)

	getPublishedTables := func() []string {
		tables, err := subscription.Publication{Name: name}.GetTables(ctx, pool)
		Expect(err).NotTo(HaveOccurred(), "failed to find published tables")

		return tables
	}

	mustExec := func(sql string, args []interface{}, message string) {
		_, err := pool.Exec(ctx, fmt.Sprintf(sql, args...))
		Expect(err).To(BeNil(), message)
	}

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)

		pool, err = pgxpool.Connect(ctx, "")
		Expect(err).NotTo(HaveOccurred(), "failed to connect to database")

		// If a publication exists from previous test runs, we should tear it down
		pool.Exec(ctx, fmt.Sprintf(`drop publication if exists %s`, name))

		// Remove all tables from previous test runs
		for _, table := range []string{existingTable, ignoredTable, newTable} {
			mustExec(`drop table if exists %s;`, []interface{}{table}, "failed to drop pre-existing table")
		}

		// Create tables that are meant to pre-date our sync run
		mustExec(`create table %s (id bigserial primary key);`, []interface{}{existingTable}, "failed to create sync table")
		mustExec(`create table %s (id bigserial primary key);`, []interface{}{ignoredTable}, "failed to create sync table")

		// Finally, create the publication. This is usually handled by the subscription, and
		// we won't bother commenting a publication ID, as it's not relevant for these tests.
		mustExec(`create publication %s;`, []interface{}{name}, "failed to create publication")

		opts = &subscription.ManagerOptions{
			Schemas:      []string{"public"},
			Excludes:     []string{ignoredTable},
			Includes:     []string{},
			PollInterval: 100 * time.Millisecond,
		}
	})

	AfterEach(func() {
		cancel()
	})

	Describe("Manage()", func() {
		JustBeforeEach(func() {
			manager = subscription.NewManager(logger, pool, *opts)

			go func() {
				defer GinkgoRecover()

				Expect(
					manager.Manage(ctx, subscription.Publication{Name: name}),
				).To(Succeed(), "Manage() returned an error")
			}()
		})

		It("adds tables to an existing publication", func() {
			Eventually(getPublishedTables, 5*time.Second).Should(ContainElement(existingTable))
		})

		Context("when tables should no longer be watched", func() {
			JustBeforeEach(func() {
				mustExec(`alter publication %s set table %s;`, []interface{}{name, ignoredTable}, "failed to add ignored table")
			})

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
