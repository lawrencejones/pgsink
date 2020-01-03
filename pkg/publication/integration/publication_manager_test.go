package integration

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx"
	"github.com/lawrencejones/pg2pubsub/pkg/publication"
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

var _ = Describe("PublicationManager", func() {
	var (
		ctx    context.Context
		cancel func()
		pool   *pgx.ConnPool
		pubmgr *publication.PublicationManager
		opts   *publication.PublicationManagerOptions
		err    error

		name          = "pubmgr_integration"
		existingTable = "public.pubmgr_integration_test_existing"
		newTable      = "public.pubmgr_integration_test_new"
		ignoredTable  = "public.pubmgr_integration_test_ignored"
	)

	getPublishedTables := func() []string {
		tables, _ := publication.GetPublishedTables(ctx, pool, pubmgr.GetPublicationID())
		return tables
	}

	mustExec := func(sql string, args []interface{}, message string) {
		_, err := pool.ExecEx(ctx, fmt.Sprintf(sql, args...), nil)
		Expect(err).To(BeNil(), message)
	}

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)

		cfg, _ := pgx.ParseEnvLibpq()
		pool, err = pgx.NewConnPool(pgx.ConnPoolConfig{ConnConfig: cfg})
		Expect(err).NotTo(HaveOccurred(), "failed to connect to database")

		// If a publication exists from previous test runs, we should tear it down
		pool.ExecEx(ctx, fmt.Sprintf(`drop publication if exists %s`, name), nil)

		// Remove all tables from previous test runs
		for _, table := range []string{existingTable, ignoredTable, newTable} {
			mustExec(`drop table if exists %s;`, []interface{}{table}, "failed to drop pre-existing table")
		}

		// Create tables that are meant to pre-date our sync run
		mustExec(`create table %s (id bigserial primary key);`, []interface{}{existingTable}, "failed to create sync table")
		mustExec(`create table %s (id bigserial primary key);`, []interface{}{ignoredTable}, "failed to create sync table")

		opts = &publication.PublicationManagerOptions{
			Name:         name,
			Schemas:      []string{"public"},
			Excludes:     []string{ignoredTable},
			PollInterval: 100 * time.Millisecond,
		}
	})

	AfterEach(func() {
		cancel()
	})

	Describe("CreatePublicationManager()", func() {
		JustBeforeEach(func() {
			pubmgr, err = publication.CreatePublicationManager(ctx, logger, pool, *opts)
		})

		It("creates a publication of the given name", func() {
			Expect(err).To(BeNil(), "failed to create publication")
			Expect(pubmgr.GetPublicationID()).NotTo(Equal(""), "publication ID should be a uuid")

			var foundName string
			err := pool.QueryRowEx(ctx, `select pubname from pg_publication where pubname = $1;`, nil, name).
				Scan(&foundName)
			Expect(err).To(BeNil(), "failed to find publication")
			Expect(foundName).To(Equal(name))
		})

		Context("when publication already exists", func() {
			BeforeEach(func() {
				mustExec(`create publication %s`, []interface{}{name}, "failed to create publication")
				mustExec(`comment on publication %s is 'la-la-la'`, []interface{}{name}, "failed to comment publication")
			})

			It("no-ops", func() {
				Expect(err).To(BeNil(), "failed when calling create on existing publication")
			})
		})
	})

	Describe("Sync()", func() {
		JustBeforeEach(func() {
			// Call Create() as we need to initialise the manager with an identifier. It's a bit
			// sad we need Create() to work in order to test Sync(), but there is a crucial
			// dependency there.
			pubmgr, err = publication.CreatePublicationManager(ctx, logger, pool, *opts)
			Expect(err).NotTo(HaveOccurred(), "failed to create manager")

			go func() {
				defer GinkgoRecover()
				Expect(pubmgr.Sync(ctx)).To(Succeed(), "Sync() returned an error")
			}()
		})

		AfterEach(func() {
			cancel()
		})

		FIt("adds tables to an existing publication", func() {
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
