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
		pool   *pgx.ConnPool
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

	getImportedTables := func() []string {
		tables, _ := pg2pubsub.ImportJobStore{pool}.GetImportedTables(ctx, pubmgr.GetIdentifier())
		return tables
	}

	mustExec := func(sql string, args []interface{}, message string) {
		_, err := pool.ExecEx(ctx, fmt.Sprintf(sql, args...), nil)
		Expect(err).To(BeNil(), message)
	}

	JustBeforeEach(func() {
		pubmgr = pg2pubsub.NewPublicationManager(logger, mustConnect(), *opts)
	})

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
		pool = mustConnect()

		// If a publication exists from previous test runs, we should tear it down
		pool.ExecEx(ctx, fmt.Sprintf(`drop publication if exists %s`, name), nil)
		pool.ExecEx(ctx, `truncate pg2pubsub.import_jobs;`, nil)

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
			identifier string
			err        error
		)

		JustBeforeEach(func() {
			identifier, err = pubmgr.Create(context.Background())
		})

		It("creates a publication of the given name", func() {
			Expect(err).To(BeNil(), "failed to create publication")
			Expect(identifier).NotTo(Equal(""), "identifier should be a uuid")

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
		var (
			errChan chan error
		)

		JustBeforeEach(func() {
			// Call Create() as we need to initialise the manager with an identifier. It's a bit
			// sad we need Create() to work in order to test Sync(), but there is a crucial
			// dependency there.
			_, err := pubmgr.Create(ctx)
			Expect(err).To(Succeed())

			go func() {
				errChan <- pubmgr.Sync(ctx)
				close(errChan)
			}()
		})

		BeforeEach(func() {
			// Provide a channel that can receive the sync error once it's concluded
			errChan = make(chan error)
		})

		AfterEach(func() {
			cancel()
			Eventually(errChan).Should(BeClosed())
		})

		It("adds tables to an existing publication", func() {
			Eventually(getPublishedTables, 5*time.Second).Should(ContainElement(existingTable))
		})

		It("enqueues a new import job for this table", func() {
			Eventually(getImportedTables, 5*time.Second).Should(ContainElement(existingTable))
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
