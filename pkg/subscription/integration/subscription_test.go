package integration

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/lawrencejones/pg2sink/pkg/changelog"
	. "github.com/lawrencejones/pg2sink/pkg/changelog/matchers"
	"github.com/lawrencejones/pg2sink/pkg/subscription"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/stdlib"

	. "github.com/onsi/ginkgo"
	_ "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gstruct"
)

var _ = Describe("Subscription", func() {
	var (
		ctx     context.Context
		cancel  func()
		db      *sql.DB
		repconn *pgx.Conn
		err     error
	)

	var (
		schemaName      = "subscription_integration_test"
		publicationName = "subscription_integration_test"

		tableOneName   = fmt.Sprintf("%s.%s", schemaName, "one")
		tableOneSchema = `
(
	id bigserial primary key,
	number int,
	message text,
	truthy boolean
)
`
		tableTwoName   = fmt.Sprintf("%s.%s", schemaName, "two")
		tableTwoSchema = `
(
	id bigserial primary key,
	message text
)
`
	)

	mustExec := func(sql string, args []interface{}, message ...interface{}) {
		_, err := db.ExecContext(ctx, fmt.Sprintf(sql, args...))
		Expect(err).To(BeNil(), message...)
	}

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)

		db, err = sql.Open("pgx", "")
		Expect(err).NotTo(HaveOccurred(), "failed to connect to database")

		repconn, err = pgx.Connect(ctx, "replication=database")
		Expect(err).NotTo(HaveOccurred(), "failed to create replication connection")

		// Remove any artifacts from previous runs
		db.ExecContext(ctx, fmt.Sprintf(`drop schema if exists %s cascade`, schemaName))
		db.ExecContext(ctx, fmt.Sprintf(`drop publication if exists %s`, publicationName))
		db.ExecContext(ctx, fmt.Sprintf(`
		select pg_drop_replication_slot(slot_name)
		from pg_replication_slots
		where slot_name like '%s%%';
		`, publicationName))

		// Recreate global state
		mustExec(`create schema %s`, []interface{}{schemaName})
		mustExec(`create table %s %s`, []interface{}{tableOneName, tableOneSchema}, tableOneName)
		mustExec(`create table %s %s`, []interface{}{tableTwoName, tableTwoSchema}, tableTwoName)
	})

	AfterEach(func() {
		cancel()
	})

	createSubscription := func(opts subscription.SubscriptionOptions) (*subscription.Subscription, *pgx.Conn) {
		repconn, err = pgx.Connect(ctx, "replication=database")
		Expect(err).NotTo(HaveOccurred(), "failed to create replication connection")

		sub, err := subscription.Create(ctx, logger, db, repconn, opts)
		Expect(err).NotTo(HaveOccurred())

		return sub, repconn
	}

	Describe("Create()", func() {
		var (
			sub  *subscription.Subscription
			opts *subscription.SubscriptionOptions
		)

		JustBeforeEach(func() {
			sub, repconn = createSubscription(*opts)
		})

		BeforeEach(func() {
			opts = &subscription.SubscriptionOptions{
				Name: publicationName,
			}
		})

		It("creates publication", func() {
			query := `select pubname from pg_publication where pubname = $1`

			var match string
			err := db.QueryRowContext(ctx, query, publicationName).Scan(&match)

			Expect(err).NotTo(HaveOccurred())
			Expect(match).To(Equal(publicationName))
		})

		It("creates replication slot", func() {
			query := `select slot_name from pg_replication_slots where slot_name like $1`

			var match string
			err := db.QueryRowContext(ctx, query, fmt.Sprintf("%s%%", publicationName)).Scan(&match)

			Expect(err).NotTo(HaveOccurred())
			Expect(match).To(Equal(sub.ReplicationSlot.Name))
		})
	})

	// These tests will verify the basic behaviour of a subscription, but assume the logical
	// decoding for each supported field type is well tested elsewhere.
	Describe("Start()", func() {
		var (
			sub     *subscription.Subscription
			stream  *subscription.Stream
			entries changelog.Changelog
		)

		createStream := func(sub *subscription.Subscription, conn *pgx.Conn) (*subscription.Stream, changelog.Changelog) {
			stream, err = sub.Start(ctx, logger, conn,
				subscription.StreamOptions{HeartbeatInterval: 500 * time.Millisecond})
			Expect(err).NotTo(HaveOccurred())

			return stream, subscription.BuildChangelog(logger, stream)
		}

		BeforeEach(func() {
			sub, repconn = createSubscription(subscription.SubscriptionOptions{Name: publicationName})
			stream, entries = createStream(sub, repconn)

			err = sub.SetTables(ctx, db, tableOneName)
			Expect(err).NotTo(HaveOccurred(), "adding table one to publication should succeed")

			// We need to perform an insert to trigger changes down the replication link.
			// Without this, we'll receive no schema or modifications.
			insertQuery := `insert into %s (number, message, truthy) values (1, 'hello', true);`
			mustExec(insertQuery, []interface{}{tableOneName}, "inserting to table one should succeed")
		})

		It("receives schema for published tables", func() {
			Eventually(entries).Should(Receive(ChangelogMatcher(
				SchemaMatcher(tableOneName).
					WithFields(
						changelog.SchemaField{
							Name: "id",
							Type: []string{"null", "long"},
							Key:  true,
						},
						changelog.SchemaField{
							Name: "number",
							Type: []string{"null", "int"},
						},
						changelog.SchemaField{
							Name: "message",
							Type: []string{"null", "string"},
						},
						changelog.SchemaField{
							Name: "truthy",
							Type: []string{"null", "boolean"},
						},
					),
			)))
		})

		It("receives changes to published tables down the channel", func() {
			Eventually(entries).Should(Receive(ChangelogMatcher(
				ModificationMatcher(tableOneName).
					WithBefore(BeNil()).
					WithAfter(
						MatchAllKeys(Keys{
							"id":      BeAssignableToTypeOf(int64(0)),
							"message": Equal("hello"),
							"number":  BeEquivalentTo(1),
							"truthy":  BeTrue(),
						}),
					),
			)))
		})

		Context("with inserts to unwatched tables", func() {
			BeforeEach(func() {
				insertQuery := `insert into %s (message) values ('world');`
				mustExec(insertQuery, []interface{}{tableTwoName}, "inserting to table two should succeed")
			})

			It("does not receive entries for non-published tables", func() {
				Consistently(entries).ShouldNot(Receive(
					SatisfyAny(
						SchemaMatcher(tableTwoName),
						ModificationMatcher(tableTwoName),
					),
				))
			})
		})

		Context("having confirmed changes", func() {
			BeforeEach(func() {
				for {
					select {
					case <-ctx.Done():
						panic("timed out")
					case entry := <-entries:
						if entry.Modification == nil {
							continue
						}

						if match, _ := ModificationMatcher(tableOneName).Match(*entry.Modification); match {
							// Wait until the stream tells us we've sent a confirmed heartbeat to the
							// upstream server
							Eventually(stream.Confirm(pglogrepl.LSN(*entry.Modification.LSN))).Should(Receive(
								BeNumerically(">=", *entry.Modification.LSN),
							))

							return
						}
					}
				}
			})

			getCurrentWalLSN := func() pglogrepl.LSN {
				var lsnText string
				err := db.QueryRowContext(ctx, "select pg_current_wal_lsn()").Scan(&lsnText)
				Expect(err).NotTo(HaveOccurred())

				lsn, err := pglogrepl.ParseLSN(lsnText)
				Expect(err).NotTo(HaveOccurred())

				return lsn
			}

			It("does not receive the same changes again", func() {
				// We use the confirmed_flush_position to restart replication. When we heartbeat,
				// we update this value in the upstream server. When restarting replication, the
				// server will use the closest WAL location that is below this confirmed flush
				// position to begin sending changes.
				//
				// If nothing happened since the original insert, Postgres might pick a value that
				// is immediately before. This would result in resending that row, suggesting we
				// don't respect the confirmed flush position, when in fact we do but Postgres
				// chose to resend that tiny little bit beforehand.
				//
				// To avoid this, generate a chunk of new information in an unpublished table then
				// confirm receipt up-to the new LSN. If we respect the confirmed flush position,
				// we expect Postgres won't send us the original insert.
				By("make many changes to advance the wal position")
				mustExec("insert into %s (message) (select uuid_generate_v4() from generate_series(0, 1000, 1))", []interface{}{
					tableTwoName,
				})

				By("wait for a confirmed heartbeat")
				Eventually(stream.Confirm(getCurrentWalLSN())).Should(Receive())

				By("cancel stream, so we can restart it")
				Expect(stream.Shutdown(ctx)).To(Succeed(), "failed to wait for stream")
				Expect(repconn.Close(ctx)).To(Succeed(), "failed to close original stream connection")

				By("make new published changes")
				insertQuery := `insert into %s (message) values ('this should be streamed');`
				mustExec(insertQuery, []interface{}{tableOneName}, "inserting to table one should succeed")

				By("recreate stream")
				sub, repconn = createSubscription(subscription.SubscriptionOptions{Name: publicationName})
				stream, entries = createStream(sub, repconn)

				By("check only the new change arrives")
				for entry := range entries {
					modification, ok := entry.Unwrap().(*changelog.Modification)
					if !ok {
						continue
					}

					// We should never receive this, as we confirmed we had flushed it to the
					// upstream Postgres in the previous subscription stream.
					Expect(*modification).NotTo(
						ModificationMatcher(tableOneName).WithAfter(
							MatchKeys(IgnoreExtras, Keys{"message": Equal("hello")}),
						),
					)

					// This is the entry we're after, as it was created after the first stream was
					// shutdown.
					matcher := ModificationMatcher(tableOneName).WithAfter(
						MatchKeys(IgnoreExtras, Keys{"message": Equal("this should be streamed")}),
					)
					if match, _ := matcher.Match(*modification); match {
						return // we received the correct message
					}
				}

				Fail("never received second insert")
			})
		})

		Context("with on-going transaction into table which we add to publication", func() {
			// This confirms that in-flight transaction data won't be included in a
			// subscription. There is an open issue (referenced) for this, with a proposed
			// solution to record the highest transaction ID at point of creating an import job,
			// then stalling the import until all transactions lower than this ID have
			// completed.
			//
			// This is similar to the process of concurrent index creation in Postgres, which
			// takes locks against the old transactions before initiating the existing index
			// data sweep.
			//
			// https://github.com/lawrencejones/pg2sink/issues/2
			It("does not receive then committed changes", func() {
				// Use a pgx connection for this, as the way it handles transactions is more
				// friendly to timeouts
				conn, err := stdlib.AcquireConn(db)
				Expect(err).NotTo(HaveOccurred())

				tx, err := conn.Begin(ctx)
				Expect(err).NotTo(HaveOccurred())

				defer tx.Rollback(context.Background())

				_, err = tx.Exec(ctx, fmt.Sprintf("insert into %s (message) values ('on-going transaction');", tableTwoName))
				Expect(err).NotTo(HaveOccurred())

				By("add table two to the subscription, alongside table one")
				err = sub.SetTables(ctx, db, tableOneName, tableTwoName)
				Expect(err).NotTo(HaveOccurred())

				By("commit insert into table two")
				err = tx.Commit(ctx)
				Expect(err).NotTo(HaveOccurred())

				// This may seem weird, as we would prefer to receive our data. This test is about
				// confirming a limitation in Postgres that affects our durability guarantees. We
				// test it to confirm those implications and because we'll have to work around it,
				// not because it's desirable.
				Consistently(entries).ShouldNot(Receive(ChangelogMatcher(
					ModificationMatcher(tableTwoName).WithAfter(
						MatchKeys(IgnoreExtras, Keys{"message": Equal("on-going transaction")}),
					),
				)))
			})
		})
	})
})
