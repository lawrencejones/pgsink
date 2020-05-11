package integration

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/lawrencejones/pg2sink/pkg/subscription"

	"github.com/jackc/pgx/v4"
	_ "github.com/jackc/pgx/v4/stdlib"

	. "github.com/onsi/ginkgo"
	_ "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
	_ "github.com/onsi/gomega/gstruct"
)

var _ = Describe("Subscription", func() {
	var (
		ctx     context.Context
		cancel  func()
		db      *sql.DB
		repconn *pgx.Conn
		sub     *subscription.Subscription
		opts    *subscription.SubscriptionOptions
		err     error
	)

	var (
		schemaName      = "subscription_integration_test"
		publicationName = "subscription_integration_test"

		// tableOneName = fmt.Sprintf("%s.%s", schemaName, "one")
		// tableTwoName = fmt.Sprintf("%s.%s", schemaName, "two")
	)

	// mustExec := func(sql string, args []interface{}, message ...interface{}) {
	// 	_, err := db.ExecContext(ctx, fmt.Sprintf(sql, args...))
	// 	Expect(err).To(BeNil(), message...)
	// }

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
		db.ExecContext(ctx, fmt.Sprintf(`create schema %s`, schemaName))

		opts = &subscription.SubscriptionOptions{
			Name: publicationName,
		}
	})

	AfterEach(func() {
		cancel()
	})

	Describe("Create()", func() {
		JustBeforeEach(func() {
			sub, err = subscription.Create(ctx, logger, db, repconn, *opts)
			Expect(err).NotTo(HaveOccurred())
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
})
