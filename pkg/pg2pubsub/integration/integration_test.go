package integration

import (
	"os"
	"strconv"

	kitlog "github.com/go-kit/kit/log"
	"github.com/jackc/pgx"

	. "github.com/onsi/ginkgo"
	_ "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
	_ "github.com/onsi/gomega/gstruct"
)

var _ = Describe("pg2pubsub", func() {
	var (
		conn   *pgx.Conn
		logger = kitlog.NewLogfmtLogger(GinkgoWriter)
		err    error

		// We expect a Postgres database to be running for integration tests, and that
		// environment variables are appropriately configured to permit access.
		cfg = pgx.ConnConfig{
			Database: tryEnviron("PGDATABASE", "pg2pubsub_test"),
			Host:     tryEnviron("PGHOST", "127.0.0.1"),
			User:     tryEnviron("PGUSER", "pg2pubsub_test"),
			Password: tryEnviron("PGPASSWORD", ""),
			Port:     uint16(mustAtoi(tryEnviron("PGPORT", "5432"))),
		}
	)

	It("can write to the database", func() {
		conn, err = pgx.Connect(cfg)

		logger.Log("event", "yo")

		Expect(err).NotTo(HaveOccurred(), "failed to connect to postgres")
		Expect(conn.IsAlive()).To(BeTrue())
	})
})

func tryEnviron(key, otherwise string) string {
	if value, found := os.LookupEnv(key); found {
		return value
	}

	return otherwise
}

func mustAtoi(numstr string) int {
	num, err := strconv.Atoi(numstr)
	if err != nil {
		panic(err)
	}

	return num
}
