package integration

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"

	kitlog "github.com/go-kit/kit/log"
	"github.com/jackc/pgx"
	"github.com/lawrencejones/pg2pubsub/pkg/migration"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	uuid "github.com/satori/go.uuid"
)

var (
	logger = kitlog.NewLogfmtLogger(GinkgoWriter)

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

func mustConnect() *pgx.ConnPool {
	pool, err := pgx.NewConnPool(pgx.ConnPoolConfig{ConnConfig: cfg})
	Expect(err).NotTo(HaveOccurred(), "failed to connect to postgres")

	return pool
}

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

// randomSuffix provides the first component of a uuid to help construct test-local random
// identifiers. An example result is "9ed17482".
func randomSuffix() string {
	return strings.SplitN(uuid.NewV4().String(), "-", 2)[0]
}

var _ = BeforeSuite(func() {
	Expect(migration.Migrate(context.Background(), logger, cfg)).To(Succeed())
})

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "pkg/pg2pubsub/integration")
}
