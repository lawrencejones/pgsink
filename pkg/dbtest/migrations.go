package dbtest

import (
	"context"
	"database/sql"
	"time"

	"github.com/lawrencejones/pgsink/pkg/migration"

	kitlog "github.com/go-kit/kit/log"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

// Before running any Ginkgo suites that rely on dbtest, ensure we've migrated the
// database to include our schema.
var _ = BeforeSuite(func() {
	db, err := sql.Open("pgx", "")
	Expect(err).NotTo(HaveOccurred(), "failed to connect to database")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	Expect(
		migration.Migrate(ctx, kitlog.NewLogfmtLogger(GinkgoWriter), db),
	).To(
		Succeed(), "failed to migrate test database",
	)
})
