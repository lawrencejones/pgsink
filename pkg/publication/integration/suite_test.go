package integration

import (
	"testing"

	kitlog "github.com/go-kit/kit/log"

	// Ensure we migrate the database before running any tests
	_ "github.com/lawrencejones/pg2pubsub/pkg/migration"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var logger = kitlog.NewLogfmtLogger(GinkgoWriter)

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "pkg/publication/integration")
}
