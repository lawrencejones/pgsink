package integration

import (
	"testing"

	kitlog "github.com/go-kit/kit/log"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
)

var (
	logger = kitlog.NewLogfmtLogger(GinkgoWriter)
	binary string

	_ = BeforeSuite(func() {
		var err error
		binary, err = gexec.Build("github.com/lawrencejones/pgsink/cmd/pgsink")
		Expect(err).NotTo(HaveOccurred(), "failed to compile pgsink")
	})
	_ = AfterSuite(func() {
		gexec.CleanupBuildArtifacts()
	})
)

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "integration")
}
