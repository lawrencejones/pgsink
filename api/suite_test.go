package api_test

import (
	"testing"

	kitlog "github.com/go-kit/kit/log"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var logger = kitlog.NewLogfmtLogger(GinkgoWriter)

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "api")
}
