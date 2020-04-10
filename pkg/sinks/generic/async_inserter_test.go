package generic_test

import (
	"github.com/lawrencejones/pg2sink/pkg/sinks/generic"

	. "github.com/onsi/ginkgo"
	_ "github.com/onsi/ginkgo/extensions/table"
	_ "github.com/onsi/gomega"
	_ "github.com/onsi/gomega/gstruct"
)

var _ = Describe("asyncInserter", func() {
	var (
		suite = AsyncInserterSuite{
			New: func(backend *fakeInserter) generic.AsyncInserter {
				return generic.WrapAsync(backend)
			},
		}
	)

	Describe("AsyncInserter interface", func() {
		verifyGenericAsyncInserter(suite)
	})
})
