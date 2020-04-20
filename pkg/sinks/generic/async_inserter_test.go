package generic_test

import (
	"github.com/lawrencejones/pg2sink/pkg/sinks/generic"

	. "github.com/onsi/ginkgo"
	_ "github.com/onsi/ginkgo/extensions/table"
	_ "github.com/onsi/gomega"
	_ "github.com/onsi/gomega/gstruct"
)

var _ = Describe("NewAsyncInserter", func() {
	var (
		suite = AsyncInserterSuite{
			New: func(backend fakeBackend) generic.AsyncInserter {
				return generic.NewAsyncInserter(backend[0])
			},
			NewBackend: func() fakeBackend {
				return newFakeBackend(&fakeInserter{MemoryInserter: generic.NewMemoryInserter()})
			},
		}
	)

	Describe("AsyncInserter interface", func() {
		verifyGenericAsyncInserter(suite)
	})
})
