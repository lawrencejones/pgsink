package generic_test

import (
	"context"

	"github.com/lawrencejones/pgsink/pkg/changelog"
	"github.com/lawrencejones/pgsink/pkg/sinks/generic"

	. "github.com/onsi/ginkgo"
	_ "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gstruct"
)

var _ = Describe("MemoryInserter", func() {
	var (
		ctx      context.Context
		inserter *generic.MemoryInserter
		cancel   func()
	)

	BeforeEach(func() {
		ctx, cancel = context.WithCancel(context.Background())
		inserter = generic.NewMemoryInserter()
	})

	AfterEach(func() {
		cancel()
	})

	Describe(".Insert", func() {
		It("adds successive modifications to the in-memory store", func() {
			count, lsn, err := inserter.
				Insert(ctx, []*changelog.Modification{fixtureExample1Insert, fixtureExample1UpdateFirst})

			Expect(count).To(Equal(2))
			Expect(lsn).To(PointTo(BeNumerically("==", 2)))
			Expect(err).To(BeNil())

			count, lsn, err = inserter.Insert(ctx, []*changelog.Modification{fixtureExample1UpdateSecond})

			Expect(count).To(Equal(1))
			Expect(lsn).To(PointTo(BeNumerically("==", 3)))
			Expect(err).To(BeNil())

			Expect(inserter.Store()).To(ConsistOf(
				fixtureExample1Insert,
				fixtureExample1UpdateFirst,
				fixtureExample1UpdateSecond,
			))
		})
	})
})
