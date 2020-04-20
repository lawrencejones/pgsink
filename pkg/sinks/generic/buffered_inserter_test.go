package generic_test

import (
	"context"
	"time"

	"github.com/lawrencejones/pg2sink/pkg/changelog"
	"github.com/lawrencejones/pg2sink/pkg/sinks/generic"

	. "github.com/onsi/ginkgo"
	_ "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gstruct"
)

var _ = Describe("NewBufferedInserter", func() {
	var (
		ctx        context.Context
		bufferSize int
		async      generic.AsyncInserter
		backend    fakeBackend
		cancel     func()

		suite = AsyncInserterSuite{
			New: func(backend fakeBackend) generic.AsyncInserter {
				return generic.NewBufferedInserter(generic.NewAsyncInserter(backend[0]), bufferSize)
			},
			NewBackend: func() fakeBackend {
				return newFakeBackend(&fakeInserter{MemoryInserter: generic.NewMemoryInserter()})
			},
		}
	)

	// Have the suite do our setup, and bind the given variables
	suite.Bind(&ctx, &async, &backend, &cancel)

	// By default, we'll use a buffer size of 2
	BeforeEach(func() { bufferSize = 2 })

	Describe("AsyncInserter interface", func() {
		// Set the buffer size to 1, otherwise we'll timeout waiting on the buffer to overflow
		// in the generic test suite
		BeforeEach(func() { bufferSize = 1 })

		verifyGenericAsyncInserter(suite)
	})

	Describe(".Insert", func() {
		var (
			result        generic.InsertResult
			modifications []*changelog.Modification
		)

		JustBeforeEach(func() {
			result = async.Insert(ctx, modifications)
		})

		BeforeEach(func() {
			modifications = []*changelog.Modification{
				fixtureExample1Insert,
				fixtureExample1UpdateFirst,
				fixtureExample1UpdateSecond,
			}
		})

		It("inserts modifications split by batches", func() {
			_, _, err := async.Flush(ctx).Get(ctx)
			Expect(err).NotTo(HaveOccurred(), "expected flush to succeed, as no insertion should fail")

			count, lsn, err := result.Get(ctx)

			Expect(err).To(BeNil(), "no insertions should fail, so expected nil error")
			Expect(count).To(Equal(3), "inserted 3 modifications, was expecting 3 to be confirmed")
			Expect(lsn).To(PointTo(BeNumerically("==", 3)), "max LSN of insertions was 3")
			Expect(len(backend.Batches())).To(BeEquivalentTo(2), "should perform insert in 2 batches")
		})

		Context("when exactly matching buffer size", func() {
			BeforeEach(func() { bufferSize = len(modifications) })

			// If this test fails, it's probably because our batching logic has broken. The most
			// complex part of the buffered inserter is ensuring the results returned by
			// Insert() follow the AsyncInserter semantics: specifically, that your result only
			// resolves once all the modifications you supply have been inserted.
			//
			// To debug, spew the inserter and check we've attempted to flush our buffer. If we
			// have, but the result still timed out, check the handling of i.bufferResult.
			It("triggers insertion, and fulfils the original result", func() {
				count, _, err := result.Get(ctx)

				Expect(err).To(BeNil(), "no insertions should fail, so expected nil error")
				Expect(count).To(Equal(3), "inserted 3 modifications, was expecting 3 to be confirmed")
				Expect(len(backend.Batches())).To(BeEquivalentTo(1), "should only insert 1 batch")
			})
		})

		Context("when not exceeding buffer size", func() {
			BeforeEach(func() { bufferSize = len(modifications) + 1 })

			// This is a bit dodgy, as there's nothing to say the Insert isn't just too slow for
			// the expectation to catch things. We have to add a time.Sleep() to trigger the
			// asynchronous insertion, just in case.
			It("does not insert modifications", func() {
				time.Sleep(5 * time.Millisecond)
				Expect(backend.Batches()).To(BeEmpty())
			})
		})
	})
})
