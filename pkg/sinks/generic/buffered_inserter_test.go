package generic_test

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/lawrencejones/pg2sink/pkg/changelog"
	"github.com/lawrencejones/pg2sink/pkg/sinks/generic"

	. "github.com/onsi/ginkgo"
	_ "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gstruct"
)

var _ = Describe("bufferedInserter", func() {
	var (
		ctx        context.Context
		bufferSize int
		buffered   generic.AsyncInserter
		inserter   *fakeInserter
		cancel     func()
	)

	JustBeforeEach(func() {
		buffered = generic.WithBuffer(generic.WrapAsync(inserter), bufferSize)
	})

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), time.Second)
		inserter = &fakeInserter{MemoryInserter: generic.NewMemoryInserter()}
		bufferSize = 2
	})

	AfterEach(func() {
		cancel()
	})

	Describe(".Insert", func() {
		var (
			result        generic.InsertResult
			modifications []*changelog.Modification
			insertCount   uint32
		)

		JustBeforeEach(func() {
			result = buffered.Insert(ctx, modifications)
		})

		BeforeEach(func() {
			insertCount = 0
			inserter.BeforeFunc = func(context.Context, []*changelog.Modification) {
				atomic.AddUint32(&insertCount, 1)
			}

			modifications = []*changelog.Modification{
				fixtureExample1Insert,
				fixtureExample1UpdateFirst,
				fixtureExample1UpdateSecond,
			}
		})

		It("inserts modifications split by batches", func() {
			_, _, err := buffered.Flush(ctx).Get(ctx)
			Expect(err).NotTo(HaveOccurred(), "expected flush to succeed, as no insertion should fail")

			count, lsn, err := result.Get(ctx)

			Expect(err).To(BeNil(), "no insertions should fail, so expected nil error")
			Expect(count).To(Equal(3), "inserted 3 modifications, was expecting 3 to be confirmed")
			Expect(lsn).To(PointTo(BeNumerically("==", 3)), "max LSN of insertions was 3")
			Expect(atomic.LoadUint32(&insertCount)).To(Equal(uint32(2)), "should only insert 2 batches")
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
				Expect(atomic.LoadUint32(&insertCount)).To(Equal(uint32(1)), "should only insert a single batche")
			})
		})

		Context("when not exceeding buffer size", func() {
			BeforeEach(func() { bufferSize = len(modifications) + 1 })

			// This is a bit dodgy, as there's nothing to say the Insert isn't just too slow for
			// the expectation to catch things. We have to add a time.Sleep() to trigger the
			// asynchronous insertion, just in case.
			It("does not insert modifications", func() {
				time.Sleep(5 * time.Millisecond)
				Expect(inserter.Batches()).To(BeEmpty())
			})
		})
	})
})
