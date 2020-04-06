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

var _ = Describe("AsyncInserter", func() {
	var (
		ctx      context.Context
		async    generic.AsyncInserter
		inserter *fakeInserter
		cancel   func()
	)

	JustBeforeEach(func() {
		async = generic.WrapAsync(inserter)
	})

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), time.Second)
		inserter = &fakeInserter{MemoryInserter: generic.NewMemoryInserter()}
	})

	AfterEach(func() {
		cancel()
	})

	It("returns a promise that resolves with the result of the insert", func() {
		result := async.Insert(ctx, []*changelog.Modification{fixtureExample1Insert})
		count, lsn, err := result.Get(ctx)

		Expect(err).To(BeNil())
		Expect(count).To(Equal(1))
		Expect(lsn).To(PointTo(BeNumerically("==", 1)))
	})

	Describe(".Flush", func() {
		Context("with many in-flight inserts", func() {
			var (
				release chan struct{}
			)

			BeforeEach(func() {
				release = make(chan struct{})

				// Before calling the insert, all our inserters pause waiting for the release
				// channel to be closed
				inserter.BeforeFunc = func(context.Context, []*changelog.Modification) {
					select {
					case <-ctx.Done():
						panic("context expired before the inserters were released")
					case <-release:
						logger.Log("event", "inserter.release", "msg", "released, proceeding")
					}
				}

			})

			It("returns result that waits for all in-flight inserts to complete", func() {
				// Deliberately invert the order of the updates, to help identify any incorrect
				// logic around finding the max LSN
				async.Insert(ctx, []*changelog.Modification{fixtureExample1Insert})
				async.Insert(ctx, []*changelog.Modification{fixtureExample1UpdateSecond})
				async.Insert(ctx, []*changelog.Modification{fixtureExample1UpdateFirst})

				result := async.Flush(ctx)
				close(release) // release the inserters
				count, lsn, err := result.Get(ctx)

				Expect(err).To(BeNil(), "no insertions should fail, so expected nil error")
				Expect(count).To(Equal(3), "async-inserted 3 modifications, was expecting 3 to be confirmed")
				Expect(lsn).To(PointTo(BeNumerically("==", 3)), "max LSN of insertions was 3")
			})
		})
	})
})
