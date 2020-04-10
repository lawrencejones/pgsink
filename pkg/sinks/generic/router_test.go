package generic_test

import (
	"context"
	"time"

	"github.com/lawrencejones/pg2sink/pkg/changelog"
	"github.com/lawrencejones/pg2sink/pkg/sinks/generic"

	. "github.com/onsi/ginkgo"
	_ "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
	_ "github.com/onsi/gomega/gstruct"
)

func newFakeInserter() *fakeInserter {
	return &fakeInserter{MemoryInserter: generic.NewMemoryInserter()}
}

func ExpectResolveSuccess(_ int, _ *uint64, err error, msg ...interface{}) {
	Expect(err).NotTo(HaveOccurred(), msg...)
}

var _ = Describe("Router", func() {
	var (
		ctx          context.Context
		router       generic.Router
		dogNS, catNS changelog.Namespace
		dog, cat     *fakeInserter
		cancel       func()
	)

	JustBeforeEach(func() {
		router = generic.NewRouter(logger)

		dogResult := router.Register(ctx, dogNS, generic.WrapAsync(dog))
		ExpectResolveSuccess(dogResult.Get(ctx))

		catResult := router.Register(ctx, catNS, generic.WrapAsync(cat))
		ExpectResolveSuccess(catResult.Get(ctx))
	})

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), time.Second)
		dog, dogNS = newFakeInserter(), changelog.Namespace("public.dog")
		cat, catNS = newFakeInserter(), changelog.Namespace("public.cat")
	})

	AfterEach(func() {
		cancel()
	})

	It("routes inserts to the correct inserter", func() {
		ExpectResolveSuccess(router.Insert(ctx, []*changelog.Modification{fixtureDog1Insert}).Get(ctx))
		ExpectResolveSuccess(router.Insert(ctx, []*changelog.Modification{fixtureCat1Insert}).Get(ctx))

		Expect(dog.Store()).To(ConsistOf(fixtureDog1Insert))
		Expect(cat.Store()).To(ConsistOf(fixtureCat1Insert))
	})

	Describe(".Flush", func() {
		Context("when inserters take a while to flush", func() {
			var (
				release chan struct{}
			)

			BeforeEach(func() {
				release = make(chan struct{})

				// Cause the dog inserter to hang until explicitly released
				dog.BeforeFunc = func(context.Context, []*changelog.Modification) error {
					select {
					case <-ctx.Done():
						logger.Log("event", "inserter.expired", "msg", "context expired before the inserter was released")
					case <-release:
						logger.Log("event", "inserter.release", "msg", "released, proceeding")
					}

					return nil
				}
			})

			It("waits for each inserter to flush", func() {
				router.Insert(ctx, []*changelog.Modification{fixtureDog1Insert})
				router.Insert(ctx, []*changelog.Modification{fixtureCat1Insert})

				{
					timeoutCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
					defer cancel()

					_, _, err := router.Flush(timeoutCtx).Get(timeoutCtx)
					Expect(err).NotTo(BeNil(), "dog inserter is blocked, so our flush should timeout")
				}

				close(release)
				count, _, err := router.Flush(ctx).Get(ctx)
				Expect(err).To(BeNil(), "all inserters are unblocked, so flush should succeed")
				Expect(count).To(Equal(0), "Flush resets, so we expect it to be unaware of the previous insertions")
			})
		})
	})
})
