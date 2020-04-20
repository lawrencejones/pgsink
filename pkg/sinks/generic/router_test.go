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

var _ = Describe("router", func() {
	var (
		ctx                              context.Context
		router                           generic.Router
		async                            generic.AsyncInserter
		backend                          fakeBackend
		exampleRoute, dogRoute, catRoute generic.Route
		example, dog, cat                *fakeInserter
		cancel                           func()

		suite = AsyncInserterSuite{
			// We don't use the provided fakeBackend here, as we connect the router to the
			// individual inserters using Register
			New: func(_ fakeBackend) generic.AsyncInserter {
				router = generic.NewRouter(logger)

				exampleResult := router.Register(ctx, exampleRoute, generic.NewAsyncInserter(example))
				ExpectResolveSuccess(exampleResult.Get(ctx))

				dogResult := router.Register(ctx, dogRoute, generic.NewAsyncInserter(dog))
				ExpectResolveSuccess(dogResult.Get(ctx))

				catResult := router.Register(ctx, catRoute, generic.NewAsyncInserter(cat))
				ExpectResolveSuccess(catResult.Get(ctx))

				return router
			},
			NewBackend: func() fakeBackend {
				example, exampleRoute = newFakeInserter(), generic.Route("public.example")
				dog, dogRoute = newFakeInserter(), generic.Route("public.dog")
				cat, catRoute = newFakeInserter(), generic.Route("public.cat")

				return newFakeBackend(example, dog, cat)
			},
		}
	)

	// Have the suite do our setup, and bind the given variables
	suite.Bind(&ctx, &async, &backend, &cancel)

	Describe("AsyncInserter interface", func() {
		verifyGenericAsyncInserter(suite)
	})

	It("routes inserts to the correct inserter", func() {
		ExpectResolveSuccess(router.Insert(ctx, []*changelog.Modification{fixtureDog1Insert}).Get(ctx))
		ExpectResolveSuccess(router.Insert(ctx, []*changelog.Modification{fixtureCat1Insert}).Get(ctx))

		Expect(dog.Store()).To(ConsistOf(fixtureDog1Insert))
		Expect(cat.Store()).To(ConsistOf(fixtureCat1Insert))
	})

	Context("when registered RouterMatchAll route", func() {
		PIt("routes all inserters to this inserter", func() {
			// TODO
		})
	})

	Describe(".Flush", func() {
		Context("when inserters take a while to flush", func() {
			var (
				resume chan struct{}
			)

			BeforeEach(func() {
				resume = dog.Pause(ctx)
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

				close(resume)
				count, _, err := router.Flush(ctx).Get(ctx)
				Expect(err).To(BeNil(), "all inserters are unblocked, so flush should succeed")
				Expect(count).To(Equal(0), "Flush resets, so we expect it to be unaware of the previous insertions")
			})
		})
	})
})
