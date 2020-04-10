package generic_test

import (
	"context"
	"fmt"
	"time"

	"github.com/lawrencejones/pg2sink/pkg/changelog"
	"github.com/lawrencejones/pg2sink/pkg/sinks/generic"

	. "github.com/onsi/ginkgo"
	_ "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gstruct"
)

// AsyncInserterSuite provides shared test hooks that can be useful across various
// AsyncInserters, along with the ability to validate an AsyncInserter adheres to the
// correct interface semantics.
//
// All implementations of AsyncInserters should pass this test suite.
type AsyncInserterSuite struct {
	New func(backend *fakeInserter) generic.AsyncInserter
}

func (s AsyncInserterSuite) Setup(ctx *context.Context, async *generic.AsyncInserter, backend **fakeInserter, cancel *func()) {
	JustBeforeEach(func() {
		*async = s.New(*backend)
	})

	BeforeEach(func() {
		*ctx, *cancel = context.WithTimeout(context.Background(), time.Second)
		*backend = &fakeInserter{MemoryInserter: generic.NewMemoryInserter()}
	})

	AfterEach(func() {
		(*cancel)()
	})
}

func (s AsyncInserterSuite) InsertOne(ctx context.Context, async generic.AsyncInserter) ([]*changelog.Modification, generic.InsertResult) {
	ms := []*changelog.Modification{fixtureExample1Insert}
	return ms, async.Insert(ctx, ms)
}

func (s AsyncInserterSuite) InsertMore(ctx context.Context, async generic.AsyncInserter) ([]*changelog.Modification, generic.InsertResult) {
	ms := []*changelog.Modification{fixtureExample1UpdateFirst, fixtureExample1UpdateSecond}
	return ms, async.Insert(ctx, ms)
}

func (s AsyncInserterSuite) InsertMany(ctx context.Context, async generic.AsyncInserter) ([][]*changelog.Modification, []generic.InsertResult) {
	// Deliberately invert the order of the updates, to help identify any incorrect
	// logic around finding the max LSN
	batches := [][]*changelog.Modification{
		[]*changelog.Modification{fixtureExample1Insert},
		[]*changelog.Modification{fixtureExample1UpdateSecond},
		[]*changelog.Modification{fixtureExample1UpdateFirst},
	}

	results := []generic.InsertResult{}
	for _, batch := range batches {
		results = append(results, async.Insert(ctx, batch))
	}

	return batches, results
}

func verifyGenericAsyncInserter(suite AsyncInserterSuite) {
	var (
		ctx     context.Context
		async   generic.AsyncInserter
		backend *fakeInserter
		cancel  func()
	)

	suite.Setup(&ctx, &async, &backend, &cancel)

	maxLSN := func(modifications []*changelog.Modification) *uint64 {
		var lsn *uint64
		for _, m := range modifications {
			if m.LSN != nil {
				if lsn == nil || *lsn < *m.LSN {
					lsn = m.LSN
				}
			}
		}

		return lsn
	}

	Describe(".Insert", func() {
		It("returns a promise that resolves once modification is inserted", func() {
			ms, result := suite.InsertOne(ctx, async)
			count, lsn, err := result.Get(ctx)

			Expect(err).To(BeNil(), "insert should succeed, so an error is unexpected")
			Expect(count).To(Equal(len(ms)), "insertion count doesn't match what we inserted")
			Expect(lsn).To(PointTo(BeNumerically("==", *maxLSN(ms))), "should equal the highest inserted lsn")
			Expect(backend.Store()).To(
				ConsistOf(ms), "result resolved, so expected modifications to be inserted",
			)
		})

		Context("when underlying insert hangs", func() {
			var (
				resumes []chan struct{}
			)

			BeforeEach(func() {
				resumes = []chan struct{}{backend.Pause()}
			})

			AfterEach(func() {
				for _, resume := range resumes {
					close(resume)
				}
			})

			It("fails with a timeout", func() {
				timeoutCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
				defer cancel()

				_, result := suite.InsertOne(ctx, async)
				_, _, err := result.Get(timeoutCtx)

				Expect(err).NotTo(BeNil(), "inserter is paused, so our async insert should timeout")
				Expect(backend.Store()).To(BeEmpty(), "inserts are paused, so nothing should have inserted")
			})
		})
	})

	Describe(".Flush", func() {
		It("returns a promise that resolves once modifications are inserted", func() {
			ms, _ := suite.InsertOne(ctx, async)
			count, lsn, err := async.Flush(ctx).Get(ctx)

			Expect(err).To(BeNil(), "no insertions should fail, so expected nil error")
			Expect(count).To(Equal(1), "called InsertOne, so expected 1 insertion")
			Expect(lsn).To(PointTo(BeNumerically("==", *maxLSN(ms))), "should equal the highest inserted lsn")
		})

		Context("when insert fails", func() {
			var (
				succeeds []func()
				result   generic.InsertResult
			)

			BeforeEach(func() {
				succeeds = []func(){backend.Fail(fmt.Errorf("hot dang"))}
			})

			JustBeforeEach(func() {
				_, insertResult := suite.InsertOne(ctx, async)
				_, _, err := insertResult.Get(ctx)
				result = async.Flush(ctx)

				Expect(err).To(MatchError("hot dang"), "configured backend to fail, so expected error")
			})

			It("resolves with error", func() {
				_, _, err := result.Get(ctx)
				Expect(err).To(MatchError("hot dang"))
			})

			Describe("successive flush", func() {
				JustBeforeEach(func() {
					for _, succeed := range succeeds {
						succeed()
					}
				})

				It("succeeds", func() {
					ms, secondResult := suite.InsertMore(ctx, async)
					_, _, err := secondResult.Get(ctx)
					Expect(err).NotTo(HaveOccurred(), "removed force failure, so expected insertion to succeed")

					count, _, err := async.Flush(ctx).Get(ctx)
					Expect(err).NotTo(HaveOccurred(), "flushes reset errors, but this gave an old error")
					Expect(count).To(BeEquivalentTo(len(ms)), "expected flushed count to match the second insert")
				})
			})
		})

		Context("with many in-flight inserts", func() {
			var (
				resumes []chan struct{}
				batches [][]*changelog.Modification
				results []generic.InsertResult
			)

			JustBeforeEach(func() {
				batches, results = suite.InsertMany(ctx, async)
			})

			BeforeEach(func() {
				resumes = []chan struct{}{backend.Pause()}
			})

			It("returns a promise that resolves once all in-flight inserts complete", func() {
				result := async.Flush(ctx)
				ms := []*changelog.Modification{}
				for _, batch := range batches {
					ms = append(ms, batch...)
				}

				for idx, resume := range resumes {
					// Resume the backend inserter
					close(resume)

					timeoutCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
					defer cancel()

					count, lsn, err := result.Get(timeoutCtx)

					// If we're not the last backend inserter to be resumed, then we should expect
					// to timeout. Flush should only succeed when all pending inserts have
					// completed, so nothing should resolve until everything has been inserted.
					if idx != len(resumes)-1 {
						Expect(err).NotTo(BeNil(), "inserters remain paused, so flush should not have succeeded")
						Expect(backend.Store()).NotTo(
							ConsistOf(ms), "we should not have inserted everything until all inserters are resumed",
						)

						continue
					}

					Expect(err).To(BeNil(), "all inserters have been resumed, so flush should have succeeded")
					Expect(count).To(Equal(len(ms)), "flush count should include all modifications that were pending")
					Expect(lsn).To(PointTo(BeNumerically("==", *maxLSN(ms))), "should equal the highest inserted lsn")
					Expect(backend.Store()).To(ConsistOf(ms), "we expect all modifications to have been inserted")

					for _, result := range results {
						_, _, err := result.Get(ctx)
						Expect(err).NotTo(HaveOccurred(), "all insert results should also succeed")
					}

					return
				}

				panic("there were no resumes, Pause is implemented incorrectly")
			})
		})

		Describe("successive calls", func() {
			var (
				firstFlush         generic.InsertResult
				firstModifications []*changelog.Modification
			)

			JustBeforeEach(func() {
				firstModifications, _ = suite.InsertOne(ctx, async)
				firstFlush = async.Flush(ctx)
			})

			It("reference only what has been inserted after the original call", func() {
				secondModifications, _ := suite.InsertMore(ctx, async)
				secondFlush := async.Flush(ctx)

				firstCount, _, firstFlushErr := firstFlush.Get(ctx)
				Expect(firstFlushErr).NotTo(HaveOccurred())
				Expect(firstCount).To(BeEquivalentTo(len(firstModifications)),
					"flush should count only up-to the flush call insertions")

				secondCount, _, secondFlushErr := secondFlush.Get(ctx)
				Expect(secondFlushErr).NotTo(HaveOccurred())
				Expect(secondCount).To(BeEquivalentTo(len(secondModifications)),
					"flush should count only up-to the flush call insertions")
			})
		})
	})
}
