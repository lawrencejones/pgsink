package generic_test

import (
	"context"
	"fmt"

	"github.com/lawrencejones/pg2sink/pkg/sinks/generic"

	. "github.com/onsi/ginkgo"
	_ "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gstruct"
)

var _ = Describe("InsertResult", func() {
	var (
		ctx, cancel = context.WithCancel(context.Background())
		result      generic.InsertResult
	)

	BeforeEach(func() {
		result = generic.NewInsertResult()
	})

	type resolveable interface {
		Resolve(int, *uint64, error) generic.InsertResult
	}

	Describe(".Get", func() {
		It("returns result once resolved", func() {
			result.(resolveable).Resolve(3, nil, nil)
			count, lsn, err := result.Get(ctx)

			Expect(err).To(BeNil())
			Expect(count).To(Equal(3))
			Expect(lsn).To(BeNil())
		})

		Context("with expired context", func() {
			BeforeEach(func() {
				cancel()
			})

			It("returns context expired error", func() {
				_, _, err := result.Get(ctx)
				Expect(err).To(MatchError("context canceled"))
			})

			Context("when already resolved", func() {
				BeforeEach(func() {
					result.(resolveable).Resolve(3, nil, nil)
				})

				// When the context has expired but we've already done the work, there is no need
				// for us to throwaway the result. We may as well return it, and have the parent
				// function decide on an appropriate action.
				It("returns result anyway", func() {
					Expect(result.Get(ctx)).To(Equal(3))
				})
			})
		})
	})

	newLSN := func(lsn uint64) *uint64 {
		return &lsn
	}

	Describe(".Fold", func() {
		BeforeEach(func() {
			result = result.(resolveable).Resolve(1, newLSN(5), nil)
			result = result.Fold(generic.NewInsertResult().Resolve(2, newLSN(11), nil))
			result = result.Fold(generic.NewInsertResult().Resolve(3, nil, nil))
			result = result.Fold(generic.NewInsertResult().Resolve(4, newLSN(7), nil))
		})

		It("combines results together, resolving with all sub-results", func() {
			count, lsn, err := result.Get(ctx)

			Expect(err).To(BeNil(), "error should be nil, if all are successful")
			Expect(count).To(Equal(10), "count should be sum of sub-results")
			Expect(lsn).To(PointTo(BeNumerically("==", 11)), "lsn should be greatest of sub-results")
		})

		Context("when any fail", func() {
			BeforeEach(func() {
				result = result.Fold(generic.NewInsertResult().Resolve(0, nil, fmt.Errorf("oops")))
			})

			It("resolves the error of the sub-result", func() {
				_, _, err := result.Get(ctx)
				Expect(err).To(MatchError("oops"))
			})
		})
	})
})
