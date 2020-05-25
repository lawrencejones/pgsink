package util_test

import (
	"github.com/lawrencejones/pgsink/pkg/util"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("SyncSet", func() {
	var (
		set *util.SyncSet
	)

	BeforeEach(func() {
		set = new(util.SyncSet)

		set.Add(1)
		set.Add(2)
	})

	Describe("Add()", func() {
		BeforeEach(func() {
			set.Add(3)
		})

		It("adds element to group", func() {
			Expect(set.All()).To(ContainElement(3))
		})

		Context("when adding existing element", func() {
			BeforeEach(func() {
				set.Add(3)
			})

			It("doesn't duplicate element in result set", func() {
				Expect(set.All()).To(ConsistOf(1, 2, 3))
			})
		})
	})

	Describe("Remove()", func() {
		BeforeEach(func() {
			set.Add(1)
			set.Add(2)

			Expect(set.All()).To(ContainElement(1))
			Expect(set.All()).To(ContainElement(2))

			set.Remove(1)
		})

		It("removes element from set", func() {
			Expect(set.All()).NotTo(ContainElement(1))
		})

		Context("when element is not there", func() {
			It("does nothing", func() {
				set.Remove(3)

				Expect(set.All()).To(ConsistOf(2))
			})
		})
	})

	Describe("All()", func() {
		Context("with type cast", func() {
			It("generates appropriate slice type", func() {
				Expect(set.All([]int{})).To(BeAssignableToTypeOf([]int{}))
			})
		})
	})
})
