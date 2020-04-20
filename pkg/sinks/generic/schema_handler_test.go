package generic_test

import (
	"context"
	"encoding/json"
	"io/ioutil"

	kitlog "github.com/go-kit/kit/log"
	"github.com/lawrencejones/pg2sink/pkg/changelog"
	"github.com/lawrencejones/pg2sink/pkg/sinks/generic"

	. "github.com/onsi/ginkgo"
	_ "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
	_ "github.com/onsi/gomega/gstruct"
)

func mustSchemaFixture(path string) *changelog.Schema {
	var schema changelog.Schema

	jsonBytes, err := ioutil.ReadFile(path)
	Expect(err).NotTo(HaveOccurred(), "failed to read fixture")

	err = json.Unmarshal(jsonBytes, &schema)
	Expect(err).NotTo(HaveOccurred(), "failed to parse json fixture")

	return &schema
}

var _ = Describe("SchemaHandler", func() {
	type callStub struct {
		inserter generic.Inserter
		outcome  generic.SchemaHandlerOutcome
		err      error
	}

	var (
		calls                   []callStub
		example, exampleAnother *changelog.Schema
		handler                 generic.SchemaHandler
		handlerFunc             = func(context.Context, kitlog.Logger, *changelog.Schema) (generic.Inserter, generic.SchemaHandlerOutcome, error) {
			var call callStub
			call, calls = calls[0], calls[1:]
			return call.inserter, call.outcome, call.err
		}
	)

	JustBeforeEach(func() {
		handler = generic.SchemaHandlerFunc(handlerFunc)
	})

	BeforeSuite(func() {
		example = mustSchemaFixture("testdata/schema.example.json")
		exampleAnother = mustSchemaFixture("testdata/schema.example_another.json")
	})

	Describe("SchemaHandlerCacheOnFingerprint", func() {
		JustBeforeEach(func() {
			handler = generic.SchemaHandlerCacheOnFingerprint(handler)
		})

		var (
			inserterOnce, inserterTwice generic.Inserter
		)

		BeforeEach(func() {
			inserterOnce = generic.NewMemoryInserter()
			inserterTwice = generic.NewMemoryInserter()

			calls = []callStub{
				callStub{inserterOnce, generic.SchemaHandlerUpdate, error(nil)},
				callStub{inserterTwice, generic.SchemaHandlerUpdate, error(nil)},
			}
		})

		It("returns inserter from the wrapped handler", func() {
			inserter, _, _ := handler.Handle(context.Background(), logger, example)
			Expect(inserter).To(Equal(inserterOnce))

			inserter, _, _ = handler.Handle(context.Background(), logger, exampleAnother)
			Expect(inserter).To(Equal(inserterTwice))
		})

		Context("when schema has same fingerprint", func() {
			It("returns cached inserter", func() {
				_, _, err := handler.Handle(context.Background(), logger, example)
				Expect(err).NotTo(HaveOccurred())

				inserter, _, _ := handler.Handle(context.Background(), logger, example)
				Expect(inserter).To(Equal(inserterOnce))
			})
		})
	})
})
