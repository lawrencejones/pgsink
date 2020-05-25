package bigquery

import (
	"encoding/json"
	"io/ioutil"

	bq "cloud.google.com/go/bigquery"

	"github.com/lawrencejones/pgsink/pkg/changelog"
	. "github.com/onsi/ginkgo"
	_ "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
	_ "github.com/onsi/gomega/gstruct"
)

func mustSchemaFixture(path string) *changelog.Schema {
	var schema changelog.Schema

	err := json.Unmarshal([]byte(mustFixture(path)), &schema)
	Expect(err).NotTo(HaveOccurred(), "failed to parse json fixture")

	return &schema
}

func mustFixture(path string) string {
	bytes, err := ioutil.ReadFile(path)
	Expect(err).NotTo(HaveOccurred(), "failed to read fixture")

	return string(bytes)
}

var _ = Describe("buildView", func() {
	var (
		md  *bq.TableMetadata
		err error

		tableName         string
		rawTableName      string
		schemaFixtureName string
	)

	JustBeforeEach(func() {
		md, err = buildView(tableName, rawTableName, mustSchemaFixture(schemaFixtureName).Spec)
	})

	BeforeEach(func() {
		tableName = "example"
		rawTableName = "project.dataset.example_raw"
		schemaFixtureName = "testdata/schema.example.json"
	})

	It("generates view that correctly uses primary keys", func() {
		Expect(err).NotTo(HaveOccurred(), "unexpected error with valid table configuration")
		Expect(md.ViewQuery).To(Equal(mustFixture("testdata/view.example.sql")))
	})

	// Here to confirm we don't regress support for non-ID columns, as was previously the
	// case
	Context("with non-id primary key column", func() {
		BeforeEach(func() {
			rawTableName = "project.dataset.dogs_raw"
			schemaFixtureName = "testdata/schema.dogs.json"
		})

		It("generates view that correctly uses primary keys", func() {
			Expect(err).NotTo(HaveOccurred(), "unexpected error with valid table configuration")
			Expect(md.ViewQuery).To(Equal(mustFixture("testdata/view.dogs.sql")))
		})
	})
})
