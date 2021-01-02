package bigquery

import (
	"io/ioutil"

	bq "cloud.google.com/go/bigquery"

	"github.com/jackc/pgtype"
	"github.com/lawrencejones/pgsink/pkg/changelog"
	"github.com/lawrencejones/pgsink/pkg/logical"
	. "github.com/onsi/ginkgo"
	_ "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
	_ "github.com/onsi/gomega/gstruct"
)

func mustFixture(path string) string {
	bytes, err := ioutil.ReadFile(path)
	Expect(err).NotTo(HaveOccurred(), "failed to read fixture")

	return string(bytes)
}

var (
	dogsSchemaFixture = changelog.Schema{
		Spec: logical.Relation{
			Namespace: "public",
			Name:      "dogs",
			Columns: []logical.Column{
				{
					Key:  true,
					Name: "tag",
					Type: pgtype.Int8OID,
				},
				{
					Name: "name",
					Type: pgtype.TextOID,
				},
			},
		},
	}
	exampleSchemaFixture = changelog.Schema{
		Spec: logical.Relation{
			Namespace: "public",
			Name:      "example",
			Columns: []logical.Column{
				{
					Key:  true,
					Name: "id",
					Type: pgtype.Int8OID,
				},
				{
					Name: "msg",
					Type: pgtype.TextOID,
				},
				{
					Name: "another",
					Type: pgtype.TextOID,
				},
			},
		},
	}
)

var _ = Describe("buildView", func() {
	var (
		md  *bq.TableMetadata
		err error

		tableName     string
		rawTableName  string
		schemaFixture changelog.Schema
	)

	JustBeforeEach(func() {
		md, err = buildView(tableName, rawTableName, schemaFixture.Spec)
	})

	BeforeEach(func() {
		tableName = "example"
		rawTableName = "project.dataset.example_raw"
		schemaFixture = exampleSchemaFixture
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
			schemaFixture = dogsSchemaFixture
		})

		It("generates view that correctly uses primary keys", func() {
			Expect(err).NotTo(HaveOccurred(), "unexpected error with valid table configuration")
			Expect(md.ViewQuery).To(Equal(mustFixture("testdata/view.dogs.sql")))
		})
	})
})
