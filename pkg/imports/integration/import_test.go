package integration

import (
	"context"
	"time"

	"github.com/lawrencejones/pgsink/pkg/dbschema/pgsink/model"
	"github.com/lawrencejones/pgsink/pkg/dbtest"
	"github.com/lawrencejones/pgsink/pkg/decode"
	"github.com/lawrencejones/pgsink/pkg/decode/gen/mappings"
	"github.com/lawrencejones/pgsink/pkg/imports"
	"github.com/lawrencejones/pgsink/pkg/logical"

	"github.com/jackc/pgtype"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gstruct"
	. "github.com/onsi/gomega/types"
)

var _ = Describe("Import", func() {
	var (
		ctx    context.Context
		cancel func()
	)

	var (
		schema             = "imports_import_integration_test"
		tableOneName       = "one"
		tableCompositeName = "composite"
		tableKeylessName   = "keyless"
		tableTextName      = "text"
	)

	db := dbtest.Configure(
		dbtest.WithSchema(schema),
		dbtest.WithTable(schema, tableOneName, "id bigserial primary key", "msg text"),
		dbtest.WithTable(schema, tableCompositeName, "id bigserial", "msg text", "primary key(id, msg)"),
		dbtest.WithTable(schema, tableKeylessName, "id bigserial", "msg text"),
		dbtest.WithTable(schema, tableTextName, "id text primary key", "msg text"),
	)

	BeforeEach(func() {
		ctx, cancel = db.Setup(context.Background(), 10*time.Second)
	})

	AfterEach(func() {
		cancel()
	})

	Describe(".Build()", func() {
		var (
			subject *imports.Import
			job     model.ImportJobs
			err     error
		)

		JustBeforeEach(func() {
			subject, err = imports.Build(ctx, logger, decode.NewDecoder(mappings.Mappings), db.GetConnection(ctx), job)
		})

		BeforeEach(func() {
			job = model.ImportJobs{
				ID:             123,
				SubscriptionID: "nothingness",
				Schema:         schema,
				TableName:      tableOneName,
				Cursor:         nil,
			}
		})

		It("succeeds", func() {
			Expect(err).NotTo(HaveOccurred())
		})

		It("builds import", func() {
			Expect(subject).To(PointTo(MatchFields(IgnoreExtras, Fields{
				"PrimaryKey": Equal("id"),
				"Cursor":     BeNil(),
			})))
		})

		It("builds relation namespace and name", func() {
			Expect(*subject.Relation).To(MatchFields(IgnoreExtras, Fields{
				"Namespace": Equal(schema),
				"Name":      Equal("one"),
			}))
		})

		It("builds relation columns", func() {
			Expect(subject.Relation.Columns).To(ConsistOf(
				logical.Column{
					Key:      true,
					Name:     "id",
					Type:     pgtype.Int8OID,
					Modifier: 0,
				},
				logical.Column{
					Key:      false,
					Name:     "msg",
					Type:     pgtype.TextOID,
					Modifier: 0,
				},
			))
		})

		Context("with a job cursor", func() {
			// Try to verify our primary key scanning works correctly. When we load a cursor
			// from an import job, we parse it into the Golang native type. This allows us to
			// work with pgx and natively providing the arguments in a prepared statement.
			//
			// We need to check that each primary key column type can be scanned, and that we
			// parse a nil value when the cursor is empty.
			DescribeTable("it scans the correct cursor value",
				func(tableName string, cursor string, matcher GomegaMatcher) {
					job.TableName = tableName
					if cursor != "" {
						job.Cursor = &cursor
					}

					subject, err = imports.Build(ctx, logger, decode.NewDecoder(mappings.Mappings), db.GetConnection(ctx), job)

					Expect(err).NotTo(HaveOccurred())
					Expect(subject.Cursor).To(matcher)
				},
				Entry("bigint", tableOneName, "1", Equal(int64(1))),
				Entry("bigint null", tableOneName, "", BeNil()),
				Entry("text", tableTextName, "PM123", Equal("PM123")),
				Entry("text null", tableTextName, "", BeNil()),
			)
		})

		Context("with composite primary key", func() {
			BeforeEach(func() {
				job.TableName = tableCompositeName
			})

			It("errors", func() {
				Expect(err).To(MatchError(ContainSubstring("unsupported multiple primary keys: id, msg")))
			})
		})

		Context("with no primary key", func() {
			BeforeEach(func() {
				job.TableName = tableKeylessName
			})

			It("errors", func() {
				Expect(err).To(MatchError(imports.NoPrimaryKeyError))
			})
		})
	})
})
