package api_test

import (
	"context"
	"time"

	"github.com/lawrencejones/pgsink/api"
	"github.com/lawrencejones/pgsink/api/gen/tables"
	"github.com/lawrencejones/pgsink/internal/dbtest"
	"github.com/lawrencejones/pgsink/pkg/subscription"

	. "github.com/lawrencejones/pgsink/internal/dbschema/pgsink/table"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gstruct"
	. "github.com/onsi/gomega/types"
)

var _ = Describe("Tables", func() {
	var (
		ctx    context.Context
		cancel func()
		pub    *subscription.Publication
		svc    tables.Service
	)

	var (
		schema = "api_tables_test"
	)

	db := dbtest.Configure(
		dbtest.WithSchema(schema),
		dbtest.WithPublication(schema),
	)

	BeforeEach(func() {
		ctx, cancel = db.Setup(context.Background(), 10*time.Second)

		var err error
		pub, err = subscription.FindOrCreatePublication(ctx, logger, db.GetDB(), schema)
		Expect(err).NotTo(HaveOccurred())

		svc = api.NewTables(db.GetDB(), pub)
	})

	AfterEach(func() {
		cancel()
	})

	// Fuzzy ignore-extras on pointer struct match
	ptrMatch := func(fields Fields) GomegaMatcher {
		return PointTo(MatchFields(IgnoreExtras, fields))
	}

	// Shorthand for creating import jobs
	createImport := func(tableName string, expiredAt, completedAt *time.Time) {
		stmt := ImportJobs.
			INSERT(ImportJobs.SubscriptionID, ImportJobs.Schema, ImportJobs.TableName, ImportJobs.ExpiredAt, ImportJobs.CompletedAt).
			VALUES(pub.ID, schema, tableName, expiredAt, completedAt)

		_, err := stmt.ExecContext(ctx, db.GetDB())
		Expect(err).NotTo(HaveOccurred())
	}

	Describe("List", func() {
		var (
			resp []*tables.Table
			err  error
		)

		JustBeforeEach(func() {
			resp, err = svc.List(ctx, &tables.ListPayload{Schema: schema})
		})

		It("returns an empty list", func() {
			Expect(resp).To(BeEmpty())
			Expect(err).NotTo(HaveOccurred())
		})

		Context("with two tables", func() {
			BeforeEach(func() {
				db.MustExec(ctx, `create table larry (id bigint);`)
				db.MustExec(ctx, `create table moe (id bigint);`)
			})

			Context("where one has an import", func() {
				BeforeEach(func() {
					createImport("larry", nil, nil)
				})
			})

			// Important to test for this case, as go-jet can change behaviour when there are
			// results for one side of the join only
			It("returns both tables", func() {
				Expect(resp).To(HaveLen(2))
			})
		})

		Context("with a table", func() {
			BeforeEach(func() {
				db.MustExec(ctx, `create table larry (id bigint);`)
			})

			It("returns the table", func() {
				Expect(resp[0]).To(ptrMatch(Fields{
					"Schema": Equal(schema),
					"Name":   Equal("larry"),
				}))
			})

			It("has inactive publication status", func() {
				Expect(resp[0]).To(ptrMatch(Fields{
					"PublicationStatus": Equal("inactive"),
				}))
			})

			It("has inactive import status", func() {
				Expect(resp[0]).To(ptrMatch(Fields{
					"ImportStatus": Equal("inactive"),
				}))
			})

			Context("which has an import", func() {
				BeforeEach(func() {
					createImport("larry", nil, nil)
				})

				It("has scheduled import status", func() {
					Expect(resp[0]).To(ptrMatch(Fields{
						"ImportStatus": Equal("scheduled"),
					}))
				})
			})

			Context("which has complete import", func() {
				BeforeEach(func() {
					completedAt := time.Now()
					createImport("larry", nil, &completedAt)
				})

				It("has completed import status", func() {
					Expect(resp[0]).To(ptrMatch(Fields{
						"ImportStatus": Equal("complete"),
					}))
				})
			})

			Context("which has an expired import", func() {
				BeforeEach(func() {
					expiredAt := time.Now()
					createImport("larry", &expiredAt, nil)
				})

				It("has expired import status", func() {
					Expect(resp[0]).To(ptrMatch(Fields{
						"ImportStatus": Equal("expired"),
					}))
				})
			})

			Context("which has scheduled import, and older expired", func() {
				BeforeEach(func() {
					expiredAt := time.Now()
					createImport("larry", &expiredAt, nil)
					createImport("larry", nil, nil)
				})

				It("has scheduled import status", func() {
					Expect(resp[0]).To(ptrMatch(Fields{
						"ImportStatus": Equal("scheduled"),
					}))
				})
			})
		})
	})
})
