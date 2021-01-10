package integration

import (
	"context"
	"fmt"
	"time"

	"github.com/lawrencejones/pgsink/internal/dbschema/pgsink/model"
	. "github.com/lawrencejones/pgsink/internal/dbschema/pgsink/table"
	"github.com/lawrencejones/pgsink/internal/dbtest"
	"github.com/lawrencejones/pgsink/pkg/changelog"
	. "github.com/lawrencejones/pgsink/pkg/changelog/matchers"
	"github.com/lawrencejones/pgsink/pkg/decode"
	"github.com/lawrencejones/pgsink/pkg/decode/gen/mappings"
	"github.com/lawrencejones/pgsink/pkg/imports"
	"github.com/lawrencejones/pgsink/pkg/sinks/generic"

	. "github.com/go-jet/jet/v2/postgres"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gstruct"
	_ "github.com/onsi/gomega/types"
)

type memoryStore struct {
	schemas       []changelog.Schema
	modifications []changelog.Modification
}

func (m *memoryStore) Inserter() generic.Inserter {
	return generic.InserterFunc(
		func(_ context.Context, modifications []*changelog.Modification) (count int, lsn *uint64, err error) {
			for _, modification := range modifications {
				m.modifications = append(m.modifications, *modification)
			}

			return len(modifications), nil, nil // no LSN, we don't use that in imports
		},
	)
}

func (m *memoryStore) SchemaHandler() func(context.Context, *changelog.Schema) error {
	return func(ctx context.Context, schema *changelog.Schema) error {
		m.schemas = append(m.schemas, *schema)
		return nil
	}
}

var _ = Describe("Importer", func() {
	var (
		ctx             context.Context
		cancel          func()
		store           *memoryStore
		importer        imports.Importer
		snapshotTimeout time.Duration
		batchLimit      int
	)

	var (
		schema       = "imports_importer_integration_test"
		tableOneName = "one"
		tableOne     = changelog.Table{Schema: schema, TableName: tableOneName}
	)

	db := dbtest.Configure(
		dbtest.WithSchema(schema),
		dbtest.WithTable(schema, tableOneName, "id bigserial primary key", "msg text"),
	)

	BeforeEach(func() {
		ctx, cancel = db.Setup(context.Background(), 10*time.Second)
		store = &memoryStore{}
		snapshotTimeout = time.Second
		batchLimit = 2
	})

	AfterEach(func() {
		cancel()
	})

	JustBeforeEach(func() {
		importer = imports.NewImporter(
			generic.SinkBuilder(
				// Not setting an interval means we continually flush, which isn't a good test
				generic.SinkBuilder.WithFlushInterval(time.Second),
				// An atypical schema handler that stores every schema we receive in the memory
				// store, so we can asset on its contents
				generic.SinkBuilder.WithSchemaHandler(
					generic.SchemaHandlerGlobalInserter(
						store.Inserter(),
						func(ctx context.Context, schema *changelog.Schema) error {
							store.schemas = append(store.schemas, *schema)
							return nil
						},
					),
				),
			),
			decode.NewDecoder(mappings.Mappings),
			imports.ImporterOptions{
				SnapshotTimeout: snapshotTimeout,
				BatchLimit:      batchLimit,
			},
		)
	})

	get := func(jobID int64) *model.ImportJobs {
		stmt := ImportJobs.
			SELECT(ImportJobs.AllColumns).
			WHERE(ImportJobs.ID.EQ(Int(jobID))).
			LIMIT(1)

		var jobs []*model.ImportJobs
		Expect(stmt.QueryContext(ctx, db.GetDB(), &jobs)).To(Succeed())

		return jobs[0]
	}

	insert := func(job model.ImportJobs) model.ImportJobs {
		stmt := ImportJobs.
			INSERT(ImportJobs.AllColumns).
			MODEL(job).
			RETURNING(ImportJobs.AllColumns)
		Expect(stmt.QueryContext(ctx, db.GetDB(), &job)).To(Succeed(), "import job should have inserted")

		return job
	}

	Describe(".Do()", func() {
		var (
			job model.ImportJobs
			err error
		)

		BeforeEach(func() {
			job = model.ImportJobs{
				ID:             123,
				SubscriptionID: "subscription-id",
				Schema:         schema,
				TableName:      tableOneName,
				Cursor:         nil,
			}
		})

		JustBeforeEach(func() {
			job = insert(job)

			tx, txErr := db.GetConnection(ctx).Begin(ctx)
			Expect(txErr).NotTo(HaveOccurred())

			// Run the import, then commit the transaction. The importer interface leaves the
			// transaction uncommitted to allow the caller to control whether a jobs work should
			// become visible, but we want our updates to be visible so we can view the state of
			// our job.
			err = importer.Do(ctx, logger, tx, job)
			tx.Commit(ctx)
		})

		itSuccessfullyImports := func() {
			It("does not error", func() {
				Expect(err).NotTo(HaveOccurred())
			})

			It("does not mark the import with an error", func() {
				Expect(get(job.ID).Error).To(BeNil())
			})

			// Schemas should be published on every import, to guarantee the sink has received
			// a schema before it gets a modification.
			It("publishes the table schema", func() {
				Expect(store.schemas).To(ConsistOf(
					SchemaMatcher(schema, tableOneName),
				))
			})
		}

		Context("when table is empty", func() {
			itSuccessfullyImports()

			It("marks the import as complete", func() {
				Expect(get(job.ID).CompletedAt).NotTo(BeNil())
			})

			It("publishes no rows", func() {
				Expect(store.modifications).To(BeEmpty())
			})
		})

		Context("with rows to be imported", func() {
			BeforeEach(func() {
				batchLimit = 3

				db.MustExec(ctx, fmt.Sprintf(`insert into %s (id, msg) values (1, 'meow');`, tableOne))
				db.MustExec(ctx, fmt.Sprintf(`insert into %s (id, msg) values (2, 'woof');`, tableOne))
			})

			itSuccessfullyImports()

			It("marks the import as complete", func() {
				Expect(get(job.ID).CompletedAt).NotTo(BeNil())
			})

			It("publishes all rows from the table", func() {
				Expect(store.modifications).To(ConsistOf(
					ModificationMatcher(schema, tableOneName).WithBefore(BeNil()).WithAfter(
						MatchAllKeys(Keys{
							"id":  BeAssignableToTypeOf(int64(0)),
							"msg": Equal("meow"),
						}),
					),
					ModificationMatcher(schema, tableOneName).WithBefore(BeNil()).WithAfter(
						MatchAllKeys(Keys{
							"id":  BeAssignableToTypeOf(int64(0)),
							"msg": Equal("woof"),
						}),
					),
				))
			})

			Context("equal to batch limit", func() {
				BeforeEach(func() {
					batchLimit = 2
				})

				It("does not mark import as complete", func() {
					Expect(get(job.ID).CompletedAt).To(BeNil())
				})

				It("sets the cursor to the last processed element", func() {
					Expect(get(job.ID).Cursor).To(PointTo(Equal("2")))
				})
			})

			Context("greater than batch limit", func() {
				BeforeEach(func() {
					batchLimit = 1
				})

				It("does not mark import as complete", func() {
					Expect(get(job.ID).CompletedAt).To(BeNil())
				})

				It("sets the cursor to the last processed element", func() {
					Expect(get(job.ID).Cursor).To(PointTo(Equal("1")))
				})
			})
		})

		strPtr := func(value string) *string {
			return &value
		}

		Context("with existing cursor", func() {
			BeforeEach(func() {
				batchLimit = 2

				// Set cursor to the value of the first rows id column. This is how we set the
				// cursor value once we finish an import, and emulates having imported only the
				// first of the rows.
				job.Cursor = strPtr("1")

				db.MustExec(ctx, fmt.Sprintf(`insert into %s (id, msg) values (1, 'meow');`, tableOneName))
				db.MustExec(ctx, fmt.Sprintf(`insert into %s (id, msg) values (2, 'woof');`, tableOneName))
				db.MustExec(ctx, fmt.Sprintf(`insert into %s (id, msg) values (3, 'roar');`, tableOneName))
			})

			itSuccessfullyImports()

			It("does not publish rows that had already been imported", func() {
				Expect(store.modifications).NotTo(ContainElement(
					ModificationMatcher(schema, tableOneName).WithAfter(
						MatchKeys(IgnoreMissing, Keys{
							"id": BeEquivalentTo(int64(1)),
						}),
					),
				))
			})

			It("publishes all rows from the given cursor", func() {
				Expect(store.modifications).To(ConsistOf(
					ModificationMatcher(schema, tableOneName).WithBefore(BeNil()).WithAfter(
						MatchAllKeys(Keys{
							"id":  BeAssignableToTypeOf(int64(0)),
							"msg": Equal("woof"),
						}),
					),
					ModificationMatcher(schema, tableOneName).WithBefore(BeNil()).WithAfter(
						MatchAllKeys(Keys{
							"id":  BeAssignableToTypeOf(int64(0)),
							"msg": Equal("roar"),
						}),
					),
				))
			})
		})
	})
})
