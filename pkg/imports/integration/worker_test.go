package integration

import (
	"context"
	"fmt"
	"time"

	"github.com/lawrencejones/pgsink/pkg/dbschema/pgsink/model"
	. "github.com/lawrencejones/pgsink/pkg/dbschema/pgsink/table"
	"github.com/lawrencejones/pgsink/internal/dbtest"
	"github.com/lawrencejones/pgsink/pkg/imports"

	. "github.com/go-jet/jet/postgres"
	_ "github.com/jackc/pgx/v4/stdlib"

	. "github.com/onsi/ginkgo"
	_ "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gstruct"
)

var _ = Describe("Worker", func() {
	var (
		ctx    context.Context
		cancel func()
		worker *imports.Worker
	)

	db := dbtest.Configure(
		dbtest.WithSchema("imports_integration_worker"),
	)

	BeforeEach(func() {
		ctx, cancel = db.Setup(context.Background(), 10*time.Second)
		worker = imports.NewWorker(
			logger, db.GetDB(), imports.WorkerOptions{
				SubscriptionID: "subscription",
				PollInterval:   250 * time.Millisecond,
			},
		)
	})

	AfterEach(func() {
		cancel()
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

	Describe(".Start()", func() {
		var (
			job      model.ImportJobs
			importer imports.Importer
		)

		BeforeEach(func() {
			job = insert(model.ImportJobs{
				SubscriptionID: "subscription",
				TableName:      "crackers",
			})
		})

		Context("when import errors", func() {
			BeforeEach(func() {
				importer = noopImporter(fmt.Errorf("awww nahh"))
			})

			It("sets error field on job", func() {
				{
					ctx, cancel := context.WithCancel(ctx)
					cancel()

					worker.Shutdown(ctx) // only allow a single iteration
				}

				Expect(worker.Start(ctx, importer)).To(Succeed(), "start should shutdown peacefully")
				Expect(get(job.ID).Error).To(PointTo(Equal("awww nahh")))
			})
		})
	})

	Describe(".AcquireAndWork()", func() {
		var (
			job      model.ImportJobs
			importer imports.Importer
		)

		BeforeEach(func() {
			importer = noopImporter(nil)
			job = model.ImportJobs{
				SubscriptionID: "subscription",
				TableName:      "crackers",
			}
		})

		JustBeforeEach(func() {
			insert(job)
		})

		It("calls importer with outstanding job", func() {
			workedJob, err := worker.AcquireAndWork(ctx, importer)

			Expect(err).NotTo(HaveOccurred())
			Expect(workedJob.ID).To(Equal(job.ID))
		})

		Context("when another importer has acquired an outstanding job", func() {
			JustBeforeEach(func() {
				acquired, importer := waitImporter(ctx, make(chan struct{}), nil)
				go worker.AcquireAndWork(ctx, importer)
				Eventually(acquired).Should(Receive(MatchFields(IgnoreExtras, Fields{
					"ID": Equal(job.ID),
				})))
			})

			It("does not work the same job", func() {
				workedJob, err := worker.AcquireAndWork(ctx, importer)

				Expect(err).NotTo(HaveOccurred())
				Expect(workedJob).To(BeNil())
			})
		})

		Context("with completed import job", func() {
			BeforeEach(func() {
				job.CompletedAt = func() *time.Time { now := time.Now(); return &now }()
			})

			It("does not import", func() {
				workedJob, err := worker.AcquireAndWork(ctx, importer)

				Expect(err).NotTo(HaveOccurred())
				Expect(workedJob).To(BeNil())
			})
		})

		Context("with expired import job", func() {
			BeforeEach(func() {
				job.ExpiredAt = func() *time.Time { now := time.Now(); return &now }()
			})

			It("does not import", func() {
				workedJob, err := worker.AcquireAndWork(ctx, importer)

				Expect(err).NotTo(HaveOccurred())
				Expect(workedJob).To(BeNil())
			})
		})
	})
})
