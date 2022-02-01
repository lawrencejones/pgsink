// +build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"time"

	bq "cloud.google.com/go/bigquery"

	"github.com/davecgh/go-spew/spew"
	. "github.com/onsi/ginkgo"
	_ "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
	_ "github.com/onsi/gomega/gstruct"
)

var _ = Describe("BigQuery", func() {
	var (
		ctx = context.Background()
	)

	It("streams to BigQuery", func() {
		cfg := struct {
			ProjectID string
			Dataset   string
			Location  string
		}{
			os.Getenv("PGSINK_INTEGRATION_BIGQUERY_PROJECT_ID"),
			os.Getenv("PGSINK_INTEGRATION_BIGQUERY_DATASET"),
			os.Getenv("PGSINK_INTEGRATION_BIGQUERY_LOCATION"),
		}

		By(fmt.Sprintf("connecting to BigQuery: %s", spew.Sdump(cfg)))
		client, err := bq.NewClient(ctx, cfg.ProjectID)
		Expect(err).NotTo(HaveOccurred(), "failed to create a BigQuery client")
		dataset := client.Dataset(cfg.Dataset)

		command := exec.Command(binary,
			"stream",
			"--import-worker-count", "1",
			"--import-worker.batch-limit", "10",
			"--import-worker.buffer-size", "10",
			"--sink=bigquery",
			"--sink.bigquery.project-id", cfg.ProjectID,
			"--sink.bigquery.dataset", cfg.Dataset,
			"--sink.bigquery.location", cfg.Location,
			"--sink.bigquery.instrument",
			"--sink.bigquery.flush-interval=3s",
		)

		By("starting pgsink")
		session, err := gexec.Start(command, GinkgoWriter, GinkgoWriter)
		Expect(err).NotTo(HaveOccurred())

		By("checking BigQuery dataset is ready")
		var md *bq.DatasetMetadata
		Eventually(func() (err error) {
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			By("poll dataset")
			md, err = dataset.Metadata(ctx)
			return
		}).Should(Succeed())

		By(fmt.Sprintf("dataset is ready: %s", spew.Sdump(md)))
		// TODO

		By("requesting psgink shutdown")
		session.Terminate()
		Eventually(session, 15*time.Second).Should(gexec.Exit(0))
	})
})
