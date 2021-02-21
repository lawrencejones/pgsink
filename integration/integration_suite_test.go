package integration

import (
	"context"
	"fmt"
	"net"
	"os/exec"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/hashicorp/go-cleanhttp"
	"github.com/lawrencejones/pgsink/api"
	"github.com/lawrencejones/pgsink/api/gen/subscriptions"
	"github.com/lawrencejones/pgsink/api/gen/tables"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
	. "github.com/onsi/gomega/gstruct"
)

func Start(ctx context.Context, binaryArgs []string) *Harness {
	var serveAddress string
	{
		listener, err := net.Listen("tcp", ":0")
		Expect(err).NotTo(HaveOccurred())
		serveAddress = fmt.Sprintf(
			"127.0.0.1:%d", listener.Addr().(*net.TCPAddr).Port)
	}

	args := []string{
		"stream",
		"--serve",
		"--serve.address", serveAddress,
		"--import-worker-count", "1",
		"--import-worker.batch-limit", "10",
		"--import-worker.buffer-size", "10",
		"--import-worker.poll-interval", "3s",
		"--import-manager",
		"--import-manager.poll-interval", "3s",
	}
	command := exec.Command(binary, append(args, binaryArgs...)...)

	By("starting pgsink")
	session, err := gexec.Start(command, GinkgoWriter, GinkgoWriter)
	Expect(err).NotTo(HaveOccurred())

	client, err := api.NewHTTPClient(fmt.Sprintf("http://%s", serveAddress),
		cleanhttp.DefaultClient())
	Expect(err).NotTo(HaveOccurred())

	By(fmt.Sprintf("connecting to control-plane API on %s", serveAddress))
	Eventually(func() error {
		ctx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
		defer cancel()

		By("poll API")
		_, err := client.Health.Check(ctx)
		return err
	}, 15*time.Second, 5*time.Second).Should(Succeed())

	return &Harness{session, client}
}

type Harness struct {
	*gexec.Session
	*api.Client
}

func (h Harness) NewShortTimeout(ctx context.Context) context.Context {
	ctx, _ = context.WithTimeout(ctx, 250*time.Millisecond)
	return ctx
}

func (h Harness) ExpectTableToExist(ctx context.Context, schema, name string) {
	By(fmt.Sprintf("check table is returned by API: %s.%s", schema, name))
	tables, err := h.Client.Tables.List(h.NewShortTimeout(ctx), &tables.ListPayload{Schema: schema})
	Expect(err).NotTo(HaveOccurred())
	Expect(tables).To(ContainElement(
		PointTo(MatchFields(IgnoreExtras, Fields{
			"Schema": Equal(schema), "Name": Equal(name),
		})),
	))
}

func (h Harness) GetTable(ctx context.Context, schema, name string) *tables.Table {
	tables, err := h.Client.Tables.List(h.NewShortTimeout(ctx), &tables.ListPayload{Schema: schema})
	Expect(err).NotTo(HaveOccurred())

	for _, table := range tables {
		if table.Schema == schema && table.Name == name {
			return table
		}
	}

	return nil
}

func (h Harness) WaitUntilTableStatusIs(ctx context.Context, schema, name string, publicationStatus, importStatus string) {
	giveUpAfter := time.After(150 * time.Second)
	var table *tables.Table

	for {
		select {
		case <-giveUpAfter:
			Expect(table).To(PointTo(MatchFields(IgnoreExtras, Fields{
				"ImportStatus":      Equal(importStatus),
				"PublicationStatus": Equal(publicationStatus),
			})), "timed out waiting for table status")

		case <-time.After(5 * time.Second):
			table := h.GetTable(ctx, schema, name)
			By(fmt.Sprintf("polled API, table: %s", spew.Sdump(table)))
			if table.PublicationStatus == publicationStatus && table.ImportStatus == importStatus {
				return
			}
		}
	}
}

func (h Harness) AddTableToSubscription(ctx context.Context, schema, name string) {
	By("adding table one to subscription")
	_, err := h.Client.Subscriptions.AddTable(ctx, &subscriptions.SubscriptionPublishedTable{
		Schema: schema,
		Name:   name,
	})
	Expect(err).NotTo(HaveOccurred())
}

func (h Harness) Shutdown() {
	By("requesting shutdown")
	h.Terminate()
	Eventually(h.Session, 15*time.Second).Should(gexec.Exit(0))
}
