package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/lawrencejones/pgsink/internal/dbtest"
	"github.com/lawrencejones/pgsink/pkg/changelog"

	. "github.com/onsi/ginkgo"
	_ "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
	_ "github.com/onsi/gomega/gstruct"
)

var _ = Describe("File", func() {
	var (
		ctx     context.Context
		modFile *os.File
		harness *Harness
		cancel  func()
	)

	var (
		schema       = "file_integration_test"
		tableOneName = "one"
		tableTwoName = "two"
	)

	db := dbtest.Configure(
		dbtest.WithSchema(schema),
		dbtest.WithTable(schema, tableOneName, "id bigserial primary key", "msg text"),
		dbtest.WithTable(schema, tableTwoName, "id bigserial", "msg text", "yesno boolean"),
	)

	BeforeEach(func() {
		ctx, cancel = db.Setup(context.Background(), 5*time.Minute)
	})

	AfterEach(func() {
		if harness != nil {
			harness.Shutdown()
		}

		cancel()
	})

	JustBeforeEach(func() {
		By("creating temporary file")
		var err error
		modFile, err = ioutil.TempFile("", "pgsink-integration-")
		Expect(err).NotTo(HaveOccurred())

		harness = Start(ctx, []string{
			"--sink=file",
			"--sink.file.modifications-path", modFile.Name(),
		})
	})

	// getModifications is a really horrible method for JSON parsing the output of the file
	// sink. It has to so some strange string splitting as the file isn't valid JSON, but
	// JSON chunks that are newline delimited.
	getModifications := func() ([]changelog.Modification, error) {
		data, err := ioutil.ReadFile(modFile.Name())
		if err != nil {
			return nil, err
		}

		var modifications []changelog.Modification
		for _, entry := range bytes.Split(data, []byte("\n}")) {
			entry := append(entry, []byte("}")...)
			var mod changelog.Modification
			if err := json.Unmarshal(entry, &mod); err != nil {
				continue
			}

			modifications = append(modifications, mod)
		}

		return modifications, err
	}

	It("streams to file", func() {
		By("insert data into table one")
		db.MustExec(ctx, fmt.Sprintf(`insert into %s (id, msg) values (1, 'meow');`, tableOneName))
		db.MustExec(ctx, fmt.Sprintf(`insert into %s (id, msg) values (2, 'woof');`, tableOneName))

		harness.ExpectTableToExist(ctx, schema, tableOneName)
		harness.ExpectTableToExist(ctx, schema, tableTwoName)

		harness.AddTableToSubscription(ctx, schema, tableOneName)
		harness.WaitUntilTableStatusIs(ctx, schema, tableOneName, "active", "complete")

		By("checking table contents was imported")
		{
			entries, err := getModifications()
			Expect(err).NotTo(HaveOccurred())

			var afters []map[string]interface{}
			for _, entry := range entries {
				afters = append(afters, entry.After.(map[string]interface{}))
			}

			Expect(afters).To(ConsistOf(
				map[string]interface{}{
					"id": float64(1), "msg": "meow",
				},
				map[string]interface{}{
					"id": float64(2), "msg": "woof",
				},
			))
		}
	})
})
