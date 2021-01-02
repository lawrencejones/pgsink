package bigquery

import (
	"fmt"

	"github.com/lawrencejones/pgsink/pkg/decode"
	. "github.com/onsi/ginkgo"
	_ "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
	_ "github.com/onsi/gomega/gstruct"
)

var _ = Describe("fieldTypeFor", func() {
	decode.EachMapping(func(name string, mapping decode.TypeMapping) {
		It(fmt.Sprintf("OID %s (%v) is supported", name, mapping.OID), func() {
			_, _, err := fieldTypeFor(mapping.OID)
			Expect(err).NotTo(HaveOccurred())
		})
	})
})
