package bigquery

import (
	"fmt"
	"reflect"

	"github.com/lawrencejones/pgsink/pkg/decode"
	"github.com/lawrencejones/pgsink/pkg/decode/gen/mappings"
	. "github.com/onsi/ginkgo"
	_ "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
	_ "github.com/onsi/gomega/gstruct"
)

// The aim of this test is to ensure anything that can be decoded by the default mappings
// can be supported by the BigQuery sink. Future work will likely push decoding
// constraints back into the decoder, but for now we want to support all the things.
var _ = Describe("fieldTypeFor", func() {
	mappings.Each(func(name string, mapping decode.TypeMapping) {
		It(fmt.Sprintf("OID %s (%v) is supported", name, mapping.OID), func() {
			_, _, err := fieldTypeFor(reflect.ValueOf(mapping.NewEmpty()).Elem().Interface())
			Expect(err).NotTo(HaveOccurred())
		})
	})
})
