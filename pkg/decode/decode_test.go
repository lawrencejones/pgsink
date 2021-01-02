package decode_test

import (
	"fmt"

	"github.com/lawrencejones/pgsink/pkg/decode"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Decoder", func() {
	mappedOIDs := map[uint32]int{}
	for _, mapping := range decode.Mappings {
		mappedOIDs[mapping.OID] = mappedOIDs[mapping.OID] + 1
	}

	decode.EachMapping(func(name string, mapping decode.TypeMapping) {
		It(fmt.Sprintf("OID %s (%v) has a single mapping", name, mapping.OID), func() {
			Expect(mappedOIDs[mapping.OID]).To(Equal(1), "multiple mappings for OID")
		})
	})
})
