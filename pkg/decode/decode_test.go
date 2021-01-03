package decode_test

import (
	"github.com/jackc/pgtype"
	"github.com/lawrencejones/pgsink/pkg/decode"
	"github.com/lawrencejones/pgsink/pkg/decode/gen/mappings"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Decoder", func() {
	var (
		decoder decode.Decoder
	)

	JustBeforeEach(func() {
		decoder = decode.NewDecoder(mappings.Mappings)
	})

	Context("with array types", func() {
		// This needs to be replaced with comprehensive tests for each of the Postgres types,
		// pulling them from the database and asserting that they match the empty values we
		// expect. For now, it shows how we can use the scanners to achieve parsing.
		It("can decode into Golang native slices", func() {
			typeMapping, err := decoder.TypeMappingForOID(pgtype.TextArrayOID)
			Expect(err).NotTo(HaveOccurred())

			// Use pgtype.TextArray to generate a text encoded array
			var raw []byte
			{
				arr := new(pgtype.TextArray)
				arr.Set([]string{"peek", "a", "boo"})
				raw, err = arr.EncodeText(nil, []byte{})
				Expect(err).NotTo(HaveOccurred())
			}

			dest, scanner := typeMapping.NewEmpty(), typeMapping.NewScanner()

			Expect(scanner.Scan(raw)).To(Succeed())
			Expect(scanner.AssignTo(dest)).To(Succeed())
			Expect(dest).To(BeEquivalentTo(&[]string{"peek", "a", "boo"}))
		})
	})
})
