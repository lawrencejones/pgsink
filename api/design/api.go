package design

import (
	"fmt"
	"io/ioutil"

	. "goa.design/goa/v3/dsl"
)

var _ = API("pgsink", func() {
	Title("pgsink")
	Server("api", func() {
		Host("direct", func() {
			Description("Direct access to an instance")
			URI("http://localhost:8000")
		})
	})

	// This will only work when we run from the repo root, as we do when running `make
	// api/gen`.
	desc, err := ioutil.ReadFile("api/design/description.md")
	if err != nil {
		panic(fmt.Sprintf("could not find description markdown: %s", err.Error()))
	}
	Description(string(desc))
})

var _ = Service("Health", func() {
	Description("Provide service health information")

	HTTP(func() {
		Path("/health")
	})

	Method("Check", func() {
		Description("Health check for probes")

		Result(func() {
			Attribute("status", String, "Status of the API", func() {
				Enum("healthy")
			})
			Required("status")
		})

		HTTP(func() {
			GET("/check")
		})
	})
})
