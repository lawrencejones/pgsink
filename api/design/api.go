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
		Path("/api/health")
	})

	Method("Check", func() {
		Description("Health check for probes")

		Result(func() {
			Attribute("status", String, "Status of the API", func() {
				Enum("healthy")
			})
			Required(
				"status",
			)
		})

		HTTP(func() {
			GET("/check")
		})
	})
})

var _ = Service("Tables", func() {
	Description("Expose Postgres tables, and their import/sync status")

	HTTP(func() {
		Path("/api/tables")
	})

	Method("List", func() {
		Description("List all tables")

		Payload(func() {
			Attribute("schema", String, func() {
				Description("Comma separated list of Postgres schemas to filter by")
				Example("public")
				Example("public,payments")
				Default("public")
			})
		})

		Result(ArrayOf(Table))

		HTTP(func() {
			GET("/")
			Response(StatusOK)
			Params(func() {
				Param("schema")
			})
		})
	})
})

var Table = Type("Table", func() {
	Description("Postgres table, with sync and import status")
	Attribute("schema", String, "Postgres table schema", func() {
		Example("public")
	})
	Attribute("name", String, "Postgres table name", func() {
		Example("payments")
	})
	Attribute("published", Boolean, "True if this table is already streaming", func() {
		Example("The table is active on the Postgres publication", true)
		Example("Table is not being streamed", false)
	})

	Required(
		"schema",
		"name",
		"published",
	)
})

var _ = Service("Imports", func() {
	Description("Manage table imports, scoped to the server subscription ID")

	HTTP(func() {
		Path("/api/imports")
	})

	Method("List", func() {
		Description("List all imports")

		Result(ArrayOf(Import))

		HTTP(func() {
			GET("/")
			Response(StatusOK)
		})
	})
})

var Import = Type("Import", func() {
	Description("Import job for a Postgres table")
	Attribute("id", Int, "Unique ID for the import", func() {
		Example(3)
	})
	Attribute("subscription_id", String, "Subscription ID, associating this import to a specific subscription", func() {
		Format(FormatUUID)
	})
	Attribute("schema", String, "Postgres table schema", func() {
		Example("public")
	})
	Attribute("table_name", String, "Postgres table name", func() {
		Example("payments")
	})
	Attribute("completed_at", String, "Import was completed at this time", func() {
		Format(FormatDateTime)
	})
	Attribute("created_at", String, "Import was created at this time", func() {
		Format(FormatDateTime)
	})
	Attribute("updated_at", String, "Import was last updated at this time", func() {
		Format(FormatDateTime)
	})
	Attribute("expired_at", String, "Import was expired at this time", func() {
		Format(FormatDateTime)
	})
	Attribute("error", String, "Last import error", func() {
		Example("failed to parse primary key")
	})
	Attribute("error_count", Int, "Count of error attempts", func() {
		Example("Failed each of the last two times it was run", 2)
	})
	Attribute("last_error_at", String, "Timestamp of last error, only reset on error", func() {
		Format(FormatDateTime)
	})

	Required(
		"id",
		"subscription_id",
		"schema",
		"table_name",
		"created_at",
		"updated_at",
		"error_count",
	)
})
