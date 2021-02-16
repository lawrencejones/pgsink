package design

import (
	"fmt"
	"io/ioutil"

	. "goa.design/goa/v3/dsl"
)

var _ = API("pgsink", func() {
	Title("pgsink")
	Version("1.0.0")
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

var _ = Service("Web", func() {
	Description("Web static web content for the UI")

	Files("/web/{*filepath}", "web/build")
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
	Attribute("approximate_row_count", Int64, "Table row estimate, using pg_stats", func() {
		Example("There are approximately 100 rows in this table", 100)
	})
	Attribute("publication_status", String, "Status of the publication, set to active when table is streaming", func() {
		Enum("inactive", "active")
		Example("Table is not in publication", "inactive")
		Example("The table is published and is streaming", "active")
	})
	Attribute("import_status", String, "Status of table imports", func() {
		Enum("inactive", "scheduled", "in_progress", "error", "complete", "expired", "unknown")
		Example("Table has never been imported", "inactive")
		Example("Import has been scheduled but no work has been completed", "scheduled")
		Example("Import is underway, not yet complete", "in_progress")
		Example("Import has thrown an error", "error")
		Example("Import has completed successfully", "complete")
		Example("Import has been expired", "expired")
		Example("Import is in an unexpected and unknown state", "unknown")
	})
	Attribute("import_rows_processed_total", Int64, "Last active import rows processed total", func() {
		Example("Processed 5 rows", 5)
	})

	Required(
		"schema",
		"name",
		"approximate_row_count",
		"publication_status",
		"import_status",
		"import_rows_processed_total",
	)
})

var _ = Service("Subscriptions", func() {
	Description("Manage a pgsink subscription")

	HTTP(func() {
		Path("/api/subscriptions")
	})

	Method("Get", func() {
		Description("Get current subscription data")

		Result(Subscription)

		HTTP(func() {
			GET("/current")
			Response(StatusCreated)
		})
	})

	Method("AddTable", func() {
		Description("Add table to publication, relying on an import manager to schedule the job")

		Payload(SubscriptionPublishedTable)

		Result(Subscription)

		HTTP(func() {
			POST("/current/actions/add-table")
			Response(StatusCreated)
		})
	})

	Method("StopTable", func() {
		Description("Stop a table by removing it from the publication, and expiring any import jobs")

		Payload(SubscriptionPublishedTable)

		Result(Subscription)

		HTTP(func() {
			POST("/current/actions/stop-table")
			Response(StatusAccepted)
		})
	})
})

var Subscription = Type("Subscription", func() {
	Description("Subscription configuration")
	Attribute("id", String, "ID of subscription", func() {
		Example("e32ur90j2r")
	})
	Attribute("published_tables", ArrayOf(SubscriptionPublishedTable), "List of published tables")

	Required(
		"id",
		"published_tables",
	)
})

var SubscriptionPublishedTable = Type("SubscriptionPublishedTable", func() {
	Description("Table on subscription that is published")
	Reference(Table)
	Attribute("schema")
	Attribute("name")

	Required(
		"schema",
		"name",
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
	Attribute("rows_processed_total", Int64, "Count of rows processed", func() {
		Example("Processed 5 rows", 5)
	})

	Required(
		"id",
		"subscription_id",
		"schema",
		"table_name",
		"created_at",
		"updated_at",
		"error_count",
		"rows_processed_total",
	)
})
