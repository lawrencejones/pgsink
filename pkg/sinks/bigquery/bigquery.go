// Implements a sink to Google BigQuery, creating a dataset containing raw tables and
// de-duplicated views. The resulting dataset should be queryable as if it were the
// original Postgres database, and should support most queries without change.
package bigquery
