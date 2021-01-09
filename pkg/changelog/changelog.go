// Defines a changelog Entry, which is a sum type of Schema or Modification. A stream of
// entries is provided as input to sinks, which export the data to the destination target.
//
// Use pkg/subsciption to generate a changelog from a logical replication connection, or
// pkg/imports to load changes from existing Postgres data.
package changelog
