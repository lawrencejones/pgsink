# Architecture

pgsink is designed to extract data from Postgres into various destinations,
called sinks. In particular, it focuses on:

- **Simplicity**: by default, everything will just work
- **Performance**: even extremely high volume or large Postgres databases should
  be streamable, without impact to existing database work
- **Durability**: no update should be lost

In order to meet these goals, we adopt a modular architecture that ensures each
component can be run and (where necessary) scaled independently.

## Subscription

[pg-logical]: https://www.postgresql.org/docs/12/logical-replication.html
[pg-publication]: https://www.postgresql.org/docs/12/logical-replication-publication.html
[pg-subscription]: https://www.postgresql.org/docs/12/logical-replication-subscription.html
[pg-replication-slots]: https://www.postgresql.org/docs/12/logical-replication-subscription.html#LOGICAL-REPLICATION-SUBSCRIPTION-SLOT

Postgres has a concept of a subscription, a high-level abstraction which models
an on-going stream of changes. Subscriptions are powered by a publication, which
specifies what tables to subscribe to, and a replication slot which tracks what
changes have been consumed.

`pkg/subscription` provides a `Subscription` interface that emulates the native
Postgres construct. It manages the lifecycle of the underlying
publication/replication slot, and hooks into our import functionality to provide
the equivilent of the Postgres `copy_data` subscription parameter.

To understand how everything works, we should first cover the PostgreSQL
concepts involved in logical replication, then how subscriptions are managed and
the lifecycle around adding & removing tables.

## PostgreSQL

Before understanding our constructs, it's useful to cover each piece of a
native Postgres logical replication setup. Full details can be found in [Chapter
30: Logical Replication][pg-logical] of the Postgres docs, but in summary:

- [Publications][pg-publication] are configured with the tables to be tracked
- [Replication slots][pg-replication-slots] are cursors into the Postgres WAL
  that instruct the primary to prevent deletion of unconsumed changes

Just as Postgres does, we tie the publication and replication slot together via
the `pkg/subscription.Subscription` interface. By coupling the creation of a
publication and associated replication slot, we can detect when changes require
tables to be imported, and ensure we track the location in the Postgres WAL that
we started our subscription with.

## Publications

The publication is created first, and begins with no tables. By default, the
publication will be called `pgsink`, but you should name your publication to
match the sink you'll be exporting data to (eg, `pgsink_bigquery` or
`pgsink_elasticsearch`).

Publications are stamped with uuids. These IDs are used to construct the
replication slot name in Postgres, ensuring we get a fresh slot for each new
publication.

## Replication Slots

Replication slots are created immediately after the associated publication, and
the internal API (`pkg/subscription.Create`) discourages creating them
separately. This is to ensure no component acts on a subscription until the
replication slot is created, as Postgres isn't guaranteed to keep subscription
WAL until the slot is in-place.

When streaming changes from Postgres, we confirm receipt of WAL only once the
underlying sink has confirmed the change has been flushed to durable storage.
Postgres won't delete WAL until we've acknowledged the sink has received it,
which helps ensure data durability.

## Table Lifecycle

Subscriptions are configured on a table-by-table basis. It's important to handle
adding and removing tables with care, to ensure data durability.

### Adding

To start receiving changes for a table, we need to add it to our publication.
This causes logical consumers to receive changes to the table, but only those
that happen after it has been added to the publication.

You can add tables to a subscription via the commandline (TODO) or use a
`pkg/subscription.Manager` with a set of include/exclude rules to automatically
enroll tables in a given schema to the subscription. Users can choose whatever
suits them.

### Importing

To provide sinks with an entire tables contents, we support import jobs that
scan the whole table and push each row into the sink. Import jobs are scheduled
by the `pkg/imports.Manager`, a process that continually scans the publication
for tables which have no associated active import, and creates it an import job
in the queue.

By running a separate manager process to schedule imports, as opposed to hooks
in the code to add tables to a publication, we continually ensure all published
tables are imported even in the face of temporary errors. This design makes it
easy to re-schedule imports, useful whenever a sink wants to reprocess a tables
contents, just by marking the active import job for that table as having
expired.

If you have a high-volume database, or want to prevent contention over your
sink, it's advised to run import workers in a process separate from the stream
(TODO).

### Removing

TODO
