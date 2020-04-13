# pg2sink [![CircleCI](https://circleci.com/gh/lawrencejones/pg2sink.svg?style=svg)](https://circleci.com/gh/lawrencejones/pg2sink)

This tool connects to a Postgres database via logical replication, creating the
plumbing required to subscribe to changes on tables within a specific schema.
These changes are then pushed into one of the supported sink destinations.

```
pkg/changelog     // Schema & Modification types, the public output types
pkg/models        // Internal database models, such as ImportJob
pkg/imports       // manages imports for subscribed tables
pkg/logical       // all logical decoding helpers
pkg/migration     // database migrations for internal pg2sink tables
pkg/publication   // creates and manages table membership for a Postgres publication
pkg/sinks         // output destinations, all implementing a Sink interface
pkg/subscription  // subscribes to publication, streaming Schema/Modification
pkg/util          // utilities that should be in Go's stdlib
```

## Developing

This project comes with a docker-compose development environment. Boot the
environment like so:

```console
$ docker-compose up -d
docker-compose up -d
pg2sink_prometheus_1 is up-to-date
pg2sink_postgres_1 is up-to-date
pg2sink_grafana_1 is up-to-date
```

Then run `make recreatedb` to create a `pg2sink` database. You can now access
your database like so:

```console
$ psql --host localhost --user pg2sink pg2sink
pg2sink=> \q
```

pg2sink will work with this database: try `pg2sink --sink=file --decode-only`.

### Database migrations

We use [goose](github.com/pressly/goose) to run database migrations. Create new
migrations like so:

```console
$ goose -dir pkg/migration create create_import_jobs_table go
2019/12/29 14:59:51 Created new file: pkg/migration/20191229145951_create_import_jobs_table.go
```

## Getting started

Boot a Postgres database, then create an example table.

```console
$ createdb pg2sink
$ psql pg2sink
psql (11.5)
Type "help" for help.

pg2sink=# create table public.example (id bigserial primary key, msg text);
CREATE TABLE

pg2sink=# insert into public.example (msg) values ('hello world');
INSERT 1
```

pg2sink will stream these changes from the database and send it to the
configured sink. Changes are expressed as a stream of messages, either a
`Schema` that describes the structure of a Postgres table, or a `Modification`
corresponding to an insert/update/delete of a row in Postgres.

Our example would produce the following modification, where `timestamp` is the
time at which the change was committed and `sequence` the operation index within
the transaction:

```json
{
  "timestamp": "2019-10-04T16:05:55.123456+01:00",
  "sequence": 1,
  "namespace": "public",
  "name": "example",
  "before": null,
  "after": {
    "id": "1",
    "msg": "hello world"
  }
}
```

Also sent, arriving before the modification element, will be a schema entry that
describes the `public.example` table. We represent these as Avro schemas, built
from the Postgres catalog information.

```json
{
  "timestamp": "2019-10-04T16:05:55.123456+01:00",
  "schema": {
    "namespace": "public.example",
    "type": "record",
    "name": "value",
    "fields": [
      {
        "name": "id",
        "type": ["long", "null"],
        "default": null
      },
      {
        "name": "msg",
        "type": ["string", "null"],
        "default": null
      }
    ]
  }
}
```

Schemas are published whenever we first discover a relation. Use the timestamp
field to order each successive schema event to ensure stale messages don't
override more recent data.
