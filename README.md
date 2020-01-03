# pg2pubsub [![CircleCI](https://circleci.com/gh/lawrencejones/pg2pubsub.svg?style=svg)](https://circleci.com/gh/lawrencejones/pg2pubsub)

This tool connects to a Postgres database via logical replication, creating the
plumbing required to subscribe to changes on tables within a specific schema.
These changes are then pushed into GCP Pub/Sub.

```
pkg/changelog     // Schema & Modification types, the public output types
pkg/imports       // manages imports for tables in the publication
pkg/logical       // all logical decoding helpers
pkg/migration     // database migrations for internal pg2pubsub tables
pkg/publication   // creates and manages table membership for a Postgres publication
pkg/pubsub        // Pub/Sub relay publisher, batched and acknowledgement hooks
pkg/subscription  // subscribes to publication, streaming Schema/Modification
pkg/util          // utilities that should be in Go's stdlib
```

## Developing

This project comes with a docker-compose development environment. Boot the
environment like so:

```console
$ docker-compose up -d
docker-compose up -d
pg2pubsub_prometheus_1 is up-to-date
pg2pubsub_postgres_1 is up-to-date
pg2pubsub_grafana_1 is up-to-date
```

Then run `make recreatedb` to create a `pg2pubsub_test` database. You can now
access your database like so:

```console
$ psql --host localhost --user pg2pubsub_test pg2pubsub_test
pg2pubsub_test=> \q
```

pg2pubsub will work with this database: try `pg2pubsub --decode-only`.

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
$ createdb pg2pubsub
$ psql pg2pubsub
psql (11.5)
Type "help" for help.

pg2pubsub=# create table example (id bigserial primary key, msg text);
CREATE TABLE

pg2pubsub=# insert into example (msg) values ('hello world');
INSERT 1
```

pg2pubsub will stream these changes from the database and send it to Google's
Pub/Sub service. Two types of message are sent, data and schema. Data messages
contain the inserted/updated/deleted row content in JSON form, with each column
value stored as a string.

Our example would produce the following data message, where `timestamp` is the
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

For some, receiving JSON with string keys will be perfectly suitable for their
use cases. For others, it will be important that schemas are available to later
interpret type information such as numeric precision. We publish Avro schema
definitions via a separate Pub/Sub topic that can be used to parse these data
messages:

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
