# pgsink [![CircleCI](https://circleci.com/gh/lawrencejones/pgsink.svg?style=svg)](https://circleci.com/gh/lawrencejones/pgsink) [![Go Reference](https://pkg.go.dev/badge/github.com/lawrencejones/pgsink.svg)](https://pkg.go.dev/github.com/lawrencejones/pgsink)

> **Path to v1.0.0: https://github.com/lawrencejones/pgsink/projects/1**

> **Draft docs can be seen at: [docs](https://github.com/lawrencejones/pgsink/tree/docs/docs)**

pgsink is a Postgres change-capture device that supports high-throughput and
low-latency capture to a variety of sinks.

You'd use this project if your primary database is Postgres and you want a
stress-free, quick-to-setup and easy-to-operate tool to replicate your data to
other stores such as BigQuery or Elasticsearch, which works with any size
Postgres database.

## Similar projects

[debezium]: https://github.com/debezium/debezium
[dblog]: https://netflixtechblog.com/dblog-a-generic-change-data-capture-framework-69351fb9099b

There are many change-capture projects out there, and several support Postgres.

As an example, pgsink is similar to [debezium][debezium] in performance and
durability goals, but with a much simpler setup (no Kafka required). We also
bear similarity to Netflix's [dblog][dblog], with the benefit of being
open-source and available for use.

We win in these comparisons when you want a simple, no additional dependencies
setup. We also benefit from the sole focus on Postgres instead of many upstream
sources, as we can optimise our data-access pattern for large, high-transaction
volume Postgres databases. Examples of this are keeping transactions short to
help vacuums, and traversing tables using efficient indexes.

This makes pgsink a much safer bet for people managing production critical
Postgres databases.

## Developing

As an overview of important packages, for those understanding the source code:

- `changelog` the input to sinks, produced by subscription or import
- `decode` configures decoding of Postgres types into Golang (text -> int64)
- `imports` create, manage and work import jobs, producing changelog entries
- `logical` parsing of the pgoutput logical encoding, used by subscription
- `sinks` implements different types of sink, from files to Google BigQuery
- `subscription` Postgres change capture via replication, generating a changelog

This project comes with a docker-compose development environment. Boot the
environment like so:

```console
$ docker-compose up -d
docker-compose up -d
pgsink_prometheus_1 is up-to-date
pgsink_postgres_1 is up-to-date
pgsink_grafana_1 is up-to-date
```

Then run `make recreatedb` to create a `pgsink` database. You can now access
your database like so:

```console
$ psql --host localhost --user pgsink pgsink
pgsink=> \q
```

pgsink will work with this database: try `pgsink --sink=file --decode-only`.

### Database migrations

We use [goose](https://github.com/pressly/goose) to run database migrations. Create
new migrations like so:

```console
$ go run internal/migration/cmd/goose.go --dir internal/migration create create_import_jobs_table go
2019/12/29 14:59:51 Created new file: internal/migration/20191229145951_create_import_jobs_table.go
```

Running migrations is done using the make target:

```console
$ make migrate structure.sql
$ go run internal/migration/cmd/goose.go --install up
2021/01/09 15:38:29 requested --install, so creating schema 'pgsink'
2021/01/09 15:38:29 goose: no migrations to run. current version: 20210102200953
docker-compose --env-file=/dev/null exec -T postgres pg_dump -U postgres pgsink --schema-only --schema=pgsink >structure.sql
```

## Getting started

Boot a Postgres database, then create an example table.

```console
$ createdb pgsink
$ psql pgsink
psql (11.5)
Type "help" for help.

pgsink=# create table public.example (id bigserial primary key, msg text);
CREATE TABLE

pgsink=# insert into public.example (msg) values ('hello world');
INSERT 1
```

pgsink will stream these changes from the database and send it to the
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
