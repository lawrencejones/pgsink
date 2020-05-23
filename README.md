# pg2sink [![CircleCI](https://circleci.com/gh/lawrencejones/pg2sink.svg?style=svg)](https://circleci.com/gh/lawrencejones/pg2sink)

> **Path to v1.0.0: https://github.com/lawrencejones/pg2sink/projects/1**

> **Draft docs can be seen at: [docs](https://github.com/lawrencejones/pg2sink/tree/docs/docs)**

[debezium]: https://github.com/debezium/debezium
[dblog]: https://netflixtechblog.com/dblog-a-generic-change-data-capture-framework-69351fb9099b

pg2sink is a Postgres change-capture device that supports high-throughput and
low-latency capture to a variety of sinks.

You'd use this project if your primary data-store is Postgres and you want a
stress-free, quick-to-setup and easy-to-operate way of replicating your data to
other stores such as BigQuery or Elasticsearch, that will work with any size
Postgres database.

## Similar projects

There are a lot of change-capture projects available, and many support
Postgres.

As an example, we are similar to [debezium][debezium] in performance and
durability goals, but have a much simpler setup (no Kafka required). We also
bear similarity to Netflix's [dblog][dblog], with the benefit of being
open-source and available for use.

In these comparisons, pg2sink wins with a much simpler setup- there's no Kafka
involved, or any secondary data-sources. We also benefit from the sole focus on
Postgres over many upstream sources, as we can optimise our data-access pattern
for large, high-transaction volume databases. Examples of this are keeping
transactions short to aid vacuums, and traversing tables using efficient
indexes.

This makes pg2sink a much safer bet for people managing production critical
Postgres databases.

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
