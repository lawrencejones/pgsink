PROG=bin/pgsink
PROJECT=github.com/lawrencejones/pgsink
VERSION=$(shell git rev-parse --short HEAD)-dev
BUILD_COMMAND=go build -ldflags "-s -w -X main.Version=$(VERSION)"
DOCKER_COMPOSE=docker-compose --env-file=/dev/null
PSQL=$(DOCKER_COMPOSE) exec -T postgres psql
PGDUMP=$(DOCKER_COMPOSE) exec -T postgres pg_dump

# Override these for different configurations
PGSUPERUSER ?= postgres
PGHOST ?= localhost
PGDATABASE ?= pgsink
PGUSER ?= pgsink

.PHONY: prog darwin linux generate clean
.PHONY: migrate migrate-run structure.sql createdb dropdb recreatedb test docs internal/dbschema

################################################################################
# Build
################################################################################

prog: $(PROG)
darwin: $(PROG:=.darwin_amd64)
linux: $(PROG:=.linux_amd64)

bin/%.linux_amd64:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(BUILD_COMMAND) -a -o $@ cmd/$*/*.go

bin/%.darwin_amd64:
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 $(BUILD_COMMAND) -a -o $@ cmd/$*/*.go

bin/%:
	$(BUILD_COMMAND) -o $@ cmd/$*/*.go

generate:
	go generate ./...

clean:
	rm -rfv $(PROG)

################################################################################
# Development
################################################################################

# Runs migrations against the ambient Postgres credentials
migrate: migrate-run structure.sql

migrate-run:
	go run internal/migration/cmd/goose.go --install up

# Generates a structure.sql from the docker-compose database, having run migrate
structure.sql:
	$(PGDUMP) -U postgres $(PGDATABASE) --schema-only --schema=pgsink >$@

createdb:
	$(PSQL) postgres -U postgres -c "CREATE ROLE $(PGUSER) WITH LOGIN CREATEDB REPLICATION;"
	$(PSQL) postgres -U $(PGUSER) -c "CREATE DATABASE $(PGDATABASE);"
	$(PSQL) $(PGDATABASE) -U postgres -c 'CREATE EXTENSION IF NOT EXISTS "uuid-ossp";'

dropdb:
	$(PSQL) -U postgres postgres -c "DROP DATABASE IF EXISTS $(PGDATABASE);"
	$(PSQL) -U postgres postgres -c "DROP ROLE IF EXISTS $(PGUSER);"

recreatedb: dropdb createdb

# go get -u github.com/onsi/ginkgo/ginkgo
test:
	PGUSER=pgsink_test PGDATABASE=pgsink_test ginkgo -r pkg

# Generates database types from live Postgres schema (start docker-compose for
# this)
internal/dbschema:
	jet -source=PostgreSQL -host=localhost -port=5432 -user=$(PGUSER) -dbname=$(PGDATABASE) -schema=pgsink -path=internal/dbschema
