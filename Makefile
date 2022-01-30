PROG=bin/pgsink
PROJECT=github.com/lawrencejones/pgsink
VERSION=$(shell git rev-parse --short HEAD)-dev
BUILD_COMMAND=go build -ldflags "-s -w -X main.Version=$(VERSION)"
DOCKER_COMPOSE=docker-compose --env-file=/dev/null
PSQL=$(DOCKER_COMPOSE) exec -T postgres psql
PGDUMP=$(DOCKER_COMPOSE) exec -T postgres pg_dump

# Override these for different configurations
export PGSUPERUSER ?= postgres
export PGHOST ?= localhost
export PGDATABASE ?= pgsink
export PGUSER ?= pgsink

.PHONY: prog darwin linux generate clean
.PHONY: psql migrate migrate-run structure.sql createdb dropdb recreatedb test docs
.PHONY: api/gen internal/dbschema openapi-generator.jar clients/typescript

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

# Installs development tools from tools.go
tools:
	go mod download \
		&& cat tools.go | grep _ | awk -F'"' '{print $$2}' | xargs -tI % go install %

psql:
	psql

psql-test: PGUSER=pgsink_test
psql-test: PGDATABASE=pgsink_test
psql-test: psql

# Runs migrations against the ambient Postgres credentials
migrate: migrate-run structure.sql

migrate-run:
	go run internal/migration/cmd/goose.go --install up

migrate-run-test: PGUSER=pgsink_test
migrate-run-test: PGDATABASE=pgsink_test
migrate-run-test: migrate-run

# Generates a structure.sql from the docker-compose database, having run migrate
structure.sql:
	$(PGDUMP) -U postgres $(PGDATABASE) --schema-only --schema=pgsink >$@

createdb:
	$(PSQL) postgres -U postgres -c "CREATE ROLE $(PGUSER) WITH LOGIN CREATEDB REPLICATION;"
	$(PSQL) postgres -U $(PGUSER) -c "CREATE DATABASE $(PGDATABASE);"
	$(PSQL) $(PGDATABASE) -U postgres -c 'CREATE EXTENSION IF NOT EXISTS "uuid-ossp";'

createdb-test: PGUSER=pgsink_test
createdb-test: PGDATABASE=pgsink_test
createdb-test: createdb

dropdb:
	$(PSQL) -U postgres postgres -c "DROP DATABASE IF EXISTS $(PGDATABASE);"
	$(PSQL) -U postgres postgres -c "DROP ROLE IF EXISTS $(PGUSER);"

recreatedb: dropdb createdb

# go get -u github.com/onsi/ginkgo/ginkgo
test:
	PGUSER=pgsink_test PGDATABASE=pgsink_test ginkgo -r pkg

docs:
	swagger serve --port=4000 api/gen/http/openapi.json

################################################################################
# Codegen
################################################################################

# Generate API code, from server to client and service stubs
api/gen:
	goa gen github.com/lawrencejones/pgsink/api/design -o api

# Generates database types from live Postgres schema (start docker-compose for
# this)
internal/dbschema:
	jet -source=PostgreSQL -host=localhost -port=5432 -user=$(PGUSER) -dbname=$(PGDATABASE) -schema=pgsink -path=tmp/dbschema
	jet -source=PostgreSQL -host=localhost -port=5432 -user=$(PGUSER) -dbname=$(PGDATABASE) -schema=pg_catalog -path=tmp/dbschema
	jet -source=PostgreSQL -host=localhost -port=5432 -user=$(PGUSER) -dbname=$(PGDATABASE) -schema=information_schema -path=tmp/dbschema
	rm -rf $@
	mv tmp/dbschema/pgsink $@

############################################################
# Our clients, for other languages
############################################################

# We can't use the version installed by brew as this generates a different
# client format to what is integrated so we must use v5 or later.
openapi-generator-cli.jar:
	curl https://repo1.maven.org/maven2/org/openapitools/openapi-generator-cli/5.0.0-beta3/openapi-generator-cli-5.0.0-beta3.jar \
		--output $@
