PROG=bin/pg2sink
PROJECT=github.com/lawrencejones/pg2sink
VERSION=$(shell git rev-parse --short HEAD)-dev
BUILD_COMMAND=go build -ldflags "-s -w -X main.Version=$(VERSION)"
PSQL=docker-compose exec -T postgres psql
PGDUMP=docker-compose exec -T postgres pg_dump
DATABASE=pg2sink

.PHONY: prog darwin linux createdb test clean

prog: $(PROG)
darwin: $(PROG:=.darwin_amd64)
linux: $(PROG:=.linux_amd64)

bin/%.linux_amd64:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(BUILD_COMMAND) -a -o $@ cmd/$*/main.go

bin/%.darwin_amd64:
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 $(BUILD_COMMAND) -a -o $@ cmd/$*/main.go

bin/%:
	$(BUILD_COMMAND) -o $@ cmd/$*/main.go

createdb:
	$(PSQL) postgres -U postgres -c "DROP ROLE IF EXISTS $(DATABASE); CREATE ROLE $(DATABASE) WITH LOGIN CREATEDB REPLICATION;"
	$(PSQL) postgres -U $(DATABASE) -c "CREATE DATABASE $(DATABASE);"

dropdb:
	$(PSQL) -U postgres postgres -c "DROP DATABASE IF EXISTS $(DATABASE);"

recreatedb: dropdb createdb

# go get -u github.com/onsi/ginkgo/ginkgo
test:
	ginkgo -v -r

clean:
	rm -rvf $(PROG) $(PROG:%=%.darwin_amd64) $(PROG:%=%.linux_amd64)
