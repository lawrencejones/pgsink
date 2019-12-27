PROG=bin/pg2pubsub
PROJECT=github.com/lawrencejones/pg2pubsub
VERSION=$(shell git rev-parse --short HEAD)-dev
BUILD_COMMAND=go build -ldflags "-X main.Version=$(VERSION)"
PGFLAGS=--host localhost --port 5432

.PHONY: prog darwin linux createdb test clean pkg/pg2pubsub/integration/testdata/structure.sql

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
	psql $(PGFLAGS) postgres -U postgres -c "DROP ROLE IF EXISTS pg2pubsub_test; CREATE ROLE pg2pubsub_test WITH LOGIN CREATEDB REPLICATION;"
	psql $(PGFLAGS) postgres -U pg2pubsub_test -c "CREATE DATABASE pg2pubsub_test;"

dropdb:
	psql $(PGFLAGS) -U postgres postgres -c "DROP DATABASE IF EXISTS pg2pubsub_test;"

structure:
	psql $(PGFLAGS) pg2pubsub_test -U pg2pubsub_test -f pkg/pg2pubsub/integration/testdata/structure.sql

pkg/pg2pubsub/integration/testdata/structure.sql:
	pg_dump $(PGFLAGS) pg2pubsub_test --schema-only > $@

recreatedb: dropdb createdb structure

# go get -u github.com/onsi/ginkgo/ginkgo
test:
	ginkgo -v -r

clean:
	rm -rvf $(PROG) $(PROG:%=%.darwin_amd64) $(PROG:%=%.linux_amd64)
