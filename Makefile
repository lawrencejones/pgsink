PROG=bin/pg2pubsub
PROJECT=github.com/lawrencejones/pg2pubsub
VERSION=$(shell git rev-parse --short HEAD)-dev
BUILD_COMMAND=go build -ldflags "-X main.Version=$(VERSION)"

.PHONY: all darwin linux createdb test clean pkg/pg2pubsub/integration/testdata/structure.sql

all: darwin linux
darwin: $(PROG)
linux: $(PROG:=.linux_amd64)

bin/%.linux_amd64:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(BUILD_COMMAND) -a -o $@ cmd/$*/main.go

bin/%:
	$(BUILD_COMMAND) -o $@ cmd/$*/main.go

createdb:
	psql postgres -U postgres -c "DROP ROLE IF EXISTS pg2pubsub_test; CREATE ROLE pg2pubsub_test WITH LOGIN CREATEDB REPLICATION;"
	psql postgres -U pg2pubsub_test -c "CREATE DATABASE pg2pubsub_test;"

dropdb:
	psql postgres -c "DROP DATABASE IF EXISTS pg2pubsub_test;"

structure:
	psql pg2pubsub_test -U pg2pubsub_test -f pkg/pg2pubsub/integration/testdata/structure.sql

pkg/pg2pubsub/integration/testdata/structure.sql:
	pg_dump pg2pubsub_test --schema-only > $@

recreatedb: dropdb createdb structure

# go get -u github.com/onsi/ginkgo/ginkgo
test:
	ginkgo -v -r

clean:
	rm -rvf $(PROG) $(PROG:%=%.linux_amd64)
