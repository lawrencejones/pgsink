PROG=bin/pg2pubsub
PROJECT=github.com/lawrencejones/pg2pubsub
VERSION=$(shell git rev-parse --short HEAD)-dev
BUILD_COMMAND=go build -ldflags "-X main.Version=$(VERSION)"
PSQL=docker-compose exec -T postgres psql
PGDUMP=docker-compose exec -T postgres pg_dump

.PHONY: prog darwin linux createdb test clean pkg/pg2pubsub/integration/testdata/structure.sql structure

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
	$(PSQL) postgres -U postgres -c "DROP ROLE IF EXISTS pg2pubsub_test; CREATE ROLE pg2pubsub_test WITH LOGIN CREATEDB REPLICATION;"
	$(PSQL) postgres -U pg2pubsub_test -c "CREATE DATABASE pg2pubsub_test;"

dropdb:
	$(PSQL) -U postgres postgres -c "DROP DATABASE IF EXISTS pg2pubsub_test;"

structure:
	cat pkg/pg2pubsub/integration/testdata/structure.sql | $(PSQL) pg2pubsub_test -U pg2pubsub_test -f -

pkg/pg2pubsub/integration/testdata/structure.sql:
	$(PGDUMP) -U postgres pg2pubsub_test --schema-only | sed 's/$$//' > $@

recreatedb: dropdb createdb structure

# go get -u github.com/onsi/ginkgo/ginkgo
test:
	ginkgo -v -r

clean:
	rm -rvf $(PROG) $(PROG:%=%.darwin_amd64) $(PROG:%=%.linux_amd64)
