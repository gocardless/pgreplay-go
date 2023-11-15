PROG=bin/pgreplay
PROJECT=github.com/gocardless/pgreplay-go
VERSION=$(shell git rev-parse --short HEAD)-dev
BUILD_COMMAND=go build -ldflags "-X main.Version=$(VERSION)"
DB_CONN_CONFIG=-h localhost -p 5432 -U postgres
DOCKER_CONN_CONFIG=-h postgres -p 5432 -U postgres

.PHONY: all darwin linux test clean

all: darwin linux
darwin: $(PROG)
linux: $(PROG:=.linux_amd64)

bin/%.linux_amd64:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(BUILD_COMMAND) -a -o $@ cmd/$*/main.go

bin/%:
	$(BUILD_COMMAND) -o $@ cmd/$*/main.go

createdb:
	psql $(DB_CONN_CONFIG) -d postgres -c "CREATE DATABASE pgreplay_test;"

dropdb:
	psql $(DB_CONN_CONFIG) -c "DROP DATABASE IF EXISTS pgreplay_test;"

structure:
	psql $(DB_CONN_CONFIG) -d pgreplay_test -f pkg/pgreplay/integration/testdata/structure.sql

recreatedb: dropdb createdb structure

createdbdocker:
	psql $(DOCKER_CONN_CONFIG) -c "DROP DATABASE IF EXISTS pgreplay_test;"
	psql $(DOCKER_CONN_CONFIG) -d postgres -c "CREATE DATABASE pgreplay_test;"
	psql $(DOCKER_CONN_CONFIG) -d pgreplay_test -f pkg/pgreplay/integration/testdata/structure.sql

# go get -u github.com/onsi/ginkgo/ginkgo
test:
	ginkgo -v -r

clean:
	rm -rvf $(PROG) $(PROG:%=%.linux_amd64)
