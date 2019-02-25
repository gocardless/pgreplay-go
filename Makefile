PROG=bin/pgreplay
PROJECT=github.com/gocardless/pgreplay-go
VERSION=$(shell git rev-parse --short HEAD)-dev
BUILD_COMMAND=go build -ldflags "-X main.Version=$(VERSION)"

.PHONY: all darwin linux test clean

all: darwin linux
darwin: $(PROG)
linux: $(PROG:=.linux_amd64)

bin/%.linux_amd64:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(BUILD_COMMAND) -a -o $@ cmd/$*/main.go

bin/%:
	$(BUILD_COMMAND) -o $@ cmd/$*/main.go

createdb:
	psql postgres -c "DROP GROUP IF EXISTS pgreplay_test_users; CREATE GROUP pgreplay_test_users WITH LOGIN CREATEDB;"
	psql postgres -U pgreplay_test_users -c "CREATE DATABASE pgreplay_test;"
	psql pgreplay_test -c "DROP ROLE IF EXISTS alice; CREATE ROLE alice LOGIN;"
	psql pgreplay_test -c "DROP ROLE IF EXISTS bob;   CREATE ROLE bob   LOGIN;"
	psql pgreplay_test -c "ALTER GROUP pgreplay_test_users ADD USER alice, bob;"

dropdb:
	psql postgres -c "DROP DATABASE IF EXISTS pgreplay_test;"

structure:
	psql pgreplay_test -U pgreplay_test_users -f pkg/pgreplay/integration/testdata/structure.sql

recreatedb: dropdb createdb structure

# go get -u github.com/onsi/ginkgo/ginkgo
test:
	ginkgo -v -r

clean:
	rm -rvf $(PROG) $(PROG:%=%.linux_amd64)
