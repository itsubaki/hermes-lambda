SHELL := /bin/bash
DATE := $(shell date +%Y%m%d-%H:%M:%S)
HASH := $(shell git rev-parse HEAD)
GOVERSION := $(shell go version)
LDFLAGS := -X 'main.date=${DATE}' -X 'main.hash=${HASH}' -X 'main.goversion=${GOVERSION}'

update:
	go get -u ./...
	go mod tidy

.PHONY: test
test:
	go test -cover $(shell go list ./... | grep -v /vendor/ | grep -v /build/) -v
