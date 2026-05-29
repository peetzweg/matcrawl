.PHONY: test build install fmt vet tidy

VERSION ?= dev
LDFLAGS := -X github.com/peetzweg/matcrawl/internal/cli.version=$(VERSION)
# goolm: pure-Go Olm implementation used by mautrix/crypto. Keeps CGO_ENABLED=0
# working, matching the family-wide pure-Go discipline (see MATCRAWL.md §10).
TAGS := goolm

test:
	go test -tags '$(TAGS)' ./...

build:
	go build -tags '$(TAGS)' -ldflags "$(LDFLAGS)" -o bin/matcrawl ./cmd/matcrawl

install:
	go install -tags '$(TAGS)' -ldflags "$(LDFLAGS)" ./cmd/matcrawl

fmt:
	gofmt -s -w .

vet:
	go vet -tags '$(TAGS)' ./...

tidy:
	go mod tidy
