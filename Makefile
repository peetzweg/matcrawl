.PHONY: test build install fmt vet tidy

VERSION ?= dev
LDFLAGS := -X github.com/peetzweg/matcrawl/internal/cli.version=$(VERSION)

test:
	go test ./...

build:
	go build -ldflags "$(LDFLAGS)" -o bin/matcrawl ./cmd/matcrawl

install:
	go install -ldflags "$(LDFLAGS)" ./cmd/matcrawl

fmt:
	gofmt -s -w .

vet:
	go vet ./...

tidy:
	go mod tidy
