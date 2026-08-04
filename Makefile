.PHONY: all build vet test test-integration lint check tidy clean

GO ?= go

# The checkpoint gate. Anything that fails here blocks a stage from being called done.
all: check

check: build vet test lint

build:
	$(GO) build ./...

vet:
	$(GO) vet ./...

# Default suite: hermetic, no Docker, no network. Must stay fast.
test:
	$(GO) test -race ./...

# Redis-backed suite. Requires a running Docker daemon (testcontainers).
test-integration:
	$(GO) test -race -tags=integration -timeout=10m ./...

lint:
	golangci-lint run ./...

tidy:
	$(GO) mod tidy

clean:
	$(GO) clean -testcache
	rm -rf bin dist
