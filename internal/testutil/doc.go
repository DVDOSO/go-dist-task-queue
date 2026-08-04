// Package testutil provides shared fixtures for the integration test suite.
//
// Everything of substance here is behind the `integration` build tag, so that
// the default `go test ./...` neither imports testcontainers nor requires a
// Docker daemon. This file carries no build tag on purpose: without it the
// package would have zero files in a normal build and `go build ./...` would
// fail with "build constraints exclude all Go files".
package testutil
