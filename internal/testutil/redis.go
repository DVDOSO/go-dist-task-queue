//go:build integration

package testutil

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

// RedisImage is pinned so that local runs and CI exercise the same server.
// Redis 7 is required: these scripts call TIME, and effects replication being
// the default from 7 onwards is what makes a non-deterministic command legal
// inside a script that also writes.
const RedisImage = "redis:7-alpine"

// StartRedis boots a Redis container and returns a client for it plus a
// termination function.
//
// Intended to be called once per test binary from TestMain rather than per
// test: a container costs roughly 2.7s to start, which is tolerable once and
// not tolerable thirty times. Tests isolate themselves with distinct key
// prefixes instead, which also lets them run in parallel.
func StartRedis(ctx context.Context) (client *redis.Client, terminate func(), err error) {
	ctr, err := tcredis.Run(ctx, RedisImage)
	if err != nil {
		return nil, nil, fmt.Errorf("start redis container: %w", err)
	}

	stop := func() {
		if err := testcontainers.TerminateContainer(ctr); err != nil {
			fmt.Printf("warning: terminate redis container: %v\n", err)
		}
	}

	endpoint, err := ctr.Endpoint(ctx, "")
	if err != nil {
		stop()
		return nil, nil, fmt.Errorf("redis endpoint: %w", err)
	}

	c := redis.NewClient(&redis.Options{Addr: endpoint})
	if err := c.Ping(ctx).Err(); err != nil {
		_ = c.Close()
		stop()
		return nil, nil, fmt.Errorf("ping redis: %w", err)
	}

	return c, func() {
		_ = c.Close()
		stop()
	}, nil
}

// Prefix derives a Redis key namespace unique to one test, so that tests
// sharing a container cannot see each other's keys and may run in parallel.
func Prefix(t *testing.T) string {
	t.Helper()
	safe := strings.NewReplacer("/", "_", " ", "_", "#", "_").Replace(t.Name())
	return "tq_test:" + safe
}
