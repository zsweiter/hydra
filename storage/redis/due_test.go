package redis

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/zsweiter/hydra/storage"
	"github.com/zsweiter/hydra/storage/storagetest"
)

func TestConformance(t *testing.T) {
	storagetest.Run(t, func(t *testing.T) storage.Storage { return newDB(t) })
}

// TestConformanceCluster runs the suite on a Redis Cluster
// named by REDIS_CLUSTER_ADDRS (comma-separated): every key of
// a prefix shares one hash slot, so the scripts and
// transactions are accepted.
func TestConformanceCluster(t *testing.T) {
	addrs := os.Getenv("REDIS_CLUSTER_ADDRS")
	if addrs == "" {
		t.Skip("REDIS_CLUSTER_ADDRS not set")
	}

	storagetest.Run(t, func(t *testing.T) storage.Storage {
		rdb := redis.NewClusterClient(&redis.ClusterOptions{Addrs: strings.Split(addrs, ",")})
		if err := rdb.Ping(context.Background()).Err(); err != nil {
			t.Skipf("cluster not available: %v", err)
		}
		t.Cleanup(func() { rdb.Close() })
		// A prefix per test keeps tests apart without flushing.
		return Use(rdb, WithPrefix(fmt.Sprintf("hydra-test-%d", time.Now().UnixNano())))
	})
}
