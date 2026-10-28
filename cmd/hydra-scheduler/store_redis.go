//go:build redis || !(sqlite || postgres)

package main

import (
	"net/url"
	"strings"

	goredis "github.com/redis/go-redis/v9"
	"github.com/zsweiter/hydra/storage"
	"github.com/zsweiter/hydra/storage/redis"
)

// Valkey and Dragonfly speak the Redis protocol and use the
// same URLs. redis-cluster:// connects to a Redis (or Valkey)
// Cluster: the URL names one node and ?addr= the others.
func init() {
	registerStore(storeDriver{
		name:     "redis",
		prefixes: []string{"redis://", "rediss://", "redis-cluster://", "rediss-cluster://"},
		shared:   true,
		open: func(dsn string) (storage.ScheduleStore, error) {
			dsn, prefix, err := takeParam(dsn, "prefix")
			if err != nil {
				return nil, err
			}

			if scheme, rest, ok := strings.Cut(dsn, "-cluster://"); ok {
				opts, err := goredis.ParseClusterURL(scheme + "://" + rest)
				if err != nil {
					return nil, err
				}
				return redis.Use(goredis.NewClusterClient(opts), redis.WithPrefix(prefix)), nil
			}

			opts, err := goredis.ParseURL(dsn)
			if err != nil {
				return nil, err
			}
			return redis.Use(goredis.NewClient(opts), redis.WithPrefix(prefix)), nil
		},
	})
}

// takeParam removes a query parameter the client does not know
// from dsn and returns its value.
func takeParam(dsn, name string) (string, string, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return "", "", err
	}
	q := u.Query()
	value := q.Get(name)
	q.Del(name)
	u.RawQuery = q.Encode()
	return u.String(), value, nil
}
