//go:build postgres || !(sqlite || redis)

package main

import (
	"github.com/zsweiter/hydra/storage"
	"github.com/zsweiter/hydra/storage/postgres"
)

func init() {
	registerStore(storeDriver{
		name:     "postgres",
		prefixes: []string{"postgres://", "postgresql://"},
		shared:   true,
		open: func(dsn string) (storage.ScheduleStore, error) {
			return postgres.New(dsn)
		},
	})
}
