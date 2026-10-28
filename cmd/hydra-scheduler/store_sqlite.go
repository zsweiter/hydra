//go:build sqlite || !(redis || postgres)

package main

import (
	"strings"

	"github.com/zsweiter/hydra/storage"
	"github.com/zsweiter/hydra/storage/sqlite"
)

func init() {
	defaultStore = "sqlite:hydra.db"
	registerStore(storeDriver{
		name:     "sqlite",
		prefixes: []string{"sqlite:"},
		open: func(dsn string) (storage.ScheduleStore, error) {
			return sqlite.New(strings.TrimPrefix(dsn, "sqlite:"))
		},
	})
}
