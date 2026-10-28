package postgres

import (
	"testing"

	"github.com/zsweiter/hydra/storage"
	"github.com/zsweiter/hydra/storage/storagetest"
)

func BenchmarkStore(b *testing.B) {
	storagetest.Bench(b, func(b *testing.B) storage.Storage { return newDB(b) })
}

func BenchmarkStoreRelaxedCommit(b *testing.B) {
	storagetest.Bench(b, func(b *testing.B) storage.Storage { return newDB(b, WithRelaxedCommit()) })
}
