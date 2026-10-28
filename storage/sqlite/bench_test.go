package sqlite

import (
	"path/filepath"
	"testing"

	"github.com/zsweiter/hydra/storage"
	"github.com/zsweiter/hydra/storage/storagetest"
)

// BenchmarkStore runs on a database file, as deployed: an
// in-memory database would hide the cost of syncing to disk.
func BenchmarkStore(b *testing.B) {
	storagetest.Bench(b, func(b *testing.B) storage.Storage {
		s, err := New(filepath.Join(b.TempDir(), "bench.db"))
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { s.Close() })
		return s
	})
}
