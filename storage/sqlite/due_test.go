package sqlite

import (
	"path/filepath"
	"testing"

	"github.com/zsweiter/hydra/storage"
	"github.com/zsweiter/hydra/storage/storagetest"
)

func TestConformance(t *testing.T) {
	storagetest.Run(t, func(t *testing.T) storage.Storage { return newDB(t) })
}

// On a file, reads go through the read-only pool: the suite
// checks they see every committed write.
func TestConformanceFile(t *testing.T) {
	storagetest.Run(t, func(t *testing.T) storage.Storage {
		s, err := New(filepath.Join(t.TempDir(), "hydra.db"))
		if err != nil {
			t.Fatal(err)
		}

		t.Cleanup(func() { s.Close() })
		return s
	})
}
