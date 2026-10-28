package postgres

import (
	"testing"

	"github.com/zsweiter/hydra/storage"
	"github.com/zsweiter/hydra/storage/storagetest"
)

func TestConformance(t *testing.T) {
	storagetest.Run(t, func(t *testing.T) storage.Storage { return newDB(t) })
}

// Relaxed commits change durability, not behaviour.
func TestConformanceRelaxedCommit(t *testing.T) {
	storagetest.Run(t, func(t *testing.T) storage.Storage { return newDB(t, WithRelaxedCommit()) })
}
