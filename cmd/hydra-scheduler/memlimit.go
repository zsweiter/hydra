package main

import (
	"os"
	"runtime/debug"
	"strconv"
	"strings"

	"github.com/zsweiter/hydra/pkg/logger"
)

// setMemoryLimit gives the runtime a soft memory limit of 90% of
// the container's (cgroup v2 or v1), unless GOMEMLIMIT sets one.
// With it the GC runs less often while memory is plentiful and
// harder only near the limit, instead of being killed past it.
// Go sizes GOMAXPROCS to the container's CPU, not its memory.
func setMemoryLimit(log logger.Logger) {
	if os.Getenv("GOMEMLIMIT") != "" {
		return
	}
	for _, path := range []string{
		"/sys/fs/cgroup/memory.max",                   // v2
		"/sys/fs/cgroup/memory/memory.limit_in_bytes", // v1
	} {
		raw, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		limit, err := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)
		// "max" (v2) or a huge number (v1) mean no limit.
		if err != nil || limit <= 0 || limit >= 1<<60 {
			return
		}

		soft := limit / 10 * 9
		debug.SetMemoryLimit(soft)
		log.Info("memory limit set from the container", logger.Fields{"container_bytes": limit, "limit_bytes": soft})
		return
	}
}
