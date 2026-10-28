package domain

import (
	"fmt"
	"testing"
)

// Patterned IDs, e.g. a subject and an id sharing digits, must
// still spread over the owners.
func TestScheduleShard_SpreadsPatternedIDs(t *testing.T) {
	patterns := []string{"e2e.job.%02[1]d|s%02[1]d", "s%02d", "tenant-%d", "%d"}
	for _, pattern := range patterns {
		for _, owners := range []int{2, 3, 4} {
			count := make([]int, owners)
			for i := range 400 {
				count[ScheduleShard(fmt.Sprintf(pattern, i))%owners]++
			}
			for o, n := range count {
				if want := 400 / owners; n < want*2/3 || n > want*4/3 {
					t.Errorf("%q over %d owners: owner %d got %d of 400 (%v)", pattern, owners, o, n, count)
				}
			}
		}
	}
}

func members(n int) []string {
	m := make([]string, n)
	for i := range m {
		m[i] = fmt.Sprintf("01JINSTANCE%02d", i)
	}
	return m
}

func TestOwnedShards_Partition(t *testing.T) {
	for n := 1; n <= 5; n++ {
		live := members(n)
		seen := make(map[int]int)
		for _, m := range live {
			shards := OwnedShards(m, live)
			if n == 1 && shards != nil {
				t.Fatalf("a single member must own all shards (nil), got %d", len(shards))
			}
			if n > 1 && (len(shards) < ShardCount/n/2 || len(shards) > ShardCount/n*2) {
				t.Errorf("%d members: %s owns %d shards", n, m, len(shards))
			}
			for _, s := range shards {
				seen[s]++
			}
		}
		if n > 1 && len(seen) != ShardCount {
			t.Fatalf("%d members cover %d shards, want %d", n, len(seen), ShardCount)
		}
		for s, c := range seen {
			if c != 1 {
				t.Fatalf("shard %d owned %d times", s, c)
			}
		}
	}
}

// A member joining takes shards only from the others; none
// move between the members already there.
func TestOwnedShards_MinimalMovement(t *testing.T) {
	before := members(4)
	after := members(5)

	owner := func(live []string) map[int]string {
		o := make(map[int]string)
		for _, m := range live {
			for _, s := range OwnedShards(m, live) {
				o[s] = m
			}
		}
		return o
	}

	was, now := owner(before), owner(after)
	moved := 0
	for s := range ShardCount {
		if was[s] != now[s] {
			if now[s] != after[4] {
				t.Fatalf("shard %d moved from %s to %s, not to the new member", s, was[s], now[s])
			}
			moved++
		}
	}
	if moved == 0 || moved > ShardCount/5*2 {
		t.Fatalf("%d shards moved to the new member, want about %d", moved, ShardCount/5)
	}
}
