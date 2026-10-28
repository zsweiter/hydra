package domain

import "hash/fnv"

// ShardCount is the number of virtual shards schedules are
// spread over. Instances sharing a storage split the shards
// between them, so each one loads and fires only its share.
const ShardCount = 256

// ScheduleShard returns the shard a schedule belongs to. It is
// stable across processes, so every instance agrees on it.
func ScheduleShard(id string) int {
	h := fnv.New32a()
	h.Write([]byte(id))

	return int(fmix32(h.Sum32()) % ShardCount)
}

// fmix32 is the MurmurHash3 finalizer. FNV mixes its low bits
// poorly (the lowest is the parity of all bytes, so IDs such as
// "job.37|s37" would all land on even shards); the finalizer
// spreads every input bit over the shard bits.
func fmix32(h uint32) uint32 {
	h ^= h >> 16
	h *= 0x85ebca6b
	h ^= h >> 13
	h *= 0xc2b2ae35
	h ^= h >> 16

	return h
}

// OwnedShards returns the shards owned by member among the
// live members, by rendezvous (highest random weight) hashing:
// each shard goes to the member scoring highest for it. When a
// member joins or leaves, only the shards it gains or loses
// move (about 1/n of them), so the others keep their windows
// loaded. A single member, or one not listed, owns them all
// (nil).
func OwnedShards(member string, members []string) []int {
	if len(members) <= 1 {
		return nil
	}

	seeds := make([]uint64, len(members))
	self := -1
	for i, m := range members {
		seeds[i] = fnv64a(m)
		if m == member {
			self = i
		}
	}
	if self < 0 {
		return nil
	}

	shards := make([]int, 0, ShardCount/len(members)+1)
	for s := range ShardCount {
		best, bestScore := 0, uint64(0)
		for i, seed := range seeds {
			// Ties are broken by member order, which every
			// instance sees sorted the same way.
			if score := mix64(seed ^ uint64(s)*0x9e3779b97f4a7c15); i == 0 || score > bestScore {
				best, bestScore = i, score
			}
		}
		if best == self {
			shards = append(shards, s)
		}
	}

	return shards
}

func fnv64a(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}

// mix64 is the splitmix64 finalizer.
func mix64(h uint64) uint64 {
	h ^= h >> 30
	h *= 0xbf58476d1ce4e5b9
	h ^= h >> 27
	h *= 0x94d049bb133111eb
	h ^= h >> 31

	return h
}
