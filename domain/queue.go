package domain

type (
	QueueName string
)

func (q QueueName) Hash() uint64 {
	const offset uint64 = 14695981039346656037
	const prime uint64 = 1099511628211

	h := offset

	for i := 0; i < len(q); i++ {
		h ^= uint64(q[i])
		h *= prime
	}

	h ^= h >> 30
	h *= 0xbf58476d1ce4e5b9
	h ^= h >> 27
	h *= 0x94d049bb133111eb
	h ^= h >> 31

	return h
}

func (q QueueName) Shard(shards uint64) uint64 {
	if shards == 0 {
		panic("shards must be greater than zero")
	}

	return q.Hash() % shards
}

func (q QueueName) Bit() uint64 {
	return q.Hash() & 63
}

func (q QueueName) String() string {
	return string(q)
}
