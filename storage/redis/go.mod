module github.com/zsweiter/hydra/storage/redis

go 1.26.1

require (
	github.com/redis/go-redis/v9 v9.7.1
	github.com/zsweiter/hydra v0.0.0
)

require (
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/oklog/ulid/v2 v2.1.1 // indirect
)

replace github.com/zsweiter/hydra => ../../
