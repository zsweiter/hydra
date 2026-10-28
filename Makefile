.PHONY: help fmt lint fix test test-all test-integration bench bench-stores loadtest \
	scheduler scheduler-binaries scheduler-images clean

MODULES := . storage/sqlite storage/redis storage/postgres broker/nats cmd/hydra-scheduler contract/cetest

# hydra-scheduler build settings. One binary and image per store
# (redis also covers Valkey and Dragonfly). aarch64 is arm64 in
# Go and Docker platform names.
VERSION    ?= 0.2.0
IMAGE      ?= hydra-scheduler
STORES     ?= sqlite redis postgres
PLATFORMS  ?= linux/amd64,linux/arm64
BIN        := bin
DOCKERFILE := cmd/hydra-scheduler/Dockerfile
TAGS       := timetzdata netgo osusergo
LDFLAGS    := -s -w -X main.version=$(VERSION)
BUILDX     := docker buildx build -f $(DOCKERFILE) --build-arg VERSION=$(VERSION)

help:
	@echo "make scheduler            one binary per store for this machine: $(BIN)/hydra-scheduler-<store>"
	@echo "make scheduler-STORE      just one, e.g. make scheduler-redis (sqlite needs a C compiler)"
	@echo "make scheduler-binaries   static linux amd64 + arm64 binaries per store in $(BIN)/linux_<arch>/ (Docker)"
	@echo "make scheduler-images     multi-arch image per store, $(IMAGE):$(VERSION)-<store>; PUSH=1 pushes them"
	@echo "                          STORES, PLATFORMS, VERSION and IMAGE can be overridden"
	@echo "make test | test-all | test-integration | bench | fmt | lint | fix | clean"
	@echo "make bench-stores         storage benchmarks on SQLite, Valkey, Dragonfly and PostgreSQL (Docker)"
	@echo "make loadtest             fire a peak of LOADTEST schedules due at once (default 1000000)"

fmt:
	gofmt -w .

lint:
	golangci-lint run ./...

fix:
	go fix ./...

test:
	go test -race ./... -v -short

# Unit tests of every module. Redis, Postgres and NATS tests skip
# unless REDIS_ADDR / POSTGRES_DSN / NATS_URL are set;
# test-integration runs them against throwaway containers.
test-all:
	@for m in $(MODULES); do (cd $$m && go vet ./... && go test -race ./...) || exit 1; done

# Throwaway services for the integration tests and store
# benchmarks. The Valkey cluster uses the host network so its
# nodes announce addresses the tests can reach.
CLUSTER_PORTS := 17000 17001 17002
REDIS_ENV     := REDIS_ADDR=localhost:16379 REDIS_CLUSTER_ADDRS=127.0.0.1:17000,127.0.0.1:17001,127.0.0.1:17002
POSTGRES_ENV  := POSTGRES_DSN="postgres://postgres:hydra@localhost:15432/hydra?sslmode=disable"

define services_up
	@docker run -d --rm --name hydra-it-valkey -p 16379:6379 valkey/valkey:9.1.2-alpine >/dev/null
	@docker run -d --rm --name hydra-it-dragonfly -p 16380:6379 docker.dragonflydb.io/dragonflydb/dragonfly:v1.34.2 --proactor_threads=2 >/dev/null
	@docker run -d --rm --name hydra-it-postgres -p 15432:5432 -e POSTGRES_PASSWORD=hydra -e POSTGRES_DB=hydra postgres:17.11-alpine >/dev/null
	@docker run -d --rm --name hydra-it-nats -p 14222:4222 nats:2.15.1-alpine -js >/dev/null
	@for p in $(CLUSTER_PORTS); do docker run -d --rm --name hydra-it-cluster-$$p --network host valkey/valkey:9.1.2-alpine \
		valkey-server --port $$p --cluster-enabled yes --cluster-config-file nodes-$$p.conf --save "" >/dev/null; done
	@sleep 1; docker exec hydra-it-cluster-17000 valkey-cli --cluster create $(foreach p,$(CLUSTER_PORTS),127.0.0.1:$(p)) --cluster-replicas 0 --cluster-yes >/dev/null
	@for i in $$(seq 30); do docker exec hydra-it-cluster-17000 valkey-cli -p 17000 cluster info | grep -q cluster_state:ok && break; sleep 1; done
	@for i in $$(seq 30); do docker exec hydra-it-postgres pg_isready -U postgres -d hydra -h 127.0.0.1 >/dev/null 2>&1 && break; sleep 1; done
endef

SERVICES := hydra-it-valkey hydra-it-dragonfly hydra-it-postgres hydra-it-nats $(addprefix hydra-it-cluster-,$(CLUSTER_PORTS))

# Starts Valkey, a Valkey cluster, Dragonfly, PostgreSQL and NATS
# in Docker, runs the tests that need them (the Redis store
# against all three Redis flavours), and removes the containers.
test-integration:
	$(services_up)
	@status=0; \
	(cd storage/redis && $(REDIS_ENV) go test -race -count=1 ./...) || status=1; \
	(cd storage/redis && REDIS_ADDR=localhost:16380 go test -race -count=1 ./...) || status=1; \
	(cd storage/postgres && $(POSTGRES_ENV) go test -race -count=1 ./...) || status=1; \
	(cd broker/nats && NATS_URL=nats://localhost:14222 go test -race -count=1 ./...) || status=1; \
	docker rm -f $(SERVICES) >/dev/null; \
	exit $$status

bench:
	go test -run '^$$' -bench=. ./...

# Storage benchmarks (storagetest.Bench) on every store. ns/op
# is per job or occurrence; FirePeak* is the whole scheduler.
BENCH_N ?= 5000x
bench-stores:
	$(services_up)
	@status=0; \
	(cd storage/sqlite && go test -run '^$$' -bench Store -benchtime $(BENCH_N) .) || status=1; \
	(cd storage/redis && echo "valkey:" && REDIS_ADDR=localhost:16379 go test -run '^$$' -bench Store -benchtime $(BENCH_N) .) || status=1; \
	(cd storage/redis && echo "dragonfly:" && REDIS_ADDR=localhost:16380 go test -run '^$$' -bench Store -benchtime $(BENCH_N) .) || status=1; \
	(cd storage/postgres && $(POSTGRES_ENV) go test -run '^$$' -bench Store -benchtime $(BENCH_N) .) || status=1; \
	docker rm -f $(SERVICES) >/dev/null; \
	exit $$status

# The scheduler alone (memory store, direct delivery): LOADTEST
# schedules all due at the same instant, plus heap and memory
# per schedule.
LOADTEST ?= 1000000
loadtest:
	HYDRA_LOADTEST=$(LOADTEST) go test -run '^$$' -bench 'Fire|Heap|WindowMemory' -benchtime 1x -timeout 30m ./internal/scheduler/

# Native builds with the host toolchain, static like the image
# so they run on any Linux of the same architecture. Only the
# SQLite build uses CGO.
scheduler: $(addprefix scheduler-,$(STORES))

scheduler-sqlite:
	CGO_ENABLED=1 go build -trimpath -tags "$(TAGS) sqlite sqlite_omit_load_extension" \
		-ldflags "$(LDFLAGS) -linkmode external -extldflags -static" \
		-o $(BIN)/hydra-scheduler-sqlite ./cmd/hydra-scheduler

scheduler-redis scheduler-postgres: scheduler-%:
	CGO_ENABLED=0 go build -trimpath -tags "$(TAGS) $*" -ldflags "$(LDFLAGS)" \
		-o $(BIN)/hydra-scheduler-$* ./cmd/hydra-scheduler

# Cross-compiled in Docker (tonistiigi/xx, no QEMU needed), so
# no local cross toolchain is required for arm64.
scheduler-binaries:
	@for s in $(STORES); do \
		$(BUILDX) --build-arg STORE=$$s --platform $(PLATFORMS) --target binary --output type=local,dest=$(BIN) . || exit 1; \
	done

# Tags $(IMAGE):$(VERSION)-<store> and $(IMAGE):<store>. Loading
# a multi-arch image locally needs Docker's containerd image
# store; otherwise build with PUSH=1 and IMAGE set to a registry
# repository.
scheduler-images:
	@for s in $(STORES); do \
		$(BUILDX) --build-arg STORE=$$s --platform $(PLATFORMS) \
			-t $(IMAGE):$(VERSION)-$$s -t $(IMAGE):$$s $(if $(PUSH),--push,--load) . || exit 1; \
	done

clean:
	rm -rf $(BIN)
