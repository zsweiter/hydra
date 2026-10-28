// Command hydra-scheduler is the scheduler of NATS JetStream
// streams. It exposes no API: it consumes recurrence
// definitions (CloudEvents in binary mode, see
// docs/CONTRACT.md) published under a subject prefix
// (schedule.notification.billing) and publishes their
// occurrences, with the same body, on the subject without it
// (notification.billing). Messages without a recurrence are
// ignored. An optional internal port serves health checks
// and metrics, and subcommands inspect the store (run
// "hydra-scheduler help").
//
// It stores nothing but the schedules: an occurrence is claimed
// once JetStream acknowledged it, and published again, with
// backoff, until it is.
//
// Configuration (environment):
//
//	NATS_URL            nats://localhost:4222
//	NATS_CREDS          credentials file (JWT + NKey)
//	NATS_NKEY_SEED      NKey seed file
//	NATS_USER, NATS_PASSWORD, NATS_TOKEN
//	NATS_TLS_CA         CA bundle to verify the server
//	NATS_TLS_CERT, NATS_TLS_KEY  client certificate (mutual TLS)
//
//	HYDRA_STREAMS       comma-separated streams holding the definitions (required)
//	HYDRA_SUBJECT_PREFIX prefix of the definitions' subjects; a definition on
//	                    <prefix><subject> schedules occurrences on <subject>
//	                    (default: schedule.)
//	HYDRA_SUBJECTS      comma-separated subject filters under the prefix, wildcards
//	                    allowed; each stream is consumed through those in it
//	                    (default: <prefix>>)
//	HYDRA_CONSUMER      durable consumer name (default: hydra-scheduler)
//	HYDRA_DELIVER       new | all, where a new consumer starts (default: new)
//	HYDRA_CONSUMER_WORKERS definitions processed at once, at most, in order per schedule;
//	                    workers start with the load and stop when idle
//	                    (default: 16, 2 with sqlite)
//	HYDRA_CONSUMER_BATCH   definitions stored per write, at most (default: 256)
//	HYDRA_STORE         sqlite:<path> | redis://<host>:<port>/<db> | redis-cluster://<host>:<port>?addr=<host>:<port>
//	                    | postgres://<user>:<pass>@<host>/<db>
//	                    (default: sqlite:hydra.db in the sqlite build, required otherwise);
//	                    redis URLs take ?prefix=<name> to share a server between deployments
//	HYDRA_SYNC_INTERVAL how often instances sharing a store resync (default: 5s with redis and postgres, off otherwise)
//	HYDRA_CATCHUP_BATCH occurrences a catch_up schedule replays per claim (default: 100)
//	HYDRA_WINDOW        how far ahead schedules are held in memory (default: PT1M)
//	HYDRA_WINDOW_LIMIT  schedules loaded at most per window (default: 10000)
//	HYDRA_FIRE_BATCH    due schedules fired together (default: 512)
//	HYDRA_CLAIM_CONCURRENCY concurrent claim round trips per batch (default: 4)
//	HYDRA_HISTORY_TTL   delete ended, cancelled and archived schedules older than this;
//	                    0 keeps them (default: P7D)
//	HYDRA_HTTP_ADDR     internal address for /healthz, /readyz and /metrics (default: off)
//	HYDRA_LOG_FORMAT    json | text (default: json)
//	HYDRA_LOG_LEVEL     debug | info | warn | error (default: info)
//
// Durations are ISO 8601 (PT5S, PT1M, P7D), like the contract's.
//
// Several instances can run against the same Redis (Valkey,
// Dragonfly) or PostgreSQL store: they split the schedules into
// shards, and each occurrence is claimed atomically, so it is
// claimed once even while instances join or leave. SQLite is for
// a single instance.
//
// Each store is compiled in through a build tag (sqlite, redis
// or postgres), so released binaries carry a single store; a
// build without tags includes all of them.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/zsweiter/hydra/bridge"
	natsbroker "github.com/zsweiter/hydra/broker/nats"
	"github.com/zsweiter/hydra/pkg/logger"
	"github.com/zsweiter/hydra/scheduling"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	args := os.Args[1:]
	if len(args) > 0 {
		switch args[0] {
		case "help", "-h", "--help":
			fmt.Println(adminUsage)
			return
		case "version", "--version":
			fmt.Println(version)
			return
		}
		if err := admin(ctx, args, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}

	log, err := newLogger()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := run(ctx, log); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}

func run(ctx context.Context, log logger.Logger) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}

	setMemoryLimit(log)
	if len(cfg.streams) == 0 {
		return errors.New("HYDRA_STREAMS is required")
	}

	store, err := openStore(cfg.store)
	if err != nil {
		return err
	}
	defer store.Close()

	nc, err := nats.Connect(env("NATS_URL", nats.DefaultURL), natsOptions(log)...)
	if err != nil {
		return fmt.Errorf("connect nats: %w", err)
	}
	defer nc.Drain()

	js, err := jetstream.New(nc)
	if err != nil {
		return fmt.Errorf("jetstream: %w", err)
	}

	b := bridge.New(natsbroker.NewPublisher(js), bridge.WithPrefix(cfg.prefix), bridge.WithLogger(log))
	svc, err := scheduling.New(scheduling.Config{
		Store:            store,
		Deliver:          b.Deliver,
		Logger:           log,
		SyncInterval:     cfg.syncInterval,
		CatchUpBatch:     cfg.catchUpBatch,
		Window:           cfg.window,
		WindowLimit:      cfg.windowLimit,
		FireBatch:        cfg.fireBatch,
		ClaimConcurrency: cfg.concurrency,
		HistoryTTL:       cfg.historyTTL,
	})
	if err != nil {
		return err
	}
	b.Register(svc)

	if err := svc.Start(ctx); err != nil {
		return fmt.Errorf("start scheduler: %w", err)
	}
	defer func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := svc.Stop(shutdown); err != nil {
			log.Error(fmt.Errorf("stop scheduler: %w", err))
		}
	}()

	health := &natsbroker.Health{}
	if cfg.httpAddr != "" {
		ops := &opsServer{nc: nc, store: store, svc: svc, bridge: b, consumer: health}
		go ops.serve(ctx, cfg.httpAddr, log)
	}

	log.Info("hydra-scheduler started", logger.Fields{
		"version": version, "streams": cfg.streams, "prefix": cfg.prefix, "subjects": cfg.subjects,
		"sync_interval": cfg.syncInterval.String(), "history_ttl": cfg.historyTTL.String(),
	})

	return natsbroker.Consume(ctx, js, natsbroker.Config{
		Streams:    cfg.streams,
		Subjects:   cfg.subjects,
		Consumer:   cfg.consumer,
		DeliverAll: cfg.deliverAll,
		Workers:    cfg.workers,
		Batch:      cfg.batch,
		Health:     health,
		Logger:     log,
	}, b)
}
