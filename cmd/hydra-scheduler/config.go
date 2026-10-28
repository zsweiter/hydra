package main

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/zsweiter/hydra/contract"
	"github.com/zsweiter/hydra/pkg/logger"
	"github.com/zsweiter/hydra/storage"
)

type config struct {
	streams      []string
	prefix       string
	subjects     []string
	consumer     string
	deliverAll   bool
	workers      int
	batch        int
	store        string
	syncInterval time.Duration
	httpAddr     string
	catchUpBatch int
	window       time.Duration
	windowLimit  int
	fireBatch    int
	concurrency  int
	historyTTL   time.Duration
}

func loadConfig() (config, error) {
	cfg := config{
		streams:    splitList(os.Getenv("HYDRA_STREAMS")),
		prefix:     env("HYDRA_SUBJECT_PREFIX", contract.DefinitionPrefix),
		subjects:   splitList(os.Getenv("HYDRA_SUBJECTS")),
		consumer:   os.Getenv("HYDRA_CONSUMER"),
		deliverAll: env("HYDRA_DELIVER", "new") == "all",
		store:      env("HYDRA_STORE", defaultStore),
		httpAddr:   os.Getenv("HYDRA_HTTP_ADDR"),
	}

	// Definitions are consumed under the prefix only, so the
	// scheduler never reads back its own occurrences.
	if !strings.HasSuffix(cfg.prefix, ".") || strings.ContainsAny(cfg.prefix, "*> \t") || strings.HasPrefix(cfg.prefix, ".") {
		return cfg, fmt.Errorf("HYDRA_SUBJECT_PREFIX must be literal subject tokens ending in a dot (e.g. schedule.), got %q", cfg.prefix)
	}
	if len(cfg.subjects) == 0 {
		cfg.subjects = []string{cfg.prefix + ">"}
	}
	for _, s := range cfg.subjects {
		if !strings.HasPrefix(s, cfg.prefix) {
			return cfg, fmt.Errorf("HYDRA_SUBJECTS: %s is outside HYDRA_SUBJECT_PREFIX %s", s, cfg.prefix)
		}
	}

	if cfg.store == "" {
		return cfg, fmt.Errorf("HYDRA_STORE is required; this build supports: %s", supportedStores())
	}

	// Stores shared by several instances resync by default.
	// SQLite has a single writer, so more than two workers only
	// queue for it.
	defaultSync, defaultWorkers := "0", "2"
	if d := findDriver(cfg.store); d != nil && d.shared {
		defaultSync, defaultWorkers = "PT5S", "16"
	}

	var err error
	ints := []struct {
		name, fallback string
		dst            *int
	}{
		{"HYDRA_CONSUMER_WORKERS", defaultWorkers, &cfg.workers},
		{"HYDRA_CONSUMER_BATCH", "256", &cfg.batch},
		{"HYDRA_CATCHUP_BATCH", "100", &cfg.catchUpBatch},
		{"HYDRA_WINDOW_LIMIT", "10000", &cfg.windowLimit},
		{"HYDRA_FIRE_BATCH", "512", &cfg.fireBatch},
		{"HYDRA_CLAIM_CONCURRENCY", "4", &cfg.concurrency},
	}
	for _, v := range ints {
		if *v.dst, err = strconv.Atoi(env(v.name, v.fallback)); err != nil || *v.dst <= 0 {
			return cfg, fmt.Errorf("%s must be a positive integer", v.name)
		}
	}

	durations := []struct {
		name, fallback string
		dst            *time.Duration
		positive       bool
	}{
		{"HYDRA_SYNC_INTERVAL", defaultSync, &cfg.syncInterval, false},
		{"HYDRA_WINDOW", "PT1M", &cfg.window, true},
		{"HYDRA_HISTORY_TTL", "P7D", &cfg.historyTTL, false},
	}
	for _, v := range durations {
		d, err := parseDuration(env(v.name, v.fallback))
		if err != nil || d < 0 || (v.positive && d == 0) {
			return cfg, fmt.Errorf("%s must be a non-negative ISO 8601 duration (e.g. PT5S, P7D)", v.name)
		}
		*v.dst = d
	}

	return cfg, nil
}

// parseDuration reads an ISO 8601 duration, like the contract,
// or a Go one ("5s"); "0" is zero.
func parseDuration(s string) (time.Duration, error) {
	if s == "0" {
		return 0, nil
	}
	if d, err := contract.ParseDuration(s); err == nil {
		return d, nil
	}
	return time.ParseDuration(s)
}

// newLogger builds the process logger from HYDRA_LOG_FORMAT
// (json, the default, or text) and HYDRA_LOG_LEVEL.
func newLogger() (logger.Logger, error) {
	var level slog.Level
	if err := level.UnmarshalText([]byte(env("HYDRA_LOG_LEVEL", "info"))); err != nil {
		return nil, fmt.Errorf("HYDRA_LOG_LEVEL: %w", err)
	}
	opts := &slog.HandlerOptions{Level: level}

	var handler slog.Handler
	switch format := env("HYDRA_LOG_FORMAT", "json"); format {
	case "json":
		handler = slog.NewJSONHandler(os.Stdout, opts)
	case "text":
		handler = slog.NewTextHandler(os.Stdout, opts)
	default:
		return nil, fmt.Errorf("HYDRA_LOG_FORMAT must be json or text, got %q", format)
	}

	return logger.NewSlog(slog.New(handler)), nil
}

// natsOptions builds connection options from the environment.
// Credentials and TLS files are read by the NATS client.
func natsOptions(log logger.Logger) []nats.Option {
	log = log.With(logger.Fields{"component": "nats"})
	opts := []nats.Option{
		nats.Name("hydra-scheduler"),
		nats.MaxReconnects(-1),
		nats.RetryOnFailedConnect(true),
		// While disconnected nothing is consumed or published;
		// occurrences due meanwhile are retried, not lost.
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			fields := logger.Fields{}
			if err != nil {
				fields["error"] = err.Error()
			}
			log.Warn("disconnected", fields)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Info("reconnected", logger.Fields{"url": nc.ConnectedUrlRedacted()})
		}),
		nats.ErrorHandler(func(_ *nats.Conn, sub *nats.Subscription, err error) {
			fields := logger.Fields{}
			if sub != nil {
				fields["subject"] = sub.Subject
			}
			log.Error(err, fields)
		}),
	}

	if creds := os.Getenv("NATS_CREDS"); creds != "" {
		opts = append(opts, nats.UserCredentials(creds))
	}
	if seed := os.Getenv("NATS_NKEY_SEED"); seed != "" {
		if opt, err := nats.NkeyOptionFromSeed(seed); err == nil {
			opts = append(opts, opt)
		} else {
			// Surfaced by Connect through an always-failing option.
			opts = append(opts, func(*nats.Options) error { return fmt.Errorf("NATS_NKEY_SEED: %w", err) })
		}
	}
	if user := os.Getenv("NATS_USER"); user != "" {
		opts = append(opts, nats.UserInfo(user, os.Getenv("NATS_PASSWORD")))
	}
	if token := os.Getenv("NATS_TOKEN"); token != "" {
		opts = append(opts, nats.Token(token))
	}
	if ca := os.Getenv("NATS_TLS_CA"); ca != "" {
		opts = append(opts, nats.RootCAs(ca))
	}
	if cert, key := os.Getenv("NATS_TLS_CERT"), os.Getenv("NATS_TLS_KEY"); cert != "" || key != "" {
		opts = append(opts, nats.ClientCert(cert, key))
	}

	return opts
}

// storeDriver opens one kind of HYDRA_STORE. Each store is
// compiled in through its build tag (store_*.go), so a binary
// built for one store carries none of the others' code.
type storeDriver struct {
	name     string
	prefixes []string
	// shared marks stores several instances can use at once.
	shared bool
	open   func(dsn string) (storage.ScheduleStore, error)
}

var drivers []storeDriver

// defaultStore is the HYDRA_STORE used when unset; only the
// SQLite build has one.
var defaultStore string

func registerStore(d storeDriver) { drivers = append(drivers, d) }

func findDriver(dsn string) *storeDriver {
	for i, d := range drivers {
		for _, p := range d.prefixes {
			if strings.HasPrefix(dsn, p) {
				return &drivers[i]
			}
		}
	}

	return nil
}

func supportedStores() string {
	var names []string
	for _, d := range drivers {
		names = append(names, d.name)
	}

	return strings.Join(names, ", ")
}

func openStore(dsn string) (storage.ScheduleStore, error) {
	d := findDriver(dsn)
	if d == nil {
		return nil, fmt.Errorf("unsupported HYDRA_STORE %q; this build supports: %s", dsn, supportedStores())
	}

	s, err := d.open(dsn)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", d.name, err)
	}

	return s, nil
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}

	return fallback
}

func splitList(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}

	return out
}
