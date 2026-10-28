package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/zsweiter/hydra/bridge"
	natsbroker "github.com/zsweiter/hydra/broker/nats"
	"github.com/zsweiter/hydra/pkg/logger"
	"github.com/zsweiter/hydra/scheduling"
	"github.com/zsweiter/hydra/storage"
)

// opsServer exposes health and Prometheus metrics on an
// internal port. It is off unless HYDRA_HTTP_ADDR is set and
// accepts no input: it is for probes and scraping, not an API.
//
// Broker-side figures (consumer lag, redeliveries, stream
// rates) come from NATS monitoring and prometheus-nats-exporter;
// these metrics cover what only hydra knows.
type opsServer struct {
	nc       *nats.Conn
	store    storage.ScheduleStore
	svc      *scheduling.Service
	bridge   *bridge.Bridge
	consumer *natsbroker.Health
}

func (o *opsServer) serve(ctx context.Context, addr string, log logger.Logger) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("GET /readyz", o.ready)
	mux.HandleFunc("GET /metrics", o.metrics)

	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutdown)
	}()

	log.Info("ops server listening", logger.Fields{"addr": addr})
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error(fmt.Errorf("ops server: %w", err))
	}
}

// ready reports whether the broker connection is up, every
// stream is being consumed and the store answers.
func (o *opsServer) ready(w http.ResponseWriter, r *http.Request) {
	if !o.nc.IsConnected() {
		http.Error(w, "nats: "+o.nc.Status().String(), http.StatusServiceUnavailable)
		return
	}
	if down := o.consumer.Down(); len(down) > 0 {
		http.Error(w, "not consuming: "+strings.Join(down, ","), http.StatusServiceUnavailable)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if _, err := o.store.LoadSchedules(ctx, []string{"readyz"}); err != nil {
		http.Error(w, "store: "+err.Error(), http.StatusServiceUnavailable)
		return
	}

	fmt.Fprintln(w, "ok")
}

func (o *opsServer) metrics(w http.ResponseWriter, _ *http.Request) {
	var b strings.Builder
	m := o.bridge.Metrics()
	st := o.svc.Stats()

	counter := func(name, help string, v int64) {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s counter\n%s %d\n", name, help, name, name, v)
	}
	gauge := func(name, help string, v float64) {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s gauge\n%s %g\n", name, help, name, name, v)
	}

	counter("hydra_messages_ignored_total", "Messages without recurrence attributes, ignored.", m.MessagesIgnored.Load())
	// One metric with a result label: their sum is every
	// definition processed, and a ratio of any two is one query.
	fmt.Fprint(&b, "# HELP hydra_definitions_total Definitions processed, by result.\n# TYPE hydra_definitions_total counter\n")
	for _, r := range []struct {
		result string
		v      *atomic.Int64
	}{
		{"registered", &m.DefinitionsRegistered}, // created or replaced
		{"replaced", &m.DefinitionsReplaced},     // archived a previous revision
		{"duplicate", &m.DefinitionsDuplicate},   // identical to the stored one
		{"paused", &m.DefinitionsPaused},
		{"resumed", &m.DefinitionsResumed},
		{"cancelled", &m.DefinitionsCancelled},
		{"deleted", &m.DefinitionsDeleted},
		{"stale", &m.DefinitionsStale},       // older than the stored change
		{"conflict", &m.DefinitionsConflict}, // create over a different live definition
		{"unknown", &m.DefinitionsUnknown},   // change of a schedule that does not exist
		{"invalid", &m.DefinitionsInvalid},   // breaks the contract
	} {
		fmt.Fprintf(&b, "hydra_definitions_total{result=%q} %d\n", r.result, r.v.Load())
	}
	counter("hydra_occurrences_published_total", "Occurrences published to the broker.", m.OccurrencesPublished.Load())
	counter("hydra_publish_errors_total", "Occurrences the broker did not accept (published again with backoff).", m.PublishErrors.Load())
	histogram(&b, "hydra_publish_delay_seconds", "How late occurrences were published compared to when they were due.", &m.PublishDelay)

	counter("hydra_occurrences_claimed_total", "Occurrences claimed by this instance.", st.Claimed)
	counter("hydra_claims_lost_total", "Claims another instance won first.", st.Lost)
	counter("hydra_deliveries_retried_total", "Deliveries retried after the broker did not accept them.", st.Retried)
	gauge("hydra_schedules_stalled", "Schedules whose delivery failed 10 times in a row.", float64(st.Stalled))
	gauge("hydra_schedules_loaded", "Schedules held in memory (due within the window).", float64(st.Loaded))
	gauge("hydra_catchup_backlog", "Catch-up schedules still replaying missed occurrences.", float64(st.Backlog))
	gauge("hydra_shards_owned", "Schedule shards owned by this instance.", float64(st.Shards))
	counter("hydra_history_purged_total", "Ended, cancelled and archived schedules deleted after HYDRA_HISTORY_TTL.", st.Purged)

	fmt.Fprintf(&b, "# HELP hydra_store_errors_total Failed store operations, retried: definitions not stored (consumer) or schedules not loaded, claimed or synced (scheduler).\n"+
		"# TYPE hydra_store_errors_total counter\n"+
		"hydra_store_errors_total{component=\"consumer\"} %d\nhydra_store_errors_total{component=\"scheduler\"} %d\n",
		m.StoreErrors.Load(), st.StoreErrors)

	connected := 0.0
	if o.nc.IsConnected() {
		connected = 1
	}
	gauge("hydra_nats_connected", "Whether the NATS connection is up.", connected)
	counter("hydra_nats_reconnects_total", "Times the NATS connection was re-established.", int64(o.nc.Stats().Reconnects))
	gauge("hydra_streams_down", "Streams not being consumed: not created yet, or whose consumer is being recreated.", float64(len(o.consumer.Down())))
	gauge("hydra_consumer_workers", "Consumer workers running: they start with the load and stop when idle.", float64(o.consumer.Workers()))
	counter("hydra_consumer_restarts_total", "Consumers created again after being lost (deleted, or their stream recreated).", o.consumer.Restarts())

	fmt.Fprintf(&b, "# HELP hydra_build_info The running version.\n# TYPE hydra_build_info gauge\nhydra_build_info{version=%q} 1\n", version)

	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprint(w, b.String())
}

// histogram writes h in the Prometheus text format.
func histogram(b *strings.Builder, name, help string, h *bridge.Histogram) {
	buckets, count, sum := h.Snapshot()
	fmt.Fprintf(b, "# HELP %s %s\n# TYPE %s histogram\n", name, help, name)
	for i, le := range bridge.DelayBuckets {
		fmt.Fprintf(b, "%s_bucket{le=\"%g\"} %d\n", name, le.Seconds(), buckets[i])
	}
	fmt.Fprintf(b, "%s_bucket{le=\"+Inf\"} %d\n%s_sum %g\n%s_count %d\n", name, count, name, sum.Seconds(), name, count)
}
