package natsbroker

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/zsweiter/hydra/bridge"
	"github.com/zsweiter/hydra/contract"
	"github.com/zsweiter/hydra/internal/storage/memory"
	"github.com/zsweiter/hydra/scheduling"
)

// testStream is a pair of streams: events, where occurrences
// are published, and defs, holding the definitions under the
// prefix.
type testStream struct {
	events, defs string
}

// subject returns a subject of the events stream.
func (s testStream) subject(name string) string { return s.events + "." + name }

// testJetStream connects to NATS_URL and creates fresh
// streams. It skips unless NATS_URL is set, so tests never
// touch a server that happens to listen on the default port
// (make test-integration starts one).
func testJetStream(t *testing.T, n int) (jetstream.JetStream, []testStream) {
	t.Helper()
	url := os.Getenv("NATS_URL")
	if url == "" {
		t.Skip("NATS_URL not set")
	}
	nc, err := nats.Connect(url, nats.Timeout(time.Second))
	if err != nil {
		t.Skipf("NATS not available at %s: %v", url, err)
	}
	t.Cleanup(nc.Close)

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}

	var streams []testStream
	for i := range n {
		s := testStream{events: fmt.Sprintf("HYDRA_TEST_%d_%d", time.Now().UnixNano(), i)}
		s.defs = s.events + "_DEFS"
		createStream(t, js, s.events, s.events+".>")
		createStream(t, js, s.defs, contract.DefinitionSubject(s.events+".>"))
		streams = append(streams, s)
	}
	return js, streams
}

func createStream(t *testing.T, js jetstream.JetStream, name, subject string, retention ...jetstream.RetentionPolicy) {
	t.Helper()
	cfg := jetstream.StreamConfig{Name: name, Subjects: []string{subject}}
	if len(retention) > 0 {
		cfg.Retention = retention[0]
	}
	if _, err := js.CreateOrUpdateStream(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { js.DeleteStream(context.Background(), name) })
}

func defStreams(streams []testStream) []string {
	var names []string
	for _, s := range streams {
		names = append(names, s.defs)
	}
	return names
}

func startScheduler(t *testing.T, js jetstream.JetStream, cfg Config) {
	t.Helper()
	b := bridge.New(NewPublisher(js))
	svc, err := scheduling.New(scheduling.Config{Store: memory.New(), Deliver: b.Deliver})
	if err != nil {
		t.Fatal(err)
	}
	b.Register(svc)

	ctx, cancel := context.WithCancel(context.Background())
	if err := svc.Start(ctx); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		Consume(ctx, js, cfg, b)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
		svc.Stop(context.Background())
	})

	for _, name := range cfg.Streams {
		waitForConsumer(t, js, name, cfg.Consumer)
	}
}

// waitForConsumer waits for the scheduler's consumer: a new one
// delivers only new messages, so tests publish after it exists.
func waitForConsumer(t *testing.T, js jetstream.JetStream, stream, consumer string) {
	t.Helper()
	if consumer == "" {
		consumer = "hydra-scheduler"
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := js.Consumer(context.Background(), stream, consumer); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("consumer on %s not created", stream)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// occurrences collects every message published on subject of
// the events stream: with definitions in a stream of their own,
// all of them must be occurrences.
func occurrences(t *testing.T, js jetstream.JetStream, s testStream, subject string) <-chan jetstream.Msg {
	t.Helper()
	cons, err := js.OrderedConsumer(context.Background(), s.events, jetstream.OrderedConsumerConfig{FilterSubjects: []string{subject}})
	if err != nil {
		t.Fatal(err)
	}
	out := make(chan jetstream.Msg, 100)
	cc, err := cons.Consume(func(m jetstream.Msg) {
		if m.Headers().Get("ce-hydraschedule") == "" {
			t.Errorf("%s carries a message that is not an occurrence: %v", subject, m.Headers())
		}
		out <- m
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cc.Stop)
	return out
}

// publish publishes a definition of occurrences due on subject.
func publish(t *testing.T, js jetstream.JetStream, subject string, d contract.Definition, data []byte) {
	t.Helper()
	headers, err := contract.Headers(contract.NATS, contract.Event{
		ID: fmt.Sprintf("evt-%d", time.Now().UnixNano()), Source: "/tests", Type: "com.acme.reminder",
		ContentType: "application/octet-stream",
	}, d)
	if err != nil {
		t.Fatal(err)
	}
	m := nats.NewMsg(contract.DefinitionSubject(subject))
	m.Data = data
	for k, v := range headers {
		m.Header.Set(k, v)
	}
	if _, err := js.PublishMsg(context.Background(), m); err != nil {
		t.Fatal(err)
	}
}

func expect(t *testing.T, ch <-chan jetstream.Msg, what string) jetstream.Msg {
	t.Helper()
	select {
	case m := <-ch:
		return m
	case <-time.After(5 * time.Second):
		t.Fatalf("%s: no occurrence", what)
		return nil
	}
}

func TestScheduler_RepublishesOccurrences(t *testing.T) {
	js, streams := testJetStream(t, 1)
	startScheduler(t, js, Config{Streams: defStreams(streams)})

	subject := streams[0].subject("reminder")
	got := occurrences(t, js, streams[0], subject)
	body := []byte{0xde, 0xad, 0xbe, 0xef}
	publish(t, js, subject, contract.Definition{ID: "r1", Type: contract.TypeInterval, Rule: "PT0.2S", Count: 2, Offset: -50 * time.Millisecond}, body)

	for i := range 2 {
		m := expect(t, got, fmt.Sprint("occurrence ", i))
		if !bytes.Equal(m.Data(), body) {
			t.Fatalf("occurrence %d body %x, want it untouched", i, m.Data())
		}
		if m.Headers().Get(jetstream.MsgIDHeader) == "" || m.Headers().Get("ce-recurrencetype") != "" {
			t.Fatalf("occurrence %d headers %v", i, m.Headers())
		}
	}
	select {
	case m := <-got:
		t.Fatalf("published beyond recurrencecount: %v", m.Headers())
	case <-time.After(500 * time.Millisecond):
	}
}

func TestScheduler_SeveralStreamsAndFilters(t *testing.T) {
	js, streams := testJetStream(t, 2)
	startScheduler(t, js, Config{Streams: defStreams(streams), Subjects: []string{
		contract.DefinitionSubject(streams[0].subject("billing.>")),
		contract.DefinitionSubject(streams[1].events + ".>"),
	}})

	billing := occurrences(t, js, streams[0], streams[0].subject("billing.charge"))
	other := occurrences(t, js, streams[0], streams[0].subject("other"))
	second := occurrences(t, js, streams[1], streams[1].subject("reminder"))

	once := contract.Definition{Type: contract.TypeOnce, Rule: "PT0.05S"}
	once.ID = "a"
	publish(t, js, streams[0].subject("billing.charge"), once, nil)
	once.ID = "b"
	publish(t, js, streams[0].subject("other"), once, nil) // filtered out
	once.ID = "c"
	publish(t, js, streams[1].subject("reminder"), once, nil)

	expect(t, billing, "billing")
	expect(t, second, "second stream")
	select {
	case <-other:
		t.Fatal("a subject outside the filters was scheduled")
	case <-time.After(500 * time.Millisecond):
	}
}

func TestScheduler_CancelStopsOccurrences(t *testing.T) {
	js, streams := testJetStream(t, 1)
	startScheduler(t, js, Config{Streams: defStreams(streams)})

	subject := streams[0].subject("billing")
	got := occurrences(t, js, streams[0], subject)

	publish(t, js, subject, contract.Definition{ID: "sub-1", Type: contract.TypeInterval, Rule: "PT0.1S"}, nil)
	expect(t, got, "before cancel")

	publish(t, js, subject, contract.Definition{ID: "sub-1", Action: contract.ActionCancel}, nil)
	time.Sleep(300 * time.Millisecond)
	for len(got) > 0 {
		<-got
	}
	select {
	case m := <-got:
		t.Fatalf("occurrence after cancel: %v", m.Headers())
	case <-time.After(400 * time.Millisecond):
	}
}

// A consumer deleted by hand, or with its stream, is created
// again, and definitions published meanwhile are not lost.
func TestScheduler_RecreatesLostConsumer(t *testing.T) {
	t.Run("limits", func(t *testing.T) { testRecreatesLostConsumer(t, jetstream.LimitsPolicy) })
	t.Run("workqueue", func(t *testing.T) { testRecreatesLostConsumer(t, jetstream.WorkQueuePolicy) })
}

func testRecreatesLostConsumer(t *testing.T, retention jetstream.RetentionPolicy) {
	js, streams := testJetStream(t, 1)
	// A work queue cannot change retention in place.
	if err := js.DeleteStream(context.Background(), streams[0].defs); err != nil {
		t.Fatal(err)
	}
	createStream(t, js, streams[0].defs, contract.DefinitionSubject(streams[0].events+".>"), retention)
	health := &Health{}
	startScheduler(t, js, Config{Streams: defStreams(streams), CheckInterval: 100 * time.Millisecond, Health: health})
	s := streams[0]
	got := occurrences(t, js, s, s.subject("a"))
	once := contract.Definition{Type: contract.TypeOnce, Rule: "PT0.05S"}
	ctx := context.Background()

	once.ID = "before"
	publish(t, js, s.subject("a"), once, nil)
	expect(t, got, "before any loss")
	// A schedule changed after the stream is recreated, when
	// sequences start over.
	ticks := occurrences(t, js, s, s.subject("tick"))
	publish(t, js, s.subject("tick"), contract.Definition{ID: "tick", Type: contract.TypeInterval, Rule: "PT0.1S"}, nil)
	expect(t, ticks, "tick")

	waitHealthy := func(restarts int64) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for health.Restarts() < restarts || len(health.Down()) > 0 {
			if time.Now().After(deadline) {
				t.Fatalf("restarts %d, down %v; want %d restarts and every stream consumed", health.Restarts(), health.Down(), restarts)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}

	// The consumer alone: resumed from when it was last seen,
	// so a definition published before it is recreated counts.
	if err := js.DeleteConsumer(ctx, s.defs, "hydra-scheduler"); err != nil {
		t.Fatal(err)
	}
	once.ID = "consumer-deleted"
	publish(t, js, s.subject("a"), once, nil)
	waitHealthy(1)
	if m := expect(t, got, "after the consumer was deleted"); m.Headers().Get("ce-hydraschedule") != "consumer-deleted" {
		t.Fatalf("occurrence of %s", m.Headers().Get("ce-hydraschedule"))
	}

	// The stream: recreated, it is consumed from its start.
	if err := js.DeleteStream(ctx, s.defs); err != nil {
		t.Fatal(err)
	}
	createStream(t, js, s.defs, contract.DefinitionSubject(s.events+".>"), retention)
	once.ID = "stream-recreated"
	publish(t, js, s.subject("a"), once, nil)
	waitHealthy(2)
	if m := expect(t, got, "after the stream was recreated"); m.Headers().Get("ce-hydraschedule") != "stream-recreated" {
		t.Fatalf("occurrence of %s", m.Headers().Get("ce-hydraschedule"))
	}

	publish(t, js, s.subject("tick"), contract.Definition{ID: "tick", Action: contract.ActionCancel}, nil)
	time.Sleep(500 * time.Millisecond)
	for len(ticks) > 0 {
		<-ticks
	}
	select {
	case <-ticks:
		t.Fatal("cancel published on the recreated stream was ignored")
	case <-time.After(500 * time.Millisecond):
	}
}

func TestSubjectsCollide(t *testing.T) {
	cases := []struct {
		a, b string
		want bool
	}{
		{"a.b", "a.b", true},
		{"a.*", "a.b", true},
		{"a.>", "a.b.c", true},
		{"a.b.>", "a.>", true},
		{"a.b", "a.c", false},
		{"a.*", "a.b.c", false},
		{"a", "a.b", false},
		{"a.>", "a", false},
		{">", "x.y", true},
	}
	for _, c := range cases {
		if got := subjectsCollide(c.a, c.b); got != c.want {
			t.Errorf("subjectsCollide(%q, %q) = %v", c.a, c.b, got)
		}
	}
	if _, err := filtersFor([]string{"orders.>"}, []string{"billing.>"}); err == nil {
		t.Error("filters matching nothing in the stream accepted")
	}
}

// Workers start with the load and stop when idle, and the
// changes of a schedule still apply in order.
func TestConsume_ElasticWorkers(t *testing.T) {
	js, streams := testJetStream(t, 1)
	health := &Health{}
	startScheduler(t, js, Config{Streams: defStreams(streams), Workers: 4, IdleTimeout: 200 * time.Millisecond, Health: health})
	s := streams[0]
	got := occurrences(t, js, s, s.subject("a"))

	if n := health.Workers(); n != 0 {
		t.Fatalf("%d workers before any message", n)
	}
	// Created, then replaced: only the replacement fires.
	publish(t, js, s.subject("a"), contract.Definition{ID: "x", Type: contract.TypeOnce, Rule: "PT1H"}, nil)
	publish(t, js, s.subject("a"), contract.Definition{ID: "x", Action: contract.ActionReplace, Type: contract.TypeOnce, Rule: "PT0.05S"}, nil)
	if m := expect(t, got, "replacement"); m.Headers().Get("ce-hydrarevision") != "2" {
		t.Fatalf("revision %s", m.Headers().Get("ce-hydrarevision"))
	}
	if n := health.Workers(); n < 1 || n > 4 {
		t.Fatalf("%d workers under load, want 1 to 4", n)
	}

	deadline := time.Now().Add(3 * time.Second)
	for health.Workers() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%d workers still running after idling", health.Workers())
		}
		time.Sleep(20 * time.Millisecond)
	}

	// An idle slot starts again.
	publish(t, js, s.subject("a"), contract.Definition{ID: "y", Type: contract.TypeOnce, Rule: "PT0.05S"}, nil)
	expect(t, got, "after idling")
}
