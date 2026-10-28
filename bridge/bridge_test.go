package bridge

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/zsweiter/hydra/contract"
	"github.com/zsweiter/hydra/domain"
	"github.com/zsweiter/hydra/internal/storage/memory"
	"github.com/zsweiter/hydra/scheduling"
)

type fakePublisher struct {
	mu   sync.Mutex
	msgs []Message
	fail map[string]int // remaining failures per message id
}

func (p *fakePublisher) Publish(_ context.Context, msg Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.fail[msg.ID] > 0 {
		p.fail[msg.ID]--
		return errors.New("broker unavailable")
	}
	p.msgs = append(p.msgs, msg)
	return nil
}

func (p *fakePublisher) published() []Message {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]Message(nil), p.msgs...)
}

func (p *fakePublisher) waitFor(t *testing.T, n int, timeout time.Duration) []Message {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if msgs := p.published(); len(msgs) >= n {
			return msgs
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("expected %d published messages, got %d", n, len(p.published()))
	return nil
}

type harness struct {
	b     *Bridge
	pub   *fakePublisher
	store *memory.Storage
	svc   *scheduling.Service
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	h := &harness{pub: &fakePublisher{fail: map[string]int{}}, store: memory.New()}
	h.b = New(h.pub)
	svc, err := scheduling.New(scheduling.Config{Store: h.store, Deliver: h.b.Deliver})
	if err != nil {
		t.Fatal(err)
	}
	h.svc = svc
	h.b.Register(svc)
	if err := svc.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { svc.Stop(context.Background()) })
	return h
}

// msg builds a definition event for occurrences on subject (it
// is published under the definitions prefix); rec maps
// recurrence attribute names (without the "recurrence" prefix)
// to values.
func msg(subject string, version int64, rec map[string]string, data string) Message {
	h := Header{
		"ce-specversion": {"1.0"},
		"ce-id":          {fmt.Sprintf("evt-%d", version)},
		"ce-source":      {"/tests"},
		"ce-type":        {"com.acme.reminder"},
		"content-type":   {"application/octet-stream"},
	}
	for k, v := range rec {
		h["ce-recurrence"+k] = []string{v}
	}
	return Message{Subject: contract.DefinitionSubject(subject), Header: h, Data: []byte(data), ID: fmt.Sprintf("stream:%d", version), Version: version}
}

func (h *harness) handle(t *testing.T, msgs ...Message) []Result {
	t.Helper()
	return h.b.Handle(context.Background(), msgs)
}

func (h *harness) ok(t *testing.T, msgs ...Message) {
	t.Helper()
	for i, r := range h.handle(t, msgs...) {
		if r.Outcome != Ack {
			t.Fatalf("message %d: outcome %v, err %v", i, r.Outcome, r.Err)
		}
	}
}

func (h *harness) schedule(t *testing.T, key string) *domain.JobSchedule {
	t.Helper()
	sch, err := h.store.LoadSchedule(context.Background(), key)
	if err != nil {
		t.Fatalf("load %s: %v", key, err)
	}
	return sch
}

func TestRepublishesBodyAndHeadersUntouched(t *testing.T) {
	h := newHarness(t)
	body := []byte{0x08, 0x96, 0x01, 0xff, 0x00} // not JSON: never read
	m := msg("notification.adherence", 1, map[string]string{"id": "adh-1", "type": "interval", "rule": "PT0.1S"}, "")
	m.Data = body
	m.Header["X-Tenant"] = []string{"acme"}
	m.Header["ce-traceparent"] = []string{"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}
	h.ok(t, m)

	msgs := h.pub.waitFor(t, 2, 2*time.Second)
	for _, o := range msgs[:2] {
		attrs := h.b.Attributes(o.Header)
		if o.Subject != "notification.adherence" || !bytes.Equal(o.Data, body) {
			t.Fatalf("occurrence %s %x, want the same subject and body", o.Subject, o.Data)
		}
		if o.Header.Get("X-Tenant") != "acme" || attrs["type"] != "com.acme.reminder" || attrs["source"] != "/tests" ||
			attrs["datacontenttype"] != "application/octet-stream" {
			t.Fatalf("headers not kept: %v", o.Header)
		}
		if attrs["traceparent"] != "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01" {
			t.Fatalf("traceparent not inherited: %v", attrs)
		}
		for name := range attrs {
			if len(name) > 10 && name[:10] == "recurrence" {
				t.Fatalf("recurrence attribute %s republished", name)
			}
		}
		occ, ok, err := contract.ParseOccurrence(attrs)
		if !ok || err != nil || occ.Schedule != "adh-1" || occ.Revision != 1 || occ.Trace != "4bf92f3577b34da6a3ce929d0e0e4736" {
			t.Fatalf("occurrence attributes %+v %v %v", occ, ok, err)
		}
		if attrs["id"] == "evt-1" || attrs["id"] != o.ID {
			t.Fatalf("ce-id %q, want a new id equal to the dedup id %q", attrs["id"], o.ID)
		}
	}
	if msgs[0].ID == msgs[1].ID {
		t.Fatalf("occurrences share id %q", msgs[0].ID)
	}
}

func TestOnceWithNegativeOffsetFiresEarly(t *testing.T) {
	h := newHarness(t)
	m := msg("reminders", 1, map[string]string{"id": "r", "type": "once", "rule": "PT0.4S", "offset": "-PT0.2S"}, "meeting")
	m.Time = time.Now()
	h.ok(t, m)

	o := h.pub.waitFor(t, 1, 2*time.Second)[0]
	if elapsed := time.Since(m.Time); elapsed >= 400*time.Millisecond {
		t.Errorf("fired after %v, want ~200ms (before the nominal 400ms)", elapsed)
	}
	occ, _, _ := contract.ParseOccurrence(h.b.Attributes(o.Header))
	if want := m.Time.Add(400 * time.Millisecond); occ.At.Sub(want).Abs() > time.Millisecond {
		t.Errorf("hydraoccurrence %v, want the nominal %v", occ.At, want)
	}
	time.Sleep(300 * time.Millisecond)
	if n := len(h.pub.published()); n != 1 {
		t.Errorf("once published %d times", n)
	}
}

func TestIgnoresMessagesWithoutRecurrence(t *testing.T) {
	h := newHarness(t)
	plain := Message{Subject: "a", Header: Header{}, Data: []byte(`{"recurrence_type":"interval"}`)}
	ce := msg("a", 1, nil, "x")
	for i, r := range h.handle(t, plain, ce) {
		if r.Outcome != Ack {
			t.Fatalf("message %d: %v %v", i, r.Outcome, r.Err)
		}
	}
	if n := h.b.Metrics().MessagesIgnored.Load(); n != 2 {
		t.Fatalf("ignored = %d, want 2", n)
	}
}

func TestDiscardsInvalidDefinitions(t *testing.T) {
	h := newHarness(t)
	cases := map[string]Message{
		"no id":        msg("s", 1, map[string]string{"type": "interval", "rule": "PT1S"}, ""),
		"go duration":  msg("s", 1, map[string]string{"id": "x", "type": "interval", "rule": "1s"}, ""),
		"bad cron":     msg("s", 1, map[string]string{"id": "x", "type": "cron", "rule": "not a cron"}, ""),
		"typo":         msg("s", 1, map[string]string{"id": "x", "type": "once", "rul": "PT1S"}, ""),
		"bad timezone": msg("s", 1, map[string]string{"id": "x", "type": "interval", "rule": "PT1S", "timezone": "Mars/Base"}, ""),
		"once garbage": msg("s", 1, map[string]string{"id": "x", "type": "once", "rule": "tomorrow"}, ""),
	}
	structured := msg("s", 1, map[string]string{"id": "x", "type": "once"}, "{}")
	structured.Header["content-type"] = []string{"application/cloudevents+json"}
	cases["structured"] = structured
	noSource := msg("s", 1, map[string]string{"id": "x", "type": "once"}, "")
	delete(noSource.Header, "ce-source")
	cases["no source"] = noSource
	// Definitions travel under the prefix, and occurrences never
	// do: they would be read back as definitions.
	unprefixed := msg("s", 1, map[string]string{"id": "x", "type": "once"}, "")
	unprefixed.Subject = "s"
	cases["outside prefix"] = unprefixed
	cases["target under prefix"] = msg("s", 1, map[string]string{"id": "x", "type": "once", "target": "schedule.t"}, "")

	for name, m := range cases {
		r := h.handle(t, m)[0]
		if r.Outcome != Discard || !errors.Is(r.Err, contract.ErrInvalid) {
			t.Errorf("%s: %v %v, want Discard with ErrInvalid", name, r.Outcome, r.Err)
		}
	}
	if n := h.b.Metrics().DefinitionsInvalid.Load(); n != int64(len(cases)) {
		t.Fatalf("invalid = %d, want %d", n, len(cases))
	}
}

// Occurrences go to the definition's subject without the
// prefix, or to recurrencetarget; the key is the former either
// way, so a target can change without making a new schedule.
func TestOccurrenceSubject(t *testing.T) {
	h := newHarness(t)
	h.ok(t,
		msg("notification.billing", 1, map[string]string{"id": "a", "type": "once", "rule": "PT0.1S"}, "x"),
		msg("notification.billing", 2, map[string]string{"id": "b", "type": "once", "rule": "PT0.1S", "target": "billing.due"}, "x"),
	)
	got := map[string]string{}
	for _, o := range h.pub.waitFor(t, 2, 2*time.Second) {
		got[o.Header.Get("ce-hydraschedule")] = o.Subject
	}
	if got["a"] != "notification.billing" || got["b"] != "billing.due" {
		t.Fatalf("occurrence subjects %v", got)
	}
	if h.schedule(t, "notification.billing|b") == nil {
		t.Fatal("schedule b not stored under the definition's subject")
	}

	// Without a prefix, occurrences share the definition's subject.
	pub := &fakePublisher{}
	b := New(pub, WithPrefix(""))
	svc, err := scheduling.New(scheduling.Config{Store: memory.New(), Deliver: b.Deliver})
	if err != nil {
		t.Fatal(err)
	}
	b.Register(svc)
	svc.Start(context.Background())
	defer svc.Stop(context.Background())
	m := msg("s", 1, map[string]string{"id": "c", "type": "once", "rule": "PT0.1S"}, "x")
	m.Subject = "s"
	if r := b.Handle(context.Background(), []Message{m})[0]; r.Outcome != Ack {
		t.Fatalf("outcome %v %v", r.Outcome, r.Err)
	}
	if o := pub.waitFor(t, 1, 2*time.Second)[0]; o.Subject != "s" {
		t.Fatalf("occurrence on %s, want s", o.Subject)
	}
}

func TestDeliveryFailureRetriesWithoutAdvancing(t *testing.T) {
	h := newHarness(t)
	h.ok(t, msg("s", 1, map[string]string{"id": "x", "type": "once", "rule": "PT0.3S"}, "x"))

	// Fail the first two publishes of the occurrence.
	deadline := time.Now().Add(time.Second)
	for {
		jobs := h.schedule(t, "s|x")
		if jobs.NextRunAt != nil {
			id := fmt.Sprintf("s|x@%d", jobs.NextRunAt.UTC().UnixNano())
			h.pub.mu.Lock()
			h.pub.fail[id] = 2
			h.pub.mu.Unlock()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("schedule not stored")
		}
	}

	o := h.pub.waitFor(t, 1, 10*time.Second)[0]
	if got := h.schedule(t, "s|x"); got.GeneratedJobs != 1 || got.Status != domain.ScheduleFinished {
		t.Fatalf("after delivery: generated %d status %v", got.GeneratedJobs, got.Status)
	}
	if n := h.b.Metrics().PublishErrors.Load(); n != 2 {
		t.Fatalf("publish errors = %d, want 2", n)
	}
	if o.ID == "" || len(h.pub.published()) != 1 {
		t.Fatalf("published %d times, want once", len(h.pub.published()))
	}
	if st := h.svc.Stats(); st.Retried < 2 || st.Delivered != 1 {
		t.Fatalf("stats %+v, want 2 retries and 1 delivery", st)
	}
}

func TestKey(t *testing.T) {
	h := newHarness(t)
	m := msg("s", 1, map[string]string{"id": "a", "type": "once"}, "")
	if k := h.b.Key(m); k != "s|a" {
		t.Fatalf("Key = %q", k)
	}
	m.Header["CE-RecurrenceId"] = m.Header["ce-recurrenceid"]
	delete(m.Header, "ce-recurrenceid")
	if k := h.b.Key(m); k != "s|a" {
		t.Fatalf("Key with another case = %q", k)
	}
	if k := h.b.Key(Message{Subject: "s", Header: Header{}}); k != "" {
		t.Fatalf("Key of a plain message = %q", k)
	}
}
