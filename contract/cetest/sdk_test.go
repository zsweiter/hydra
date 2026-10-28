package cetest

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	natsce "github.com/cloudevents/sdk-go/protocol/nats_jetstream/v2"
	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/nats-io/nats.go"
	"github.com/zsweiter/hydra/bridge"
	"github.com/zsweiter/hydra/contract"
	"github.com/zsweiter/hydra/internal/storage/memory"
	"github.com/zsweiter/hydra/scheduling"
)

type publisher struct {
	mu   sync.Mutex
	msgs []bridge.Message
}

func (p *publisher) Publish(_ context.Context, msg bridge.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.msgs = append(p.msgs, msg)
	return nil
}

func (p *publisher) wait(t *testing.T, n int) []bridge.Message {
	t.Helper()
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		p.mu.Lock()
		msgs := append([]bridge.Message(nil), p.msgs...)
		p.mu.Unlock()
		if len(msgs) >= n {
			return msgs
		}
	}
	t.Fatalf("expected %d occurrences", n)
	return nil
}

func newBridge(t *testing.T) (*bridge.Bridge, *publisher) {
	t.Helper()
	pub := &publisher{}
	b := bridge.New(pub)
	svc, err := scheduling.New(scheduling.Config{Store: memory.New(), Deliver: b.Deliver})
	if err != nil {
		t.Fatal(err)
	}
	b.Register(svc)
	if err := svc.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { svc.Stop(context.Background()) })
	return b, pub
}

// encode writes e as the SDK's NATS binding does in binary
// mode, as a definition of occurrences on subject.
func encode(t *testing.T, subject string, e event.Event, version int64) bridge.Message {
	t.Helper()
	var body bytes.Buffer
	header, err := natsce.WriteMsg(context.Background(), binding.ToMessage(&e), &body)
	if err != nil {
		t.Fatal(err)
	}
	return bridge.Message{
		Subject: contract.DefinitionSubject(subject), Header: bridge.Header(header), Data: body.Bytes(),
		ID: e.ID(), Time: time.Now(), Version: version,
	}
}

// decode reads msg with the SDK, as an SDK consumer would.
func decode(t *testing.T, msg bridge.Message) *event.Event {
	t.Helper()
	m := nats.NewMsg(msg.Subject)
	m.Data = msg.Data
	for k, values := range msg.Header {
		for _, v := range values {
			m.Header.Add(k, v)
		}
	}
	e, err := binding.ToEvent(context.Background(), natsce.NewMessage(m))
	if err != nil {
		t.Fatalf("SDK cannot read the occurrence: %v (headers %v)", err, msg.Header)
	}
	if err := e.Validate(); err != nil {
		t.Fatalf("occurrence is not a valid CloudEvent: %v", err)
	}
	return e
}

func definition(id string) event.Event {
	e := event.New()
	e.SetID(id)
	e.SetSource("/billing")
	e.SetType("com.acme.charge.due")
	e.SetSubject("sub-9")
	e.SetExtension("recurrenceid", "sub-9")
	e.SetExtension("recurrencetype", "once")
	e.SetExtension("recurrencerule", "PT0.1S")
	e.SetExtension("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	return e
}

func TestSDKDefinitionRoundTrip(t *testing.T) {
	b, pub := newBridge(t)

	def := definition("evt-1")
	body := []byte{0x0a, 0x05, 's', 'u', 'b', '-', '9', 0x10, 0xc6, 0x0f}
	if err := def.SetData("application/x-protobuf", body); err != nil {
		t.Fatal(err)
	}
	def.SetExtension("recurrencecount", 1) // an SDK Integer, not a string

	for i, r := range b.Handle(context.Background(), []bridge.Message{encode(t, "billing", def, 1)}) {
		if r.Outcome != bridge.Ack {
			t.Fatalf("definition %d: outcome %v, %v", i, r.Outcome, r.Err)
		}
	}

	occ := decode(t, pub.wait(t, 1)[0])
	if !bytes.Equal(occ.Data(), body) {
		t.Errorf("data %x, want %x byte for byte", occ.Data(), body)
	}
	if !strings.HasPrefix(occ.ID(), "billing|sub-9@") {
		t.Errorf("id %q", occ.ID())
	}
	for name, want := range map[string]string{
		"source":          "/billing",
		"type":            "com.acme.charge.due",
		"subject":         "sub-9",
		"datacontenttype": "application/x-protobuf",
	} {
		if got := attr(occ, name); got != want {
			t.Errorf("%s %q, want %q", name, got, want)
		}
	}
	ext := occ.Extensions()
	for name, want := range map[string]string{
		"hydraschedule": "sub-9",
		"hydrarevision": "1",
		"hydratrace":    "4bf92f3577b34da6a3ce929d0e0e4736",
		"traceparent":   "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
	} {
		if got, _ := ext[name].(string); got != want {
			t.Errorf("extension %s %q, want %q", name, got, want)
		}
	}
	if _, ok := ext["hydraoccurrence"]; !ok {
		t.Error("missing hydraoccurrence")
	}
	for name := range ext {
		if strings.HasPrefix(name, "recurrence") {
			t.Errorf("occurrence carries %s", name)
		}
	}
}

// The SDK writes datacontenttype as ce-datacontenttype, not as
// content-type; both must be accepted and kept.
func TestSDKContentTypeHeader(t *testing.T) {
	def := definition("evt-2")
	def.SetData("application/json", []byte(`{"a":1}`))
	msg := encode(t, "billing", def, 1)
	if msg.Header.Get("ce-datacontenttype") == "" && msg.Header.Get("content-type") == "" {
		t.Fatalf("SDK wrote no content type: %v", msg.Header)
	}

	b, pub := newBridge(t)
	if r := b.Handle(context.Background(), []bridge.Message{msg})[0]; r.Outcome != bridge.Ack {
		t.Fatalf("outcome %v, %v", r.Outcome, r.Err)
	}
	if got := decode(t, pub.wait(t, 1)[0]).DataContentType(); got != "application/json" {
		t.Errorf("datacontenttype %q", got)
	}
}

// Structured mode carries every attribute in a JSON body, so
// hydra, reading headers only, sees no recurrence: the event is
// acknowledged and ignored like any other, never scheduled.
func TestSDKStructuredModeIsIgnored(t *testing.T) {
	def := definition("evt-3")
	def.SetData("application/json", []byte(`{"a":1}`))

	var body bytes.Buffer
	ctx := binding.WithForceStructured(context.Background())
	header, err := natsce.WriteMsg(ctx, binding.ToMessage(&def), &body)
	if err != nil {
		t.Fatal(err)
	}

	b, pub := newBridge(t)
	msg := bridge.Message{Subject: contract.DefinitionSubject("billing"), Header: bridge.Header(header), Data: body.Bytes(), ID: "evt-3", Time: time.Now(), Version: 1}
	if r := b.Handle(context.Background(), []bridge.Message{msg})[0]; r.Outcome != bridge.Ack {
		t.Fatalf("outcome %v, %v; want ack", r.Outcome, r.Err)
	}
	if b.Metrics().MessagesIgnored.Load() != 1 {
		t.Error("structured event not counted as ignored")
	}
	time.Sleep(300 * time.Millisecond)
	pub.mu.Lock()
	defer pub.mu.Unlock()
	if len(pub.msgs) != 0 {
		t.Error("structured event was scheduled")
	}
}

func attr(e *event.Event, name string) string {
	switch name {
	case "source":
		return e.Source()
	case "type":
		return e.Type()
	case "subject":
		return e.Subject()
	case "datacontenttype":
		return e.DataContentType()
	}
	return ""
}
