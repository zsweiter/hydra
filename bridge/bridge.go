// Package bridge makes hydra the scheduler of a message broker,
// following the wire contract of package contract: CloudEvents
// 1.0 in binary mode, with the recurrence in recurrence*
// extension attributes.
//
// A producer publishes an event whose headers carry a
// recurrence on a subject under a prefix (schedule.<subject>).
// Handle stores it as a schedule (the body, the event's data,
// byte for byte); on every occurrence Deliver publishes it on
// <subject>, with the same body and headers, a new id and time,
// hydra* attributes, and no recurrence* attributes. Messages with no recurrence attribute are
// ignored. The body is never read.
//
// A definition has a lifecycle driven by recurrenceaction
// (create, replace, pause, resume, cancel, delete): creates
// are idempotent, a replacement archives the current revision,
// and every revision and state change records a trace id.
package bridge

import (
	"context"
	"strings"
	"time"

	"github.com/zsweiter/hydra/contract"
	"github.com/zsweiter/hydra/domain"
	"github.com/zsweiter/hydra/pkg/logger"
)

// Header is a broker-neutral multi-valued header map.
type Header map[string][]string

// Get returns the first value of key, compared without case.
func (h Header) Get(key string) string {
	if v := h[key]; len(v) > 0 {
		return v[0]
	}
	for k, v := range h {
		if len(v) > 0 && strings.EqualFold(k, key) {
			return v[0]
		}
	}
	return ""
}

// Set replaces key, removing any spelling of it in another case.
func (h Header) Set(key, value string) {
	h.Del(key)
	h[key] = []string{value}
}

// Del removes key in any case.
func (h Header) Del(key string) {
	for k := range h {
		if strings.EqualFold(k, key) {
			delete(h, k)
		}
	}
}

func (h Header) Clone() Header {
	c := make(Header, len(h))
	for k, v := range h {
		c[k] = append([]string(nil), v...)
	}
	return c
}

// Message is a broker-neutral message. Adapters leave out
// their transport headers (e.g. Nats-*).
type Message struct {
	Subject string
	Header  Header
	Data    []byte

	// ID identifies the message for deduplication. Outbound,
	// it is the occurrence's id, deterministic, so the broker
	// drops a repeat publish.
	ID string

	// Time is when the broker received an inbound message.
	// Relative once rules ("PT10M") are anchored to it, so a
	// redelivery does not move them.
	Time time.Time

	// Version orders inbound messages of one schedule, e.g.
	// when the stream stored it or the partition offset. A change older
	// than the stored one is ignored, so instances processing
	// messages out of order cannot undo newer changes.
	Version int64
}

// Publisher sends occurrences to the broker.
type Publisher interface {
	Publish(ctx context.Context, msg Message) error
}

// BatchPublisher sends many messages concurrently (e.g.
// asynchronous publishes) and returns one error per message,
// or nil when the broker accepted them all.
type BatchPublisher interface {
	Publisher
	PublishBatch(ctx context.Context, msgs []Message) []error
}

// Registry stores schedules: a scheduling.Service.
type Registry interface {
	Load(ctx context.Context, ids []string) ([]*domain.JobSchedule, error)
	Save(ctx context.Context, schedules []*domain.JobSchedule) ([]error, error)
	Delete(ctx context.Context, ids []string) error
}

// Option configures a Bridge.
type Option func(*Bridge)

// WithBinding sets how the broker carries CloudEvents headers
// (default contract.NATS).
func WithBinding(b contract.Binding) Option {
	return func(br *Bridge) { br.binding = b }
}

// WithPrefix sets the subject prefix definitions are published
// under (default contract.DefinitionPrefix). A definition on
// prefix+subject schedules occurrences on subject, so consumers
// of subject never see definitions. Empty publishes occurrences
// on the definition's own subject.
func WithPrefix(prefix string) Option {
	return func(b *Bridge) { b.prefix = prefix }
}

// WithLogger sets the logger.
func WithLogger(l logger.Logger) Option {
	return func(b *Bridge) { b.log = l }
}

// Bridge connects a schedule registry with a broker.
type Bridge struct {
	reg     Registry
	pub     Publisher
	binding contract.Binding
	names   occurrenceHeaders
	prefix  string
	log     logger.Logger
	now     func() time.Time
	metrics Metrics
}

// New creates a Bridge publishing with pub. Register sets where
// definitions are stored, before Handle is called.
func New(pub Publisher, opts ...Option) *Bridge {
	b := &Bridge{
		pub:     pub,
		binding: contract.NATS,
		prefix:  contract.DefinitionPrefix,
		log:     logger.DefaultLogger,
		now:     time.Now,
	}
	for _, opt := range opts {
		opt(b)
	}
	b.names = newOccurrenceHeaders(b.binding)
	return b
}

// Register sets the registry definitions are stored in.
func (b *Bridge) Register(reg Registry) {
	b.reg = reg
}

// Metrics returns the bridge's live counters.
func (b *Bridge) Metrics() *Metrics {
	return &b.metrics
}

// Attributes returns the CloudEvents attributes of h, by name.
func (b *Bridge) Attributes(h Header) map[string]string {
	attrs := make(map[string]string, len(h))
	for k, v := range h {
		if name, ok := b.binding.Attribute(k); ok && len(v) > 0 {
			attrs[name] = v[0]
		}
	}
	return attrs
}

// Key returns the key that orders the processing of msg: the
// schedule a definition belongs to, or "" for anything else.
// Adapters that process messages concurrently send messages
// with the same key to the same worker, so changes to one
// schedule apply in order.
func (b *Bridge) Key(msg Message) string {
	id := msg.Header.Get(b.binding.Header(contract.AttrRecID))
	if id == "" {
		return ""
	}
	subject, _ := b.Subject(msg.Subject)
	return ScheduleKey(subject, id)
}

// Subject returns the subject occurrences of a definition
// published on subject go to by default: subject without the
// prefix. It reports false when subject lacks the prefix, so it
// cannot carry definitions.
func (b *Bridge) Subject(subject string) (string, bool) {
	return strings.CutPrefix(subject, b.prefix)
}

// ScheduleKey returns the schedule ID a recurrenceid is stored
// under, given the subject its occurrences go to by default
// (see Bridge.Subject); the same id can be reused on different
// subjects.
func ScheduleKey(subject, id string) string {
	return subject + "|" + strings.TrimSpace(id)
}

// revisionID is the ID an archived revision is kept under.
func revisionID(key string, revision int) string {
	return key + "#" + itoa(revision)
}
