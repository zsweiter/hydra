package bridge

import (
	"context"
	"testing"
	"time"

	"github.com/zsweiter/hydra/contract"
	"github.com/zsweiter/hydra/domain"
)

type nopPublisher struct{}

func (nopPublisher) Publish(context.Context, Message) error          { return nil }
func (nopPublisher) PublishBatch(context.Context, []Message) []error { return nil }

func benchDefinition() Message {
	m := msg("notification.billing", 1, map[string]string{
		"id": "sub-9", "type": "cron", "rule": "0 9 * * *", "timezone": "America/Santiago", "offset": "-PT5M",
	}, `{"text":"take your medication"}`)
	m.Header["ce-traceparent"] = []string{"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}
	m.Header["x-tenant"] = []string{"acme"}
	return m
}

// BenchmarkDeliver publishes batches of occurrences as the
// scheduler hands them over: the cost paid on every occurrence.
func BenchmarkDeliver(b *testing.B) {
	br := New(nopPublisher{})
	m := benchDefinition()
	attrs := br.Attributes(m.Header)
	def, err := contract.ParseDefinition(attrs)
	if err != nil {
		b.Fatal(err)
	}
	sch, err := br.build(item{msg: m, subject: "notification.billing", key: "notification.billing|sub-9", def: def, attrs: attrs}, "trace")
	if err != nil {
		b.Fatal(err)
	}

	const batch = 512
	jobs := make([]*domain.Job, batch)
	due := time.Now()
	for i := range jobs {
		due = due.Add(time.Minute)
		jobs[i] = benchJob(sch, due)
	}

	b.ReportAllocs()
	for b.Loop() {
		br.Deliver(context.Background(), jobs)
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*batch), "ns/occurrence")
}

// BenchmarkDefinition reads and builds a create: the cost paid
// once per definition, before it is stored.
func BenchmarkDefinition(b *testing.B) {
	br := New(nopPublisher{})
	m := benchDefinition()
	b.ReportAllocs()
	for b.Loop() {
		attrs := br.Attributes(m.Header)
		def, err := contract.ParseDefinition(attrs)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := br.build(item{msg: m, subject: "notification.billing", key: "k", def: def, attrs: attrs}, "t"); err != nil {
			b.Fatal(err)
		}
	}
}

// benchJob builds the job of one occurrence as the scheduler
// does for direct delivery.
func benchJob(sch *domain.JobSchedule, runAt time.Time) *domain.Job {
	metadata := make(map[string]string, len(sch.Metadata)+1)
	for k, v := range sch.Metadata {
		metadata[k] = v
	}
	metadata[domain.MetadataScheduledFor] = runAt.UTC().Format(time.RFC3339Nano)
	return &domain.Job{ID: "job", Name: sch.Name, Data: sch.Data, Metadata: metadata, RunAt: runAt, ScheduleID: &sch.ID}
}
