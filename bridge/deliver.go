package bridge

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/zsweiter/hydra/contract"
	"github.com/zsweiter/hydra/domain"
	"github.com/zsweiter/hydra/pkg/logger"
)

// occurrenceHeaders are the header names every occurrence sets,
// resolved once for the binding.
type occurrenceHeaders struct {
	id, time, schedule, occurrence, revision, trace string
}

// occurrenceSlots is how many headers an occurrence adds to its
// template.
const occurrenceSlots = 6

func newOccurrenceHeaders(b contract.Binding) occurrenceHeaders {
	return occurrenceHeaders{
		id:         b.Header(contract.AttrID),
		time:       b.Header(contract.AttrTime),
		schedule:   b.Header(contract.AttrSchedule),
		occurrence: b.Header(contract.AttrOccurrence),
		revision:   b.Header(contract.AttrRevision),
		trace:      b.Header(contract.AttrTrace),
	}
}

// occurrenceAttr reports whether every occurrence sets the
// attribute, so a definition's own value is not kept.
func (b *Bridge) occurrenceAttr(name string) bool {
	switch name {
	case contract.AttrID, contract.AttrTime, contract.AttrSchedule,
		contract.AttrOccurrence, contract.AttrRevision, contract.AttrTrace:
		return true
	}
	return false
}

// deliverBatch holds the per-batch slices of Deliver, reused
// through a pool: a publisher must not keep them after it
// returns.
type deliverBatch struct {
	msgs []Message
	due  []time.Time
}

var deliverBatches = sync.Pool{New: func() any { return new(deliverBatch) }}

// Deliver publishes due occurrences, in one batch (see
// scheduling.DeliverFunc), and returns one error per
// occurrence. An occurrence the broker did not accept is
// delivered again later; its schedule does not advance until
// it is.
func (b *Bridge) Deliver(ctx context.Context, jobs []*domain.Job) []error {
	batch := deliverBatches.Get().(*deliverBatch)
	defer func() {
		clear(batch.msgs) // drop the references to headers and bodies
		batch.msgs, batch.due = batch.msgs[:0], batch.due[:0]
		deliverBatches.Put(batch)
	}()

	// One publish time for the whole batch.
	now := b.now()
	published := now.UTC().Format(time.RFC3339Nano)
	for _, job := range jobs {
		batch.msgs = append(batch.msgs, b.occurrence(job, published))
		batch.due = append(batch.due, job.RunAt)
	}
	msgs := batch.msgs

	var errs []error
	if bp, ok := b.pub.(BatchPublisher); ok {
		errs = bp.PublishBatch(ctx, msgs)
	} else {
		for i, msg := range msgs {
			if err := b.pub.Publish(ctx, msg); err != nil {
				if errs == nil {
					errs = make([]error, len(msgs))
				}
				errs[i] = err
			}
		}
	}

	var failed int
	var first error
	for i := range msgs {
		if i < len(errs) && errs[i] != nil {
			if failed++; first == nil {
				first = errs[i]
			}
			continue
		}
		b.metrics.observePublishDelay(now.Sub(batch.due[i]))
	}
	if failed > 0 {
		// Once per batch: an outage fails all of it alike. The
		// scheduler retries each with backoff.
		b.metrics.PublishErrors.Add(int64(failed))
		b.log.Warn("publish failed, retrying", logger.Fields{"occurrences": failed, "batch": len(msgs), "error": first.Error()})
	}
	return errs
}

// occurrence builds the message of one occurrence: the stored
// header template plus a deterministic id, the publish time and
// the hydra attributes. The job's RunAt is when it was due.
func (b *Bridge) occurrence(job *domain.Job, published string) Message {
	meta := job.Metadata

	key := job.ID
	if job.ScheduleID != nil {
		key = *job.ScheduleID
	}

	header, slots, err := decodeTemplate(meta[metaHeader], occurrenceSlots)
	if err != nil {
		b.log.Error(err, logger.Fields{"schedule": key})
		header, slots = make(Header, occurrenceSlots), make([]string, occurrenceSlots)
	}

	offset, _ := time.ParseDuration(meta[metaOffset])
	jitter, _ := time.ParseDuration(meta[metaJitter])
	nominal := job.RunAt.Add(-offset - jitter).UTC()
	// Deterministic per occurrence, so the broker drops a
	// repeat: <key>@<nominal Unix nanoseconds>.
	var buf [128]byte
	id := string(strconv.AppendInt(append(append(buf[:0], key...), '@'), nominal.UnixNano(), 10))

	n := b.names
	slots = header.setSlot(n.id, id, slots)
	slots = header.setSlot(n.time, published, slots)
	slots = header.setSlot(n.occurrence, string(nominal.AppendFormat(buf[:0], time.RFC3339Nano)), slots)
	for _, h := range [...]struct{ name, value string }{
		{n.schedule, meta[metaID]},
		{n.revision, meta[metaRevision]},
		{n.trace, meta[metaTrace]},
	} {
		if h.value != "" {
			slots = header.setSlot(h.name, h.value, slots)
		}
	}

	target := meta[metaTarget]
	if target == "" {
		target = job.Name
	}
	return Message{Subject: target, Header: header, Data: job.Data, ID: id}
}
