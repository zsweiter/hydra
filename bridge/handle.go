package bridge

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/zsweiter/hydra/contract"
	"github.com/zsweiter/hydra/domain"
	"github.com/zsweiter/hydra/internal/parsers"
	"github.com/zsweiter/hydra/pkg/logger"
	"github.com/zsweiter/hydra/scheduling"
	"github.com/zsweiter/hydra/storage"
)

// Schedule metadata keys. Every revision of a schedule keeps
// the headers and target to publish with, who made it (trace
// id), its number, a fingerprint to recognise repeats, and the
// ID of the revision it replaced, so the history of a
// recurrenceid can be followed back.
const (
	metaHeader       = "bridge.headers" // see encodeTemplate
	metaTarget       = "bridge.target"
	metaID           = "bridge.id"
	metaOffset       = "bridge.offset"
	metaJitter       = "bridge.jitter"
	metaRevision     = "bridge.revision"
	metaFingerprint  = "bridge.fingerprint"
	metaTrace        = "bridge.trace_id"
	metaPrevious     = "bridge.previous"
	metaSupersededBy = "bridge.superseded_by"
	metaSupersededAt = "bridge.superseded_at"
	metaStateTrace   = "bridge.state_trace_id"
	metaStateAction  = "bridge.state_action"
	metaStateAt      = "bridge.state_at"
	metaDeleted      = "bridge.deleted"
)

// Outcome tells an adapter what to do with a message.
type Outcome int

const (
	// Ack: processed, or ignored on purpose.
	Ack Outcome = iota

	// Discard: it breaks the contract; redelivering will not
	// help (JetStream Term).
	Discard

	// Retry: it could not be processed now; redeliver it later
	// (JetStream Nak with delay).
	Retry
)

// Result is the outcome of one message.
type Result struct {
	Outcome Outcome
	Err     error
}

// ErrConflict reports a create for a recurrenceid in use by a
// live schedule with a different definition: the producer must
// send a replace.
var ErrConflict = fmt.Errorf("%w: recurrenceid in use by a different definition (send %s=%s to change it)",
	contract.ErrInvalid, contract.AttrAction, contract.ActionReplace)

// ErrUnknownSchedule reports a state change for a schedule that
// does not exist.
var ErrUnknownSchedule = fmt.Errorf("%w: unknown schedule", contract.ErrInvalid)

// unknownGrace is how long a state change for a schedule not
// found is retried rather than discarded: another instance may
// be processing the create sent just before it.
const unknownGrace = 30 * time.Second

// hasRecurrence reports whether attrs carry a recurrence: the
// mark of a definition. Anything else is ignored, occurrences
// included (they never carry one).
func hasRecurrence(attrs map[string]string) bool {
	for name := range attrs {
		if strings.HasPrefix(name, contract.RecurrencePrefix) {
			return true
		}
	}
	return false
}

// Handle processes a batch of inbound messages and returns one
// result per message. Definitions are read, and every schedule
// they change is written in a single Registry.Save; adapters
// acknowledge the messages only after Handle returns, so a
// definition is acknowledged once it is durable.
//
// Messages without recurrence attributes are acknowledged and
// ignored.
func (b *Bridge) Handle(ctx context.Context, msgs []Message) []Result {
	results := make([]Result, len(msgs))

	t := &txn{bridge: b, states: map[string]*keyState{}}
	var keys []string
	for i, msg := range msgs {
		attrs := b.Attributes(msg.Header)
		if !hasRecurrence(attrs) {
			b.metrics.MessagesIgnored.Add(1)
			b.log.Debug("ignoring message without recurrence", logger.Fields{"subject": msg.Subject, "id": msg.ID})
			continue
		}

		subject, ok := b.Subject(msg.Subject)
		if !ok {
			results[i] = b.discard(msg, fmt.Errorf("%w: definitions are published under %q, not on %s", contract.ErrInvalid, b.prefix, msg.Subject))
			continue
		}
		def, err := contract.ParseDefinition(attrs)
		if err == nil && b.prefix != "" && strings.HasPrefix(def.Target, b.prefix) {
			// Occurrences there would come back as definitions.
			err = fmt.Errorf("%w: %s %s is under the definitions prefix %q", contract.ErrInvalid, contract.AttrTarget, def.Target, b.prefix)
		}
		if err != nil {
			results[i] = b.discard(msg, err)
			continue
		}
		key := ScheduleKey(subject, def.ID)
		if _, ok := t.states[key]; !ok {
			t.states[key] = &keyState{}
			keys = append(keys, key)
		}
		t.items = append(t.items, item{index: i, msg: msg, subject: subject, def: def, attrs: attrs, key: key})
	}
	if len(t.items) == 0 {
		return results
	}

	retryAll := func(err error) []Result {
		b.metrics.StoreErrors.Add(1)
		b.log.Error(err, logger.Fields{"messages": len(t.items)})
		for _, it := range t.items {
			results[it.index] = Result{Outcome: Retry, Err: err}
		}
		return results
	}
	if b.reg == nil {
		return retryAll(errors.New("bridge: no registry"))
	}

	loaded, err := b.reg.Load(ctx, keys)
	if err != nil {
		return retryAll(fmt.Errorf("load schedules: %w", err))
	}
	for _, sch := range loaded {
		if st := t.states[sch.ID]; st != nil {
			st.orig, st.cur = sch, sch
		}
	}

	for _, it := range t.items {
		results[it.index] = t.apply(it)
	}

	writes, owners := t.writes()
	if len(writes) > 0 {
		errs, err := b.reg.Save(ctx, writes)
		if err != nil {
			return retryAll(fmt.Errorf("save schedules: %w", err))
		}
		for w, err := range errs {
			if !errors.Is(err, storage.ErrStaleVersion) {
				continue
			}
			// A newer change was saved meanwhile (another
			// instance): these changes lost, as they would have
			// processed in order.
			for _, i := range owners[w] {
				delete(t.effects, i)
				b.metrics.DefinitionsStale.Add(1)
			}
		}
	}

	if len(t.deletes) > 0 {
		// The tombstone is saved; leftover revisions would only
		// wait for the history purge.
		if err := b.reg.Delete(ctx, t.deletes); err != nil {
			b.metrics.StoreErrors.Add(1)
			b.log.Error(fmt.Errorf("delete revisions: %w", err), logger.Fields{"schedules": len(t.deletes)})
		}
	}

	for _, it := range t.items {
		if e, ok := t.effects[it.index]; ok {
			b.count(e, it)
		}
	}
	return results
}

func (b *Bridge) discard(msg Message, err error) Result {
	b.metrics.DefinitionsInvalid.Add(1)
	b.log.Warn("discarding invalid definition", logger.Fields{"subject": msg.Subject, "id": msg.ID, "error": err.Error()})
	return Result{Outcome: Discard, Err: err}
}

// effect is what a message changed, counted once it is saved.
type effect struct {
	name     string
	revision int
	trace    string
}

func (b *Bridge) count(e effect, it item) {
	fields := logger.Fields{"id": it.key, "trace_id": e.trace}
	switch e.name {
	case "registered":
		b.metrics.DefinitionsRegistered.Add(1)
		if e.revision > 1 {
			b.metrics.DefinitionsReplaced.Add(1)
		}
		fields["revision"] = e.revision
	case "duplicate":
		b.metrics.DefinitionsDuplicate.Add(1)
	case contract.ActionPause:
		b.metrics.DefinitionsPaused.Add(1)
	case contract.ActionResume:
		b.metrics.DefinitionsResumed.Add(1)
	case contract.ActionCancel:
		b.metrics.DefinitionsCancelled.Add(1)
	case contract.ActionDelete:
		b.metrics.DefinitionsDeleted.Add(1)
	}
	b.log.Debug("definition "+e.name, fields)
}

type item struct {
	index   int
	msg     Message
	subject string // msg.Subject without the prefix
	def     *contract.Definition
	attrs   map[string]string
	key     string
}

// keyState is a schedule as the batch changes it: orig as
// stored, cur after the messages applied so far.
type keyState struct {
	orig, cur *domain.JobSchedule
	dirty     bool
	owners    []int // messages whose change is in cur
}

// txn applies a batch of changes in memory, in order, and
// collects the writes.
type txn struct {
	bridge   *Bridge
	states   map[string]*keyState
	items    []item
	archives []*domain.JobSchedule
	deletes  []string
	effects  map[int]effect
}

// writes returns the schedules to save: archived revisions
// first, then the final state of every changed schedule, and
// for each write the messages it carries.
func (t *txn) writes() ([]*domain.JobSchedule, [][]int) {
	writes := slices.Clone(t.archives)
	owners := make([][]int, len(writes))
	now := t.bridge.now()
	for _, it := range t.items {
		st := t.states[it.key]
		if !st.dirty {
			continue
		}
		st.dirty = false
		live := st.cur
		if err := scheduling.Prepare(live, st.orig, now); err != nil {
			// Built from a rule validated in apply: unreachable.
			t.bridge.log.Error(fmt.Errorf("prepare %s: %w", it.key, err))
			continue
		}
		writes = append(writes, live)
		owners = append(owners, st.owners)
	}
	return writes, owners
}

func (t *txn) done(it item, st *keyState, e effect) Result {
	st.dirty = true
	st.owners = append(st.owners, it.index)
	if t.effects == nil {
		t.effects = map[int]effect{}
	}
	t.effects[it.index] = e
	return Result{Outcome: Ack}
}

// apply applies one message to the state of its schedule.
func (t *txn) apply(it item) Result {
	b := t.bridge
	st := t.states[it.key]
	cur := st.cur

	if cur != nil && it.msg.Version > 0 && cur.Version >= it.msg.Version {
		b.metrics.DefinitionsStale.Add(1)
		b.log.Debug("ignoring stale definition", logger.Fields{"id": it.key, "version": it.msg.Version})
		return Result{Outcome: Ack}
	}

	trace := traceID(it.attrs)
	if it.def.ChangesState() {
		return t.changeState(it, st, trace)
	}

	sch, err := b.build(it, trace)
	if err != nil {
		return b.discard(it.msg, err)
	}
	fp := sch.Metadata[metaFingerprint]

	revision := 1
	switch {
	case cur == nil:

	case cur.Status == domain.ScheduleCancelled:
		// A cancelled or deleted schedule is defined anew.
		revision = Revision(cur) + 1
		if !isTombstone(cur) {
			sch.Metadata[metaPrevious] = t.archive(cur, trace)
		}

	case cur.Metadata[metaFingerprint] == fp:
		// The same definition again: a producer retry or a
		// redelivery. Only the version moves, so an older change
		// processed after this one stays stale.
		if it.msg.Version > cur.Version {
			next := clone(cur)
			next.Version = it.msg.Version
			st.cur = next
			return t.done(it, st, effect{name: "duplicate", trace: trace})
		}
		b.metrics.DefinitionsDuplicate.Add(1)
		return Result{Outcome: Ack}

	case it.def.Action == contract.ActionCreate && cur.Status != domain.ScheduleFinished:
		// A live schedule cannot be overwritten by a create; one
		// that ended can be defined anew.
		b.metrics.DefinitionsConflict.Add(1)
		b.log.Warn("rejecting create of a live schedule", logger.Fields{"id": it.key, "trace_id": trace})
		return Result{Outcome: Discard, Err: fmt.Errorf("%w: %s", ErrConflict, it.key)}

	default:
		revision = Revision(cur) + 1
		sch.Metadata[metaPrevious] = t.archive(cur, trace)
	}

	sch.Metadata[metaRevision] = strconv.Itoa(revision)
	st.cur = sch
	return t.done(it, st, effect{name: "registered", revision: revision, trace: trace})
}

// archive keeps a copy of the current revision of a schedule
// before it is replaced, and returns its ID. The ID derives from
// the revision, so archiving again (a redelivery) overwrites the
// same copy.
func (t *txn) archive(cur *domain.JobSchedule, trace string) string {
	old := clone(cur)
	old.ID = revisionID(cur.ID, Revision(cur))
	old.Metadata[metaSupersededBy] = trace
	old.Metadata[metaSupersededAt] = t.bridge.now().UTC().Format(time.RFC3339Nano)
	old.Status = domain.ScheduleArchived
	old.NextRunAt = nil
	old.Version = 0
	t.archives = append(t.archives, old)
	return old.ID
}

// changeState applies pause, resume, cancel and delete.
func (t *txn) changeState(it item, st *keyState, trace string) Result {
	b := t.bridge
	cur := st.cur
	action := it.def.Action

	if action == contract.ActionDelete {
		// Archived revisions are numbered 1 .. current-1.
		if cur != nil {
			for r := 1; r < Revision(cur); r++ {
				t.deletes = append(t.deletes, revisionID(it.key, r))
			}
		}
		st.cur = tombstone(it, cur, trace, action, b.now())
		return t.done(it, st, effect{name: action, trace: trace})
	}

	if cur == nil || isTombstone(cur) {
		if action == contract.ActionCancel {
			// A cancel can be processed before the definition it
			// cancels; the tombstone's version rejects it later.
			st.cur = tombstone(it, cur, trace, action, b.now())
			return t.done(it, st, effect{name: action, trace: trace})
		}
		if !it.msg.Time.IsZero() && b.now().Sub(it.msg.Time) < unknownGrace {
			b.log.Debug("schedule not found yet, redelivering", logger.Fields{"id": it.key, "action": action})
			return Result{Outcome: Retry, Err: fmt.Errorf("%s %s: schedule not found yet", action, it.key)}
		}
		b.metrics.DefinitionsUnknown.Add(1)
		b.log.Warn("discarding change of unknown schedule", logger.Fields{"id": it.key, "action": action})
		return Result{Outcome: Discard, Err: fmt.Errorf("%w %s (%s)", ErrUnknownSchedule, it.key, action)}
	}

	next := clone(cur)
	switch action {
	case contract.ActionPause:
		if cur.Status != domain.ScheduleActive {
			return Result{Outcome: Ack}
		}
		next.Status = domain.SchedulePaused

	case contract.ActionResume:
		if cur.Status != domain.SchedulePaused {
			return Result{Outcome: Ack}
		}
		// Continue after now: occurrences due while paused are
		// skipped, whatever the misfire policy.
		at, ok, err := scheduling.FirstAfter(cur.Rule, b.now())
		if err != nil {
			return b.discard(it.msg, err)
		}
		next.Status, next.NextRunAt = domain.ScheduleActive, &at
		if !ok {
			next.Status, next.NextRunAt = domain.ScheduleFinished, nil
		}

	case contract.ActionCancel:
		if cur.Status == domain.ScheduleCancelled {
			return Result{Outcome: Ack}
		}
		next.Status = domain.ScheduleCancelled
	}

	next.Version = it.msg.Version
	next.Metadata[metaStateAction] = action
	next.Metadata[metaStateTrace] = trace
	next.Metadata[metaStateAt] = b.now().UTC().Format(time.RFC3339Nano)
	st.cur = next
	return t.done(it, st, effect{name: action, trace: trace})
}

// tombstone is a cancelled record without rule or payload: its
// version rejects a definition older than the cancel or delete
// processed after it (another instance, a redelivery), instead
// of reviving the schedule.
func tombstone(it item, cur *domain.JobSchedule, trace, action string, now time.Time) *domain.JobSchedule {
	meta := map[string]string{
		metaStateAction: action,
		metaStateTrace:  trace,
		metaStateAt:     now.UTC().Format(time.RFC3339Nano),
	}
	if cur != nil {
		meta[metaRevision] = strconv.Itoa(Revision(cur))
		if action != contract.ActionDelete {
			meta[metaPrevious] = cur.Metadata[metaPrevious]
		}
	}
	if action == contract.ActionDelete {
		meta[metaDeleted] = "true"
	}
	return &domain.JobSchedule{
		ID:       it.key,
		Name:     it.subject,
		Status:   domain.ScheduleCancelled,
		Version:  it.msg.Version,
		Metadata: meta,
	}
}

// build turns a create or replace into a schedule.
func (b *Bridge) build(it item, trace string) (*domain.JobSchedule, error) {
	d, msg := it.def, it.msg

	anchor := msg.Time
	if anchor.IsZero() {
		anchor = b.now()
	}
	jitter := jitterFor(it.key, d.Jitter)

	var rule domain.ScheduleRule
	switch d.Type {
	case contract.TypeOnce:
		at, err := onceAt(d.Rule, anchor)
		if err != nil {
			return nil, err
		}
		rule = domain.NewOnceScheduleRule(at)
	case contract.TypeInterval:
		every, err := contract.ParseDuration(d.Rule)
		if err != nil || every <= 0 {
			return nil, fmt.Errorf("%w: interval %q must be a positive ISO 8601 duration", contract.ErrInvalid, d.Rule)
		}
		rule = domain.NewIntervalScheduleRule(every)
	case contract.TypeCron:
		rule = domain.NewCronScheduleRule(d.Rule)
	case contract.TypeRRule:
		rule = domain.NewRRuleScheduleRule(d.Rule)
	}

	if d.Type != contract.TypeOnce {
		// Anchoring to the message keeps RRULE and interval
		// occurrences aligned across restarts and redeliveries.
		start := anchor
		if d.Start != nil {
			start = *d.Start
		}
		rule = rule.WithStartAt(start)
		if d.End != nil {
			rule = rule.WithEndAt(*d.End)
		}
		// Default to UTC rather than the host's zone, so every
		// instance computes the same occurrences.
		tz := d.Timezone
		if tz == "" {
			tz = "UTC"
		}
		if _, err := parsers.LoadLocation(tz); err != nil {
			return nil, fmt.Errorf("%w: %s: %v", contract.ErrInvalid, contract.AttrTimezone, err)
		}
		rule = rule.WithTimezone(tz)
	}
	rule = rule.WithOffset(d.Offset + jitter)
	if err := scheduling.Validate(rule); err != nil {
		return nil, fmt.Errorf("%w: %v", contract.ErrInvalid, err)
	}

	target := d.Target
	if target == "" {
		target = it.subject
	}

	// Stored headers are republished as they came, without the
	// recurrence: the definition's own attributes, the event's
	// and the application's. CloudEvents headers are lowercased,
	// and those every occurrence sets are left out, so
	// occurrences set them without looking for other spellings.
	headers := make(Header, len(msg.Header))
	for k, v := range msg.Header {
		if name, ok := b.binding.Attribute(k); ok {
			if strings.HasPrefix(name, contract.RecurrencePrefix) || b.occurrenceAttr(name) {
				continue
			}
			k = strings.ToLower(k)
		}
		headers[k] = append(headers[k], v...)
	}

	sch := domain.NewJobSchedule(it.key, target, msg.Data, rule)
	sch.Metadata = map[string]string{
		metaHeader:      encodeTemplate(headers),
		metaTarget:      target,
		metaID:          d.ID,
		metaOffset:      d.Offset.String(),
		metaJitter:      jitter.String(),
		metaTrace:       trace,
		metaFingerprint: fingerprint(it.attrs, target, msg.Data),
	}
	sch.MisfirePolicy = misfire(d.Misfire)
	sch.MaxJobs = d.Count
	sch.Priority = d.Priority
	sch.Version = msg.Version
	return &sch, nil
}

// onceAt resolves the time of a once rule: an RFC 3339 time, an
// ISO 8601 delay from anchor, or anchor itself when empty.
func onceAt(rule string, anchor time.Time) (time.Time, error) {
	if rule == "" {
		return anchor, nil
	}
	if t, err := time.Parse(time.RFC3339Nano, rule); err == nil {
		return t, nil
	}
	if delay, err := contract.ParseDuration(rule); err == nil {
		return anchor.Add(delay), nil
	}
	return time.Time{}, fmt.Errorf("%w: once rule %q is neither an RFC 3339 time nor an ISO 8601 duration", contract.ErrInvalid, rule)
}

func misfire(policy string) domain.MisfirePolicy {
	switch policy {
	case contract.MisfireSkip:
		return domain.MisfireSkip
	case contract.MisfireCatchUp:
		return domain.MisfireCatchUp
	default:
		return domain.MisfireRunOnce
	}
}

// fingerprint identifies a definition as sent, so a repeat is
// recognised whatever event carries it: its recurrence
// attributes (but the action), what the event is (type,
// source, content type), where it goes and its data. Event ids,
// times and traces differ between retries and are left out.
func fingerprint(attrs map[string]string, target string, data []byte) string {
	names := make([]string, 0, len(attrs))
	for name := range attrs {
		if strings.HasPrefix(name, contract.RecurrencePrefix) && name != contract.AttrAction {
			names = append(names, name)
		}
	}
	slices.Sort(names)

	h := sha256.New()
	for _, name := range names {
		fmt.Fprintf(h, "%s=%s\x00", name, attrs[name])
	}
	fmt.Fprintf(h, "%s\x00%s\x00%s\x00%s\x00", attrs[contract.AttrType], attrs[contract.AttrSource],
		attrs[contract.AttrDataContentType], target)
	h.Write(data)
	return hex.EncodeToString(h.Sum(nil)[:16])
}

// traceID picks who made a change: the trace of a W3C
// traceparent (version-traceid-spanid-flags), else the event id.
func traceID(attrs map[string]string) string {
	if parts := strings.Split(attrs[contract.AttrTraceParent], "-"); len(parts) == 4 && len(parts[1]) == 32 {
		return parts[1]
	}
	return attrs[contract.AttrID]
}

// jitterFor gives each schedule a fixed shift in [0, max),
// derived from its key, so it fires at the same point of the
// spread on every instance.
func jitterFor(key string, max time.Duration) time.Duration {
	steps := uint64(max / time.Millisecond) // stores keep milliseconds
	if steps == 0 {
		return 0
	}
	h := fnv.New64a()
	h.Write([]byte(key))
	return time.Duration(h.Sum64()%steps) * time.Millisecond
}

func clone(sch *domain.JobSchedule) *domain.JobSchedule {
	c := *sch
	c.Metadata = maps.Clone(sch.Metadata)
	if c.Metadata == nil {
		c.Metadata = map[string]string{}
	}
	return &c
}

// isTombstone reports a record kept only for its version: a
// cancel of a schedule never defined, or a deleted one.
func isTombstone(sch *domain.JobSchedule) bool {
	return sch.Rule.Type == "" || sch.Metadata[metaDeleted] == "true"
}

// Revision returns the revision number of a schedule stored by
// the bridge (1 for the first definition).
func Revision(sch *domain.JobSchedule) int {
	n, err := strconv.Atoi(sch.Metadata[metaRevision])
	if err != nil || n < 1 {
		return 1
	}
	return n
}

// PreviousRevision returns the ID of the archived revision a
// schedule replaced, or "".
func PreviousRevision(sch *domain.JobSchedule) string {
	return sch.Metadata[metaPrevious]
}

// TraceID returns the trace id of the change that made the
// schedule's current revision.
func TraceID(sch *domain.JobSchedule) string {
	return sch.Metadata[metaTrace]
}

func itoa(n int) string { return strconv.Itoa(n) }
