package bridge

import (
	"errors"
	"testing"
	"time"

	"github.com/zsweiter/hydra/domain"
)

func interval(id, rule string, extra ...string) map[string]string {
	m := map[string]string{"id": id, "type": "interval", "rule": rule}
	for i := 0; i+1 < len(extra); i += 2 {
		m[extra[i]] = extra[i+1]
	}
	return m
}

func TestCreateIsIdempotent(t *testing.T) {
	h := newHarness(t)
	def := interval("job", "PT0.04S", "count", "3")

	h.ok(t, msg("s", 1, def, "v1"))
	h.pub.waitFor(t, 2, time.Second)

	// A producer retry: another event, the same definition.
	h.ok(t, msg("s", 2, def, "v1"))
	time.Sleep(300 * time.Millisecond)

	if n := len(h.pub.published()); n != 3 {
		t.Fatalf("published %d, want 3: the repeat must not restart the count", n)
	}
	if n := h.b.Metrics().DefinitionsDuplicate.Load(); n != 1 {
		t.Fatalf("duplicates = %d, want 1", n)
	}
	if sch := h.schedule(t, "s|job"); sch.Version != 2 || Revision(sch) != 1 {
		t.Fatalf("version %d revision %d, want version moved to 2, revision 1", sch.Version, Revision(sch))
	}
	// The version moved: an older cancel is stale.
	h.ok(t, msg("s", 1, map[string]string{"id": "job", "action": "cancel"}, ""))
	if n := h.b.Metrics().DefinitionsStale.Load(); n != 1 {
		t.Fatalf("stale = %d, want 1", n)
	}
}

func TestCreateConflict(t *testing.T) {
	h := newHarness(t)
	h.ok(t, msg("s", 1, interval("job", "PT1H"), "v1"))

	r := h.handle(t, msg("s", 2, interval("job", "PT1H"), "v2"))[0]
	if r.Outcome != Discard || !errors.Is(r.Err, ErrConflict) {
		t.Fatalf("outcome %v %v, want Discard with ErrConflict", r.Outcome, r.Err)
	}
	if string(h.schedule(t, "s|job").Data) != "v1" {
		t.Fatal("a conflicting create overwrote the schedule")
	}
}

func TestReplaceArchivesRevision(t *testing.T) {
	h := newHarness(t)
	h.ok(t, msg("s", 1, interval("job", "PT1H"), "v1"))
	replace := msg("s", 2, interval("job", "PT0.05S", "action", "replace"), "v2")
	replace.Header["ce-traceparent"] = []string{"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}
	h.ok(t, replace)

	live := h.schedule(t, "s|job")
	if Revision(live) != 2 || PreviousRevision(live) != "s|job#1" || TraceID(live) != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Fatalf("live: revision %d previous %q trace %q", Revision(live), PreviousRevision(live), TraceID(live))
	}
	old := h.schedule(t, "s|job#1")
	if old.Status != domain.ScheduleArchived || string(old.Data) != "v1" || TraceID(old) != "evt-1" ||
		old.Metadata[metaSupersededBy] != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Fatalf("archived: %v %s %v", old.Status, old.Data, old.Metadata)
	}

	o := h.pub.waitFor(t, 1, time.Second)[0]
	if string(o.Data) != "v2" || h.b.Attributes(o.Header)["hydrarevision"] != "2" {
		t.Fatalf("occurrence %s revision %s, want v2 revision 2", o.Data, h.b.Attributes(o.Header)["hydrarevision"])
	}

	h.ok(t, msg("s", 3, interval("job", "PT0.05S", "action", "replace"), "v2"))
	if Revision(h.schedule(t, "s|job")) != 2 {
		t.Fatal("an identical replace started a new revision")
	}
}

func TestPauseResumeSkipsPausedOccurrences(t *testing.T) {
	h := newHarness(t)
	h.ok(t, msg("s", 1, interval("job", "PT0.03S", "misfire", "catch_up"), ""))
	h.pub.waitFor(t, 1, time.Second)

	pause := msg("s", 2, map[string]string{"id": "job", "action": "pause"}, "")
	pause.Header["ce-traceparent"] = []string{"00-11111111111111111111111111111111-00f067aa0ba902b7-01"}
	h.ok(t, pause)
	time.Sleep(50 * time.Millisecond)
	paused := len(h.pub.published())
	time.Sleep(300 * time.Millisecond)
	if n := len(h.pub.published()); n != paused {
		t.Fatalf("published %d while paused", n-paused)
	}
	if sch := h.schedule(t, "s|job"); sch.Status != domain.SchedulePaused || sch.Metadata[metaStateTrace] != "11111111111111111111111111111111" {
		t.Fatalf("status %v trace %q", sch.Status, sch.Metadata[metaStateTrace])
	}

	h.ok(t, msg("s", 3, map[string]string{"id": "job", "action": "resume"}, ""))
	h.pub.waitFor(t, paused+1, time.Second)
	time.Sleep(100 * time.Millisecond)
	if n := len(h.pub.published()) - paused; n > 6 {
		t.Fatalf("published %d after resume: the paused occurrences were replayed", n)
	}
}

func TestCancelKeepsHistoryAndRedefines(t *testing.T) {
	h := newHarness(t)
	h.ok(t, msg("s", 1, interval("job", "PT1H"), "v1"))
	h.ok(t, msg("s", 2, map[string]string{"id": "job", "action": "cancel"}, ""))

	if sch := h.schedule(t, "s|job"); sch.Status != domain.ScheduleCancelled || string(sch.Data) != "v1" {
		t.Fatalf("cancelled: %v %s", sch.Status, sch.Data)
	}
	h.ok(t, msg("s", 3, interval("job", "PT1H"), "v1"))
	if live := h.schedule(t, "s|job"); live.Status != domain.ScheduleActive || Revision(live) != 2 {
		t.Fatalf("redefined: %v revision %d", live.Status, Revision(live))
	}
	if old := h.schedule(t, "s|job#1"); old.Status != domain.ScheduleArchived {
		t.Fatalf("revision 1 is %v, want archived", old.Status)
	}
}

func TestDeleteRemovesRevisions(t *testing.T) {
	h := newHarness(t)
	h.ok(t, msg("s", 1, interval("job", "PT1H"), "v1"))
	h.ok(t, msg("s", 2, interval("job", "PT1H", "action", "replace"), "v2"))
	h.ok(t, msg("s", 3, map[string]string{"id": "job", "action": "delete"}, ""))

	sch := h.schedule(t, "s|job")
	if sch.Status != domain.ScheduleCancelled || len(sch.Data) != 0 || sch.Rule.Type != "" || sch.Metadata[metaDeleted] != "true" {
		t.Fatalf("after delete: %+v, want a tombstone without payload", sch)
	}
	if _, err := h.store.LoadSchedule(t.Context(), "s|job#1"); err == nil {
		t.Fatal("archived revision survived the delete")
	}

	// A late older definition stays dead; a newer one starts over.
	h.ok(t, msg("s", 2, map[string]string{"id": "job", "type": "once", "rule": "PT0.01S"}, "v0"))
	h.ok(t, msg("s", 4, map[string]string{"id": "job", "type": "once", "rule": "PT0.01S"}, "v4"))
	o := h.pub.waitFor(t, 1, time.Second)[0]
	if string(o.Data) != "v4" {
		t.Fatalf("published %s, want only the newer definition", o.Data)
	}
	if live := h.schedule(t, "s|job"); Revision(live) != 3 || PreviousRevision(live) != "" {
		t.Fatalf("after delete and create: revision %d previous %q", Revision(live), PreviousRevision(live))
	}
}

func TestCreateAfterFinished(t *testing.T) {
	h := newHarness(t)
	h.ok(t, msg("s", 1, map[string]string{"id": "appt", "type": "once", "rule": "PT0.01S"}, "v1"))
	h.pub.waitFor(t, 1, time.Second)
	deadline := time.Now().Add(time.Second)
	for h.schedule(t, "s|appt").Status != domain.ScheduleFinished {
		if time.Now().After(deadline) {
			t.Fatal("not finished")
		}
		time.Sleep(5 * time.Millisecond)
	}

	h.ok(t, msg("s", 2, map[string]string{"id": "appt", "type": "once", "rule": "PT0.01S"}, "v2"))
	o := h.pub.waitFor(t, 2, time.Second)[1]
	if string(o.Data) != "v2" || h.b.Attributes(o.Header)["hydrarevision"] != "2" {
		t.Fatalf("second occurrence %s revision %s", o.Data, h.b.Attributes(o.Header)["hydrarevision"])
	}
}

func TestStateChangeOfUnknownSchedule(t *testing.T) {
	h := newHarness(t)
	recent := msg("s", 1, map[string]string{"id": "ghost", "action": "pause"}, "")
	recent.Time = time.Now()
	if r := h.handle(t, recent)[0]; r.Outcome != Retry {
		t.Fatalf("recent: %v %v, want Retry", r.Outcome, r.Err)
	}
	old := recent
	old.Time = time.Now().Add(-time.Hour)
	if r := h.handle(t, old)[0]; r.Outcome != Discard || !errors.Is(r.Err, ErrUnknownSchedule) {
		t.Fatalf("old: %v %v, want Discard", r.Outcome, r.Err)
	}
}

// A batch applies the changes of one schedule in order and
// writes only the result.
func TestBatchAppliesInOrder(t *testing.T) {
	h := newHarness(t)
	results := h.handle(t,
		msg("s", 1, interval("a", "PT1H"), "v1"),
		msg("s", 2, map[string]string{"id": "a", "action": "pause"}, ""),
		msg("s", 3, interval("a", "PT1H", "action", "replace"), "v3"),
		msg("s", 4, interval("b", "PT1H"), "b1"),
		msg("s", 5, interval("b", "PT1H"), "b-conflict"),
	)
	want := []Outcome{Ack, Ack, Ack, Ack, Discard}
	for i, r := range results {
		if r.Outcome != want[i] {
			t.Fatalf("message %d: %v %v, want %v", i, r.Outcome, r.Err, want[i])
		}
	}

	a := h.schedule(t, "s|a")
	if string(a.Data) != "v3" || a.Version != 3 || Revision(a) != 2 || a.Status != domain.ScheduleActive {
		t.Fatalf("a = %s version %d revision %d %v", a.Data, a.Version, Revision(a), a.Status)
	}
	if old := h.schedule(t, "s|a#1"); old.Status != domain.ScheduleArchived || string(old.Data) != "v1" {
		t.Fatalf("a#1 = %v %s", old.Status, old.Data)
	}
	m := h.b.Metrics()
	if m.DefinitionsRegistered.Load() != 3 || m.DefinitionsPaused.Load() != 1 || m.DefinitionsConflict.Load() != 1 {
		t.Fatalf("metrics registered %d paused %d conflict %d", m.DefinitionsRegistered.Load(), m.DefinitionsPaused.Load(), m.DefinitionsConflict.Load())
	}
}

// A cancel processed before its definition (another instance)
// leaves a tombstone that rejects the late definition.
func TestCancelBeforeDefinitionWins(t *testing.T) {
	h := newHarness(t)
	h.ok(t, msg("s", 2, map[string]string{"id": "job", "action": "cancel"}, ""))
	h.ok(t, msg("s", 1, interval("job", "PT0.03S"), ""))
	time.Sleep(200 * time.Millisecond)
	if n := len(h.pub.published()); n != 0 {
		t.Fatalf("cancelled schedule published %d", n)
	}
	if n := h.b.Metrics().DefinitionsStale.Load(); n != 1 {
		t.Fatalf("stale = %d, want 1", n)
	}
}

func TestJitterKeepsNominalOccurrence(t *testing.T) {
	if a, b := jitterFor("s|a", time.Minute), jitterFor("s|b", time.Minute); a == b || a >= time.Minute || b >= time.Minute {
		t.Fatalf("jitter %v and %v: want distinct shifts within the minute", a, b)
	}
	h := newHarness(t)
	m := msg("s", 1, map[string]string{"id": "j", "type": "once", "rule": "PT0.05S", "jitter": "PT0.3S"}, "")
	m.Time = time.Now()
	h.ok(t, m)
	o := h.pub.waitFor(t, 1, 2*time.Second)[0]
	at := h.b.Attributes(o.Header)["hydraoccurrence"]
	got, _ := time.Parse(time.RFC3339Nano, at)
	if want := m.Time.Add(50 * time.Millisecond); got.Sub(want).Abs() > time.Millisecond {
		t.Fatalf("hydraoccurrence %v, want the nominal %v without jitter", got, want)
	}
}
