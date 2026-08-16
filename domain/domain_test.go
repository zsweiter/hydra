package domain

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

const validULID = "01ARZ3NDEKTSV4RRFFQ69G5FAV"

const validULIDHex = "01563e3ab5d3d6764c61efb99302bd5b"

// --- ID: happy cases -------------------------------------------------------

func TestIDGenerate(t *testing.T) {
	id, err := NewID()
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	if id.IsZero() {
		t.Fatal("New() returned the zero value")
	}

	if s := id.String(); len(s) != 32 {
		t.Fatalf("String() length = %d, want 32 (hex ULID)", len(s))
	}

	other, err := NewID()
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	if id.Equals(other) {
		t.Fatal("two consecutive New() calls returned the same ID")
	}
}

func TestIDParse(t *testing.T) {
	id, err := ParseID(validULID)
	if err != nil {
		t.Fatalf("Parse(%q) error: %v", validULID, err)
	}

	if got := id.String(); got != validULIDHex {
		t.Fatalf("String() = %q, want %q", got, validULIDHex)
	}
}

func TestMustNewID(t *testing.T) {
	id := MustNewID()
	if id.IsZero() {
		t.Fatal("MustNewID() returned the zero value")
	}
}

func TestMustParse(t *testing.T) {
	id := MustParseID(validULID)
	if id.String() != validULIDHex {
		t.Fatalf("MustParse round trip = %q, want %q", id.String(), validULIDHex)
	}
}

// --- ID: fail cases --------------------------------------------------------

func TestIDParseErrors(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"empty string", ""},
		{"too short", "abc"},
		{"too long", "012345678901234567890123456789012345"},
		{"overflow first byte", "8ZZZZZZZZZZZZZZZZZZZZZZZZZ"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := ParseID(tt.input); err == nil {
				t.Fatalf("Parse(%q) expected error, got none", tt.input)
			}
		})
	}
}

func TestMustParsePanicsOnInvalid(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("MustParse with invalid input expected panic")
		}
	}()

	MustParseID("not-a-valid-ulid")
}

func TestIDJSONMarshal(t *testing.T) {
	id, err := ParseID(validULID)
	if err != nil {
		t.Fatal(err)
	}

	data, err := id.MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON error: %v", err)
	}

	want := `"` + validULIDHex + `"`
	if string(data) != want {
		t.Fatalf("MarshalJSON = %s, want %s", data, want)
	}
}

func TestIDJSONUnmarshal(t *testing.T) {
	var id ID

	if err := id.UnmarshalJSON([]byte(`"` + validULID + `"`)); err != nil {
		t.Fatalf("UnmarshalJSON error: %v", err)
	}

	if id.String() != validULIDHex {
		t.Fatalf("UnmarshalJSON = %q, want %q", id.String(), validULIDHex)
	}
}

func TestIDJSONUnmarshalErrors(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"invalid ulid", `"not-a-valid-ulid"`},
		{"empty string", `""`},
		{"null", `null`},
		{"not a string", `123`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var id ID
			if err := id.UnmarshalJSON([]byte(tt.input)); err == nil {
				t.Fatalf("UnmarshalJSON(%s) expected error, got none", tt.input)
			}
		})
	}
}

func TestIDValue(t *testing.T) {
	id, err := ParseID(validULID)
	if err != nil {
		t.Fatal(err)
	}

	v, err := id.Value()
	if err != nil {
		t.Fatalf("Value() error: %v", err)
	}

	if v != validULIDHex {
		t.Fatalf("Value() = %v, want %q", v, validULIDHex)
	}
}

func TestIDScan(t *testing.T) {
	t.Run("string", func(t *testing.T) {
		var id ID
		if err := id.Scan(validULID); err != nil {
			t.Fatalf("Scan(string) error: %v", err)
		}
		if id.String() != validULIDHex {
			t.Fatalf("Scan = %q, want %q", id.String(), validULIDHex)
		}
	})

	t.Run("bytes", func(t *testing.T) {
		var id ID
		if err := id.Scan([]byte(validULID)); err != nil {
			t.Fatalf("Scan([]byte) error: %v", err)
		}
		if id.String() != validULIDHex {
			t.Fatalf("Scan = %q, want %q", id.String(), validULIDHex)
		}
	})

	t.Run("nil", func(t *testing.T) {
		var id ID
		if err := id.Scan(nil); err != nil {
			t.Fatalf("Scan(nil) error: %v", err)
		}
		if !id.IsZero() {
			t.Fatal("Scan(nil) did not produce the zero value")
		}
	})
}

func TestIDScanErrors(t *testing.T) {
	tests := []struct {
		name  string
		value any
	}{
		{"invalid string", "not-a-valid-ulid"},
		{"invalid bytes", []byte("bad")},
		{"unsupported type", 42},
		{"bool", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var id ID
			if err := id.Scan(tt.value); err == nil {
				t.Fatalf("Scan(%v) expected error, got none", tt.value)
			}
		})
	}
}

// --- ID: edge cases --------------------------------------------------------

func TestIDZero(t *testing.T) {
	if !IsZero(NewZero()) {
		t.Fatal("IsZero(NewZero()) = false, want true")
	}

	id, err := NewID()
	if err != nil {
		t.Fatal(err)
	}

	if IsZero(id) {
		t.Fatal("IsZero(New()) = true, want false")
	}
}

func TestIDFromString(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		id, err := FromString(validULID)
		if err != nil {
			t.Fatalf("FromString error: %v", err)
		}
		if id.String() != validULIDHex {
			t.Fatalf("FromString = %q, want %q", id.String(), validULIDHex)
		}
	})

	t.Run("invalid", func(t *testing.T) {
		if _, err := FromString("invalid"); err == nil {
			t.Fatal("FromString(invalid) expected error")
		}
	})
}

func TestIDFromStringPtr(t *testing.T) {
	t.Run("nil pointer", func(t *testing.T) {
		if got := FromStringPtr(nil); got != nil {
			t.Fatalf("FromStringPtr(nil) = %v, want nil", got)
		}
	})

	t.Run("valid", func(t *testing.T) {
		ulidStr := validULID
		got := FromStringPtr(&ulidStr)
		if got == nil {
			t.Fatal("FromStringPtr(valid) = nil, want non-nil")
		}
		if got.String() != validULIDHex {
			t.Fatalf("FromStringPtr = %q, want %q", got.String(), validULIDHex)
		}
	})

	t.Run("invalid", func(t *testing.T) {
		s := "invalid"
		if got := FromStringPtr(&s); got != nil {
			t.Fatalf("FromStringPtr(invalid) = %v, want nil", got)
		}
	})
}

func TestIDIfNil(t *testing.T) {
	def := MustParseID(validULID)

	if got := IfNil(nil, def); !got.Equals(def) {
		t.Fatal("IfNil(nil, def) did not return the default")
	}

	other := MustNewID()
	if got := IfNil(&other, def); !got.Equals(other) {
		t.Fatal("IfNil(ptr, def) did not return the pointer value")
	}
}

func TestIDEquals(t *testing.T) {
	a := MustParseID(validULID)
	b := MustParseID(validULID)
	c := MustNewID()

	if !a.Equals(b) {
		t.Fatal("two IDs parsed from the same string must be equal")
	}

	if a.Equals(c) {
		t.Fatal("distinct IDs must not be equal")
	}
}

// --- QueueName -------------------------------------------------------------

func TestQueueNameHash(t *testing.T) {
	tests := []struct {
		name  string
		queue QueueName
	}{
		{"non-empty", "emails"},
		{"empty", ""},
		{"numeric", "queue-42"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hash1 := tt.queue.Hash()
			hash2 := tt.queue.Hash()

			if hash1 != hash2 {
				t.Fatal("Hash must be deterministic")
			}
		})
	}

	if QueueName("emails").Hash() == QueueName("reports").Hash() {
		t.Fatal("expected distinct hashes for distinct queue names")
	}
}

func TestQueueNameString(t *testing.T) {
	q := QueueName("emails")
	if q.String() != "emails" {
		t.Fatalf("String() = %q, want %q", q.String(), "emails")
	}
}

func TestQueueNameBit(t *testing.T) {
	tests := []struct {
		name  string
		queue QueueName
	}{
		{"non-empty", "emails"},
		{"empty", ""},
		{"long", QueueName(strings.Repeat("a", 128))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if bit := tt.queue.Bit(); bit >= 64 {
				t.Fatalf("Bit() = %d, want 0 <= bit < 64", bit)
			}
		})
	}
}

func TestQueueNameShard(t *testing.T) {
	tests := []struct {
		name   string
		queue  QueueName
		shards uint64
	}{
		{"single shard", "emails", 1},
		{"two shards", "reports", 2},
		{"seven shards", "", 7},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for range 100 {
				shard := tt.queue.Shard(tt.shards)
				if shard >= tt.shards {
					t.Fatalf("Shard() = %d, want < %d", shard, tt.shards)
				}
			}

			if got := tt.queue.Shard(tt.shards); got != tt.queue.Shard(tt.shards) {
				t.Fatal("Shard must be deterministic")
			}
		})
	}

	if got := QueueName("emails").Shard(1); got != 0 {
		t.Fatalf("Shard(1) = %d, want 0", got)
	}
}

func TestQueueNameShardPanicsOnZero(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("Shard(0) expected panic")
		}
	}()

	QueueName("emails").Shard(0)
}

// --- Job: happy cases ------------------------------------------------------

func TestJobNewDefaults(t *testing.T) {
	job := NewJob("job-1", "send-email", []byte("payload"),
		map[string]string{"account": "42"})

	if job.ID != "job-1" {
		t.Fatalf("ID = %q, want %q", job.ID, "job-1")
	}

	if job.Status != StatusPending {
		t.Fatalf("Status = %v, want pending", job.Status)
	}

	if job.Attempts != 0 {
		t.Fatalf("Attempts = %d, want 0", job.Attempts)
	}

	if job.MaxAttempts != 3 {
		t.Fatalf("MaxAttempts = %d, want 3", job.MaxAttempts)
	}

	if job.RunAt.IsZero() {
		t.Fatal("RunAt must be set")
	}

	if job.RunAt.Location() != time.UTC {
		t.Fatalf("RunAt location = %v, want UTC", job.RunAt.Location())
	}

	if d := time.Until(job.RunAt); d > time.Second {
		t.Fatalf("RunAt = %v, want near now", job.RunAt)
	}

	if job.Backoff != DefaultBackoff() {
		t.Fatalf("Backoff = %+v, want default %+v", job.Backoff, DefaultBackoff())
	}

	if job.Metadata["account"] != "42" {
		t.Fatalf("Metadata was not preserved: %v", job.Metadata)
	}

	if job.IsScheduled() {
		t.Fatal("NewJob must not be scheduled")
	}
}

func TestJobAddDelay(t *testing.T) {
	job := NewJob("job-1", "delayed", nil, nil)

	job.AddDelay(5 * time.Minute)

	if !job.RunAt.After(time.Now().UTC()) {
		t.Fatalf("RunAt = %v, want in the future", job.RunAt)
	}
}

func TestJobUpdateStatus(t *testing.T) {
	job := NewJob("job-1", "status", nil, nil)

	job.UpdateStatus(StatusProcessing)

	if job.Status != StatusProcessing {
		t.Fatalf("Status = %v, want processing", job.Status)
	}
}

func TestJobIncrementAttempts(t *testing.T) {
	job := NewJob("job-1", "attempts", nil, nil)

	job.IncrementAttempts()
	job.IncrementAttempts()

	if job.Attempts != 2 {
		t.Fatalf("Attempts = %d, want 2", job.Attempts)
	}
}

func TestJobRetryable(t *testing.T) {
	tests := []struct {
		name        string
		attempts    int
		maxAttempts int
		want        bool
	}{
		{"no attempts yet", 0, 3, true},
		{"one retry left", 2, 3, true},
		{"budget exhausted", 3, 3, false},
		{"no budget at all", 0, 0, false},
		{"exceeded budget", 4, 3, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job := NewJob("job-1", "retry", nil, nil)
			job.Attempts = tt.attempts
			job.MaxAttempts = tt.maxAttempts

			if got := job.IsRetryable(); got != tt.want {
				t.Fatalf("IsRetryable() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestJobScheduled(t *testing.T) {
	job := NewJob("job-1", "scheduled", nil, nil)

	if job.IsScheduled() {
		t.Fatal("IsScheduled() = true, want false when ScheduleID is nil")
	}

	scheduleID := "sched-1"
	job.ScheduleID = &scheduleID

	if !job.IsScheduled() {
		t.Fatal("IsScheduled() = false, want true when ScheduleID is set")
	}
}

func TestJobStatusString(t *testing.T) {
	tests := []struct {
		status JobStatus
		want   string
	}{
		{StatusPending, "pending"},
		{StatusProcessing, "processing"},
		{StatusCompleted, "completed"},
		{StatusFailed, "failed"},
		{StatusDeadLetter, "dead_letter"},
		{JobStatus(255), "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.status.String(); got != tt.want {
				t.Fatalf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestStatusFrom(t *testing.T) {
	tests := []struct {
		input string
		want  JobStatus
	}{
		{"pending", StatusPending},
		{"processing", StatusProcessing},
		{"completed", StatusCompleted},
		{"failed", StatusFailed},
		{"dead_letter", StatusDeadLetter},
		{"", StatusUnknown},
		{"bogus", StatusUnknown},
		{"PENDING", StatusPending},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			if got := StatusFrom(tt.input); got != tt.want {
				t.Fatalf("StatusFrom(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

// --- FailDecision ----------------------------------------------------------

func TestComputeFailDecisionRetry(t *testing.T) {
	job := &Job{
		MaxAttempts: 3,
		Backoff:     BackoffPolicy{Type: BackoffFixed, BaseMs: 500, MaxMs: 10_000},
	}
	now := time.Now().UTC()

	decision := ComputeFailDecision(job, now)

	if !decision.Retry {
		t.Fatal("expected retry decision")
	}
	if job.Attempts != 1 {
		t.Fatalf("attempts = %d, want 1", job.Attempts)
	}
	want := now.Add(500 * time.Millisecond)
	if !decision.NextRunAt.Equal(want) {
		t.Fatalf("NextRunAt = %v, want %v", decision.NextRunAt, want)
	}
}

func TestComputeFailDecisionExponential(t *testing.T) {
	job := &Job{
		MaxAttempts: 5,
		Backoff:     BackoffPolicy{Type: BackoffExponential, BaseMs: 100, MaxMs: 10_000},
	}
	now := time.Now().UTC()

	decision := ComputeFailDecision(job, now)

	if !decision.Retry {
		t.Fatal("expected retry decision")
	}
	if job.Attempts != 1 {
		t.Fatalf("attempts = %d, want 1", job.Attempts)
	}
	want := now.Add(100 * time.Millisecond)
	if !decision.NextRunAt.Equal(want) {
		t.Fatalf("NextRunAt = %v, want %v", decision.NextRunAt, want)
	}
}

func TestComputeFailDecisionDeadLetter(t *testing.T) {
	job := &Job{
		MaxAttempts: 1,
		Attempts:    1,
		Backoff:     BackoffPolicy{Type: BackoffFixed, BaseMs: 500, MaxMs: 10_000},
	}

	decision := ComputeFailDecision(job, time.Now().UTC())

	if decision.Retry {
		t.Fatal("expected dead-letter decision")
	}
	if job.Attempts != 2 {
		t.Fatalf("attempts = %d, want 2", job.Attempts)
	}
}

// --- Backoff ---------------------------------------------------------------

func TestBackoffNextDelay(t *testing.T) {
	tests := []struct {
		name    string
		policy  BackoffPolicy
		attempt int
		want    time.Duration
	}{
		{
			name:    "fixed is constant",
			policy:  BackoffPolicy{Type: BackoffFixed, BaseMs: 500, MaxMs: 10_000},
			attempt: 5,
			want:    500 * time.Millisecond,
		},
		{
			name:    "exponential first attempt",
			policy:  BackoffPolicy{Type: BackoffExponential, BaseMs: 100, MaxMs: 10_000},
			attempt: 1,
			want:    100 * time.Millisecond,
		},
		{
			name:    "exponential second attempt",
			policy:  BackoffPolicy{Type: BackoffExponential, BaseMs: 100, MaxMs: 10_000},
			attempt: 2,
			want:    200 * time.Millisecond,
		},
		{
			name:    "exponential is capped by max",
			policy:  BackoffPolicy{Type: BackoffExponential, BaseMs: 100, MaxMs: 10_000},
			attempt: 8,
			want:    10 * time.Second,
		},
		{
			name:    "exponential stays capped on later attempts",
			policy:  BackoffPolicy{Type: BackoffExponential, BaseMs: 100, MaxMs: 10_000},
			attempt: 60,
			want:    10 * time.Second,
		},
		{
			name:    "max lower than base",
			policy:  BackoffPolicy{Type: BackoffExponential, BaseMs: 10_000, MaxMs: 100},
			attempt: 1,
			want:    100 * time.Millisecond,
		},
		{
			name:    "unknown policy yields no delay",
			policy:  BackoffPolicy{},
			attempt: 3,
			want:    0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.policy.NextDelay(tt.attempt); got != tt.want {
				t.Fatalf("NextDelay(%d) = %v, want %v", tt.attempt, got, tt.want)
			}
		})
	}
}

func TestDefaultBackoff(t *testing.T) {
	b := DefaultBackoff()

	if b.Type != BackoffExponential {
		t.Fatalf("Type = %v, want exponential", b.Type)
	}

	if b.BaseMs != 100 || b.MaxMs != 10_000 {
		t.Fatalf("BaseMs/MaxMs = %d/%d, want 100/10000", b.BaseMs, b.MaxMs)
	}
}

func TestBackoffNextDelayPanicsOnZeroAttempt(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("exponential NextDelay(0) expected panic (negative shift)")
		}
	}()

	BackoffPolicy{Type: BackoffExponential, BaseMs: 100, MaxMs: 10_000}.NextDelay(0)
}

// --- JobSchedule: happy cases ----------------------------------------------

func TestJobScheduleNew(t *testing.T) {
	rule := NewCronScheduleRule("0 * * * *")

	schedule := NewJobSchedule("sched-1", "hourly", []byte("payload"), rule)

	if schedule.ID != "sched-1" {
		t.Fatalf("ID = %q, want %q", schedule.ID, "sched-1")
	}

	if schedule.Status != ScheduleActive {
		t.Fatalf("Status = %v, want active", schedule.Status)
	}

	if schedule.MisfirePolicy != MisfireSkip {
		t.Fatalf("MisfirePolicy = %v, want skip", schedule.MisfirePolicy)
	}

	if schedule.MaxJobs != UnlimitedJobSchedules {
		t.Fatalf("MaxJobs = %d, want unlimited (0)", schedule.MaxJobs)
	}

	if schedule.LastRunAt != nil || schedule.NextRunAt != nil {
		t.Fatal("LastRunAt and NextRunAt must be nil for a new schedule")
	}

	if schedule.Metadata == nil {
		t.Fatal("Metadata must be a non-nil map for a new schedule")
	}

	if !schedule.CanGenerate() {
		t.Fatal("a new schedule must be able to generate jobs")
	}
}

func TestJobScheduleSetters(t *testing.T) {
	schedule := NewJobSchedule("sched-1", "setters", nil, NewCronScheduleRule("0 * * * *"))

	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(24 * time.Hour)

	got := schedule.SetStartTime(start).SetEndTime(end).SetTimezone("Europe/Madrid")

	if got != &schedule {
		t.Fatal("setters must return the receiver")
	}

	if schedule.Rule.StartAt == nil || !schedule.Rule.StartAt.Equal(start) {
		t.Fatalf("StartAt = %v, want %v", schedule.Rule.StartAt, start)
	}

	if schedule.Rule.EndAt == nil || !schedule.Rule.EndAt.Equal(end) {
		t.Fatalf("EndAt = %v, want %v", schedule.Rule.EndAt, end)
	}

	if schedule.Rule.Timezone != "Europe/Madrid" {
		t.Fatalf("Timezone = %q, want %q", schedule.Rule.Timezone, "Europe/Madrid")
	}
}

// --- JobSchedule: fail and edge cases --------------------------------------

func TestJobScheduleShouldFinish(t *testing.T) {
	tests := []struct {
		name     string
		mutate   func(*JobSchedule)
		finished bool
	}{
		{
			name:     "fresh schedule is not finished",
			mutate:   func(*JobSchedule) {},
			finished: false,
		},
		{
			name: "finished status",
			mutate: func(s *JobSchedule) {
				s.Status = ScheduleFinished
			},
			finished: true,
		},
		{
			name: "max jobs not reached",
			mutate: func(s *JobSchedule) {
				s.MaxJobs = 5
				s.GeneratedJobs = 4
			},
			finished: false,
		},
		{
			name: "max jobs reached exactly",
			mutate: func(s *JobSchedule) {
				s.MaxJobs = 5
				s.GeneratedJobs = 5
			},
			finished: true,
		},
		{
			name: "max jobs exceeded",
			mutate: func(s *JobSchedule) {
				s.MaxJobs = 5
				s.GeneratedJobs = 9
			},
			finished: true,
		},
		{
			name: "unlimited max jobs",
			mutate: func(s *JobSchedule) {
				s.GeneratedJobs = 1000
			},
			finished: false,
		},
		{
			name: "end time in the past",
			mutate: func(s *JobSchedule) {
				s.SetEndTime(time.Now().UTC().Add(-time.Hour))
			},
			finished: true,
		},
		{
			name: "end time in the future",
			mutate: func(s *JobSchedule) {
				s.SetEndTime(time.Now().UTC().Add(time.Hour))
			},
			finished: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schedule := NewJobSchedule("sched-1", "finish", nil,
				NewCronScheduleRule("0 * * * *"))

			tt.mutate(&schedule)

			if got := schedule.ShouldFinish(); got != tt.finished {
				t.Fatalf("ShouldFinish() = %v, want %v", got, tt.finished)
			}
		})
	}
}

func TestJobScheduleCanGenerate(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*JobSchedule)
		want   bool
	}{
		{
			name:   "active",
			mutate: func(*JobSchedule) {},
			want:   true,
		},
		{
			name: "paused cannot generate",
			mutate: func(s *JobSchedule) {
				s.Status = SchedulePaused
			},
			want: false,
		},
		{
			name: "finished cannot generate",
			mutate: func(s *JobSchedule) {
				s.Status = ScheduleFinished
			},
			want: false,
		},
		{
			name: "max jobs reached cannot generate",
			mutate: func(s *JobSchedule) {
				s.MaxJobs = 2
				s.GeneratedJobs = 2
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schedule := NewJobSchedule("sched-1", "generate", nil,
				NewCronScheduleRule("0 * * * *"))

			tt.mutate(&schedule)

			if got := schedule.CanGenerate(); got != tt.want {
				t.Fatalf("CanGenerate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestScheduleStatusString(t *testing.T) {
	tests := []struct {
		status ScheduleStatus
		want   string
	}{
		{ScheduleActive, "active"},
		{SchedulePaused, "paused"},
		{ScheduleFinished, "finished"},
		{ScheduleStatus(99), "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.status.String(); got != tt.want {
				t.Fatalf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestIsExpired(t *testing.T) {
	past := time.Now().UTC().Add(-time.Hour)
	future := time.Now().UTC().Add(time.Hour)
	justPast := time.Now().UTC().Add(-time.Millisecond)
	justFuture := time.Now().UTC().Add(50 * time.Millisecond)

	if isExpired(nil) {
		t.Fatal("isExpired(nil) = true, want false")
	}

	if !isExpired(&past) {
		t.Fatal("isExpired(past) = false, want true")
	}

	if isExpired(&future) {
		t.Fatal("isExpired(future) = true, want false")
	}

	if !isExpired(&justPast) {
		t.Fatal("isExpired(just past) = false, want true")
	}

	if isExpired(&justFuture) {
		t.Fatal("isExpired(just future) = true, want false")
	}
}

// --- ScheduleRule: happy cases ---------------------------------------------

func TestScheduleRuleConstructors(t *testing.T) {
	t.Run("cron", func(t *testing.T) {
		rule := NewCronScheduleRule("0 * * * *")
		if rule.Type != ScheduleCron {
			t.Fatalf("Type = %q, want cron", rule.Type)
		}
		if rule.Expression != "0 * * * *" {
			t.Fatalf("Expression = %q, want %q", rule.Expression, "0 * * * *")
		}
	})

	t.Run("rrule", func(t *testing.T) {
		rule := NewRRuleScheduleRule("FREQ=DAILY;COUNT=10")
		if rule.Type != ScheduleRRule {
			t.Fatalf("Type = %q, want rrule", rule.Type)
		}
		if rule.Expression != "FREQ=DAILY;COUNT=10" {
			t.Fatalf("Expression = %q, want %q", rule.Expression, "FREQ=DAILY;COUNT=10")
		}
	})

	t.Run("interval", func(t *testing.T) {
		rule := NewIntervalScheduleRule(90 * time.Second)
		if rule.Type != ScheduleInterval {
			t.Fatalf("Type = %q, want interval", rule.Type)
		}
		if rule.IntervalMs != 90_000 {
			t.Fatalf("IntervalMs = %d, want 90000", rule.IntervalMs)
		}
		if rule.IntervalNanos != int64(90*time.Second) {
			t.Fatalf("IntervalNanos = %d, want %d", rule.IntervalNanos, int64(90*time.Second))
		}
	})
}

func TestScheduleRuleWith(t *testing.T) {
	start := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(7 * 24 * time.Hour)

	rule := NewIntervalScheduleRule(time.Minute)
	got := rule.WithTimezone("UTC").WithStartAt(start).WithEndAt(end)

	if rule.Timezone != "" || rule.StartAt != nil || rule.EndAt != nil {
		t.Fatal("With* helpers must not mutate the receiver")
	}

	if got.Timezone != "UTC" {
		t.Fatalf("Timezone = %q, want %q", got.Timezone, "UTC")
	}

	if got.StartAt == nil || !got.StartAt.Equal(start) {
		t.Fatalf("StartAt = %v, want %v", got.StartAt, start)
	}

	if got.EndAt == nil || !got.EndAt.Equal(end) {
		t.Fatalf("EndAt = %v, want %v", got.EndAt, end)
	}
}

func TestScheduleRuleValidate(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(24 * time.Hour)

	tests := []struct {
		name string
		rule ScheduleRule
	}{
		{"cron", NewCronScheduleRule("0 * * * *")},
		{"rrule", NewRRuleScheduleRule("FREQ=DAILY;COUNT=10")},
		{"interval duration", NewIntervalScheduleRule(time.Minute)},
		{"interval nanos only", ScheduleRule{Type: ScheduleInterval, IntervalNanos: 1}},
		{"interval ms only", ScheduleRule{Type: ScheduleInterval, IntervalMs: 100}},
		{"interval nanos precedence", ScheduleRule{
			Type:          ScheduleInterval,
			IntervalMs:    1,
			IntervalNanos: 500,
		}},
		{"with timezone", NewIntervalScheduleRule(time.Minute).WithTimezone("America/New_York")},
		{"start equals end", NewCronScheduleRule("0 * * * *").
			WithStartAt(start).WithEndAt(start)},
		{"bounded window", NewCronScheduleRule("0 * * * *").
			WithStartAt(start).WithEndAt(end)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.rule.Validate(); err != nil {
				t.Fatalf("Validate() error: %v", err)
			}
		})
	}
}

// --- ScheduleRule: fail and edge cases -------------------------------------

func TestScheduleRuleValidateErrors(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(-time.Hour)

	tests := []struct {
		name string
		rule ScheduleRule
	}{
		{"cron without expression", ScheduleRule{Type: ScheduleCron}},
		{"rrule without expression", ScheduleRule{Type: ScheduleRRule}},
		{"interval without interval", ScheduleRule{Type: ScheduleInterval}},
		{"interval zero duration", NewIntervalScheduleRule(0)},
		{"negative interval", NewIntervalScheduleRule(-time.Minute)},
		{"unsupported type", ScheduleRule{Type: "bogus"}},
		{"empty type", ScheduleRule{}},
		{"start after end", NewCronScheduleRule("0 * * * *").
			WithStartAt(start).WithEndAt(end)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.rule.Validate(); err == nil {
				t.Fatalf("Validate() expected error for %+v", tt.rule)
			}
		})
	}
}

// --- JSON round trip through a container type ------------------------------

func TestIDJSONRoundTripWithUnmarshaler(t *testing.T) {
	container := struct {
		ID     ID        `json:"id"`
		RunAt  time.Time `json:"run_at"`
		Active bool      `json:"active"`
	}{
		ID:     MustParseID(validULID),
		RunAt:  time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC),
		Active: true,
	}

	data, err := json.Marshal(container)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	var decoded struct {
		ID     ID        `json:"id"`
		RunAt  time.Time `json:"run_at"`
		Active bool      `json:"active"`
	}

	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	if !decoded.ID.Equals(container.ID) {
		t.Fatalf("ID after round trip = %q, want %q", decoded.ID, container.ID)
	}

	if !decoded.RunAt.Equal(container.RunAt) {
		t.Fatalf("RunAt after round trip = %v, want %v", decoded.RunAt, container.RunAt)
	}

	if decoded.Active != container.Active {
		t.Fatalf("Active after round trip = %v, want %v", decoded.Active, container.Active)
	}
}

// --- Aggressive UTC timestamp assertions -----------------------------------
//
// Hydra is a time-driven scheduler: every timestamp flowing through
// the domain must be in UTC so that comparisons, storage epoch
// conversions, and scheduling math are always timezone-stable.

func assertUTCLocation(t *testing.T, field string, ts time.Time) {
	t.Helper()

	if ts.Location() != time.UTC {
		t.Fatalf("%s location = %v, want UTC", field, ts.Location())
	}
}

func TestJobTimestampsAreUTC(t *testing.T) {
	job := NewJob("job-1", "utc", nil, nil)

	assertUTCLocation(t, "RunAt", job.RunAt)
	assertUTCLocation(t, "CreatedAt", job.CreatedAt)
	assertUTCLocation(t, "UpdatedAt", job.UpdatedAt)

	job.AddDelay(time.Minute)
	assertUTCLocation(t, "RunAt after AddDelay", job.RunAt)

	job.UpdateStatus(StatusProcessing)
	assertUTCLocation(t, "UpdatedAt after UpdateStatus", job.UpdatedAt)

	job.IncrementAttempts()
	assertUTCLocation(t, "UpdatedAt after IncrementAttempts", job.UpdatedAt)
}

func TestJobScheduleTimestampsAreUTC(t *testing.T) {
	schedule := NewJobSchedule("sched-1", "utc", nil, NewIntervalScheduleRule(time.Minute))

	assertUTCLocation(t, "CreatedAt", schedule.CreatedAt)
	assertUTCLocation(t, "UpdatedAt", schedule.UpdatedAt)
}

// --- BackoffType -----------------------------------------------------------

func TestBackoffTypeString(t *testing.T) {
	tests := []struct {
		typ  BackoffType
		want string
	}{
		{BackoffFixed, "fixed"},
		{BackoffExponential, "exponential"},
		{BackoffType(255), "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.typ.String(); got != tt.want {
				t.Fatalf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestParseBackoffType(t *testing.T) {
	tests := []struct {
		input string
		want  BackoffType
		err   bool
	}{
		{"fixed", BackoffFixed, false},
		{"exponential", BackoffExponential, false},
		{"  Fixed  ", BackoffFixed, false},
		{"EXPONENTIAL", BackoffExponential, false},
		{"", 0, true},
		{"bogus", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ParseBackoffType(tt.input)
			if tt.err {
				if err == nil {
					t.Fatalf("ParseBackoffType(%q) = %v, want error", tt.input, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseBackoffType(%q) error: %v", tt.input, err)
			}
			if got != tt.want {
				t.Fatalf("ParseBackoffType(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestBackoffTypeIsValid(t *testing.T) {
	if !BackoffFixed.IsValid() || !BackoffExponential.IsValid() {
		t.Fatal("defined values must be valid")
	}
	if BackoffType(2).IsValid() || BackoffType(255).IsValid() {
		t.Fatal("out-of-range values must be invalid")
	}
}

func TestBackoffTypeJSON(t *testing.T) {
	encoded, err := json.Marshal(BackoffExponential)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}
	if string(encoded) != `"exponential"` {
		t.Fatalf("Marshal = %s, want %q", encoded, `"exponential"`)
	}

	var decoded BackoffType
	if err := json.Unmarshal([]byte(`"fixed"`), &decoded); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
	if decoded != BackoffFixed {
		t.Fatalf("Unmarshal = %v, want BackoffFixed", decoded)
	}

	if err := json.Unmarshal([]byte(`"bogus"`), &decoded); err == nil {
		t.Fatal("Unmarshal of unknown value must error")
	}
}

func TestBackoffTypeValueScan(t *testing.T) {
	if v, err := BackoffExponential.Value(); err != nil || v != "exponential" {
		t.Fatalf("Value() = %v, %v; want %q, nil", v, err, "exponential")
	}

	var b BackoffType
	if err := b.Scan("exponential"); err != nil {
		t.Fatalf("Scan(string) error: %v", err)
	}
	if b != BackoffExponential {
		t.Fatalf("Scan(string) = %v, want BackoffExponential", b)
	}

	if err := b.Scan([]byte("fixed")); err != nil {
		t.Fatalf("Scan([]byte) error: %v", err)
	}
	if b != BackoffFixed {
		t.Fatalf("Scan([]byte) = %v, want BackoffFixed", b)
	}

	if err := b.Scan(int64(1)); err != nil {
		t.Fatalf("Scan(int64) error: %v", err)
	}
	if b != BackoffExponential {
		t.Fatalf("Scan(int64) = %v, want BackoffExponential", b)
	}

	for _, bad := range []any{nil, "bogus", int64(99), 3.14} {
		if err := b.Scan(bad); err == nil {
			t.Fatalf("Scan(%v) must error", bad)
		}
	}
}

// --- ScheduleStatus --------------------------------------------------------

func TestParseScheduleStatus(t *testing.T) {
	tests := []struct {
		input string
		want  ScheduleStatus
		err   bool
	}{
		{"active", ScheduleActive, false},
		{"paused", SchedulePaused, false},
		{"finished", ScheduleFinished, false},
		{"  Active  ", ScheduleActive, false},
		{"", 0, true},
		{"bogus", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ParseScheduleStatus(tt.input)
			if tt.err {
				if err == nil {
					t.Fatalf("ParseScheduleStatus(%q) = %v, want error", tt.input, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseScheduleStatus(%q) error: %v", tt.input, err)
			}
			if got != tt.want {
				t.Fatalf("ParseScheduleStatus(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestScheduleStatusIsValid(t *testing.T) {
	if !ScheduleActive.IsValid() || !SchedulePaused.IsValid() || !ScheduleFinished.IsValid() {
		t.Fatal("defined values must be valid")
	}
	if ScheduleStatus(3).IsValid() || ScheduleStatus(255).IsValid() {
		t.Fatal("out-of-range values must be invalid")
	}
}

func TestScheduleStatusJSON(t *testing.T) {
	encoded, err := json.Marshal(SchedulePaused)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}
	if string(encoded) != `"paused"` {
		t.Fatalf("Marshal = %s, want %q", encoded, `"paused"`)
	}

	var decoded ScheduleStatus
	if err := json.Unmarshal([]byte(`"finished"`), &decoded); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
	if decoded != ScheduleFinished {
		t.Fatalf("Unmarshal = %v, want ScheduleFinished", decoded)
	}

	if err := json.Unmarshal([]byte(`"bogus"`), &decoded); err == nil {
		t.Fatal("Unmarshal of unknown value must error")
	}
}

func TestScheduleStatusValueScan(t *testing.T) {
	if v, err := SchedulePaused.Value(); err != nil || v != "paused" {
		t.Fatalf("Value() = %v, %v; want %q, nil", v, err, "paused")
	}

	var s ScheduleStatus
	if err := s.Scan("active"); err != nil {
		t.Fatalf("Scan(string) error: %v", err)
	}
	if s != ScheduleActive {
		t.Fatalf("Scan(string) = %v, want ScheduleActive", s)
	}

	if err := s.Scan([]byte("finished")); err != nil {
		t.Fatalf("Scan([]byte) error: %v", err)
	}
	if s != ScheduleFinished {
		t.Fatalf("Scan([]byte) = %v, want ScheduleFinished", s)
	}

	if err := s.Scan(int64(1)); err != nil {
		t.Fatalf("Scan(int64) error: %v", err)
	}
	if s != SchedulePaused {
		t.Fatalf("Scan(int64) = %v, want SchedulePaused", s)
	}

	for _, bad := range []any{nil, "bogus", int64(99), 3.14} {
		if err := s.Scan(bad); err == nil {
			t.Fatalf("Scan(%v) must error", bad)
		}
	}
}

// --- MisfirePolicy ---------------------------------------------------------

func TestMisfirePolicyString(t *testing.T) {
	tests := []struct {
		policy MisfirePolicy
		want   string
	}{
		{MisfireSkip, "skip"},
		{MisfireRunOnce, "run_once"},
		{MisfireCatchUp, "catch_up"},
		{MisfirePolicy(255), "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.policy.String(); got != tt.want {
				t.Fatalf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestParseMisfirePolicy(t *testing.T) {
	tests := []struct {
		input string
		want  MisfirePolicy
		err   bool
	}{
		{"skip", MisfireSkip, false},
		{"run_once", MisfireRunOnce, false},
		{"catch_up", MisfireCatchUp, false},
		{"  Catch_Up  ", MisfireCatchUp, false},
		{"", 0, true},
		{"bogus", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ParseMisfirePolicy(tt.input)
			if tt.err {
				if err == nil {
					t.Fatalf("ParseMisfirePolicy(%q) = %v, want error", tt.input, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseMisfirePolicy(%q) error: %v", tt.input, err)
			}
			if got != tt.want {
				t.Fatalf("ParseMisfirePolicy(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestMisfirePolicyIsValid(t *testing.T) {
	if !MisfireSkip.IsValid() || !MisfireRunOnce.IsValid() || !MisfireCatchUp.IsValid() {
		t.Fatal("defined values must be valid")
	}
	if MisfirePolicy(3).IsValid() || MisfirePolicy(255).IsValid() {
		t.Fatal("out-of-range values must be invalid")
	}
}

func TestMisfirePolicyJSON(t *testing.T) {
	encoded, err := json.Marshal(MisfireCatchUp)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}
	if string(encoded) != `"catch_up"` {
		t.Fatalf("Marshal = %s, want %q", encoded, `"catch_up"`)
	}

	var decoded MisfirePolicy
	if err := json.Unmarshal([]byte(`"run_once"`), &decoded); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
	if decoded != MisfireRunOnce {
		t.Fatalf("Unmarshal = %v, want MisfireRunOnce", decoded)
	}

	if err := json.Unmarshal([]byte(`"bogus"`), &decoded); err == nil {
		t.Fatal("Unmarshal of unknown value must error")
	}
}

func TestMisfirePolicyValueScan(t *testing.T) {
	if v, err := MisfireCatchUp.Value(); err != nil || v != "catch_up" {
		t.Fatalf("Value() = %v, %v; want %q, nil", v, err, "catch_up")
	}

	var m MisfirePolicy
	if err := m.Scan("run_once"); err != nil {
		t.Fatalf("Scan(string) error: %v", err)
	}
	if m != MisfireRunOnce {
		t.Fatalf("Scan(string) = %v, want MisfireRunOnce", m)
	}

	if err := m.Scan([]byte("skip")); err != nil {
		t.Fatalf("Scan([]byte) error: %v", err)
	}
	if m != MisfireSkip {
		t.Fatalf("Scan([]byte) = %v, want MisfireSkip", m)
	}

	if err := m.Scan(int64(2)); err != nil {
		t.Fatalf("Scan(int64) error: %v", err)
	}
	if m != MisfireCatchUp {
		t.Fatalf("Scan(int64) = %v, want MisfireCatchUp", m)
	}

	for _, bad := range []any{nil, "bogus", int64(99), 3.14} {
		if err := m.Scan(bad); err == nil {
			t.Fatalf("Scan(%v) must error", bad)
		}
	}
}

// --- ScheduleType ----------------------------------------------------------

func TestScheduleTypeString(t *testing.T) {
	if got := ScheduleCron.String(); got != "cron" {
		t.Fatalf("String() = %q, want %q", got, "cron")
	}
}

func TestParseScheduleType(t *testing.T) {
	tests := []struct {
		input string
		want  ScheduleType
		err   bool
	}{
		{"cron", ScheduleCron, false},
		{"rrule", ScheduleRRule, false},
		{"interval", ScheduleInterval, false},
		{"  Cron  ", ScheduleCron, false},
		{"CRON", ScheduleCron, false},
		{"", "", true},
		{"bogus", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ParseScheduleType(tt.input)
			if tt.err {
				if err == nil {
					t.Fatalf("ParseScheduleType(%q) = %q, want error", tt.input, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseScheduleType(%q) error: %v", tt.input, err)
			}
			if got != tt.want {
				t.Fatalf("ParseScheduleType(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestScheduleTypeIsValid(t *testing.T) {
	if !ScheduleCron.IsValid() || !ScheduleRRule.IsValid() || !ScheduleInterval.IsValid() {
		t.Fatal("defined values must be valid")
	}
	if ScheduleType("bogus").IsValid() || ScheduleType("").IsValid() {
		t.Fatal("undefined values must be invalid")
	}
}
