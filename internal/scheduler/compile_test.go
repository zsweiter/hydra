package scheduler

import (
	"testing"
	"time"

	"github.com/zsweiter/hydra/domain"
)

func mustCompile(t *testing.T, rule domain.ScheduleRule) domain.CompiledSchedule {
	t.Helper()
	c, err := Compile(rule)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return c
}

func TestCompile_NegativeOffsetFiresBeforeOccurrence(t *testing.T) {
	c := mustCompile(t, domain.NewCronScheduleRule("0 10 * * *").WithOffset(-5*time.Minute))

	base := time.Date(2026, 10, 25, 8, 0, 0, 0, time.UTC)
	next, ok := c.Next(base)
	if !ok {
		t.Fatal("expected an occurrence")
	}
	if want := time.Date(2026, 10, 25, 9, 55, 0, 0, time.UTC); !next.Equal(want) {
		t.Fatalf("next = %v, want %v", next, want)
	}

	// Asking from the firing time must advance to the next day,
	// not return the same firing again.
	next, _ = c.Next(next)
	if want := time.Date(2026, 10, 26, 9, 55, 0, 0, time.UTC); !next.Equal(want) {
		t.Fatalf("second next = %v, want %v", next, want)
	}
}

func TestCompile_NegativeOffsetCrossesNominalBoundary(t *testing.T) {
	c := mustCompile(t, domain.NewCronScheduleRule("0 10 * * *").WithOffset(-5*time.Minute))

	// 09:57 is after today's firing (09:55) but before today's
	// nominal occurrence (10:00): the next firing is tomorrow.
	base := time.Date(2026, 10, 25, 9, 57, 0, 0, time.UTC)
	next, _ := c.Next(base)
	if want := time.Date(2026, 10, 26, 9, 55, 0, 0, time.UTC); !next.Equal(want) {
		t.Fatalf("next = %v, want %v", next, want)
	}
}

func TestCompile_PositiveOffset(t *testing.T) {
	c := mustCompile(t, domain.NewCronScheduleRule("0 10 * * *").WithOffset(30*time.Second))

	next, _ := c.Next(time.Date(2026, 10, 25, 10, 0, 10, 0, time.UTC))
	if want := time.Date(2026, 10, 25, 10, 0, 30, 0, time.UTC); !next.Equal(want) {
		t.Fatalf("next = %v, want %v", next, want)
	}
}

func TestCompile_IntervalStartsAtStartAt(t *testing.T) {
	start := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)
	c := mustCompile(t, domain.NewIntervalScheduleRule(time.Hour).WithStartAt(start))

	next, _ := c.Next(start.Add(-3 * time.Hour))
	if !next.Equal(start) {
		t.Fatalf("next = %v, want %v", next, start)
	}

	next, _ = c.Next(start)
	if want := start.Add(time.Hour); !next.Equal(want) {
		t.Fatalf("after start = %v, want %v", next, want)
	}
}

func TestCompile_CronStartAtIsInclusive(t *testing.T) {
	start := time.Date(2026, 10, 25, 10, 0, 0, 0, time.UTC)
	c := mustCompile(t, domain.NewCronScheduleRule("0 10 * * *").WithStartAt(start))

	next, _ := c.Next(start.Add(-48 * time.Hour))
	if !next.Equal(start) {
		t.Fatalf("next = %v, want %v", next, start)
	}
}

func TestCompile_EndAtStopsOccurrences(t *testing.T) {
	end := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)
	c := mustCompile(t, domain.NewIntervalScheduleRule(time.Hour).WithEndAt(end))

	if _, ok := c.Next(end.Add(-30 * time.Minute)); ok {
		t.Fatal("expected no occurrence past EndAt")
	}
	if _, ok := c.Next(end.Add(-90 * time.Minute)); !ok {
		t.Fatal("expected an occurrence before EndAt")
	}
}

func TestCompile_EndAtBoundsNominalNotShiftedTime(t *testing.T) {
	end := time.Date(2026, 10, 25, 10, 0, 0, 0, time.UTC)
	c := mustCompile(t, domain.NewCronScheduleRule("0 10 * * *").
		WithEndAt(end).
		WithOffset(-5*time.Minute))

	next, ok := c.Next(time.Date(2026, 10, 25, 9, 0, 0, 0, time.UTC))
	if !ok {
		t.Fatal("the 10:00 occurrence is inside EndAt and must fire at 09:55")
	}
	if want := time.Date(2026, 10, 25, 9, 55, 0, 0, time.UTC); !next.Equal(want) {
		t.Fatalf("next = %v, want %v", next, want)
	}
}

func TestCompile_RRuleAnchoredAtStartAt(t *testing.T) {
	start := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	c := mustCompile(t, domain.NewRRuleScheduleRule("FREQ=DAILY;INTERVAL=2").WithStartAt(start))

	// Oct 1, 3, 5, ... — Oct 4 must resolve to Oct 5 regardless
	// of when the rule was compiled.
	next, ok := c.Next(time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC))
	if !ok {
		t.Fatal("expected an occurrence")
	}
	if want := time.Date(2026, 10, 5, 9, 0, 0, 0, time.UTC); !next.Equal(want) {
		t.Fatalf("next = %v, want %v", next, want)
	}
}

func TestCompile_CronHonoursTimezone(t *testing.T) {
	c := mustCompile(t, domain.NewCronScheduleRule("0 9 * * *").WithTimezone("America/Santiago"))

	// 2026-10-25 is daylight saving time in Santiago (UTC-3).
	next, _ := c.Next(time.Date(2026, 10, 25, 0, 0, 0, 0, time.UTC))
	if want := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC); !next.Equal(want) {
		t.Fatalf("next = %v, want %v", next.UTC(), want)
	}
}
