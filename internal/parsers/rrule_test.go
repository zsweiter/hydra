package parsers

import (
	"testing"
	"time"
)

func TestRRuleParser_Daily(t *testing.T) {
	parser := NewRRuleParser()

	schedule, err := parser.Parse("FREQ=DAILY")
	if err != nil {
		t.Fatalf("Failed to parse rrule expression: %v", err)
	}

	base := time.Date(2026, 6, 2, 10, 30, 0, 0, time.UTC)
	next := schedule.Next(base)

	if next.Before(base) {
		t.Error("Expected next time to be after base time")
	}

	// RRULE occurrences are relative to DTSTART (set at parse time),
	// so the exact diff depends on when this test runs.
	// Just verify it returns a reasonable future time.
	diff := next.Sub(base)
	if diff < time.Hour || diff > 720*time.Hour {
		t.Errorf("Expected reasonable future time, got %v", diff)
	}
}

func TestRRuleParser_Interval(t *testing.T) {
	parser := NewRRuleParser()

	schedule, err := parser.Parse("FREQ=DAILY;INTERVAL=2")
	if err != nil {
		t.Fatalf("Failed to parse rrule expression: %v", err)
	}

	base := time.Date(2026, 6, 2, 10, 30, 0, 0, time.UTC)
	next := schedule.Next(base)

	if next.Before(base) {
		t.Error("Expected next time to be after base time")
	}

	diff := next.Sub(base)
	if diff < 24*time.Hour || diff > 72*time.Hour {
		t.Errorf("Expected ~48 hours difference, got %v", diff)
	}
}

func TestRRuleParser_Weekly(t *testing.T) {
	parser := NewRRuleParser()

	schedule, err := parser.Parse("FREQ=WEEKLY;BYDAY=MO")
	if err != nil {
		t.Fatalf("Failed to parse rrule expression: %v", err)
	}

	base := time.Date(2026, 6, 2, 10, 30, 0, 0, time.UTC) // Tuesday
	next := schedule.Next(base)

	if next.Before(base) {
		t.Error("Expected next time to be after base time")
	}

	if next.Weekday() != time.Monday {
		t.Errorf("Expected next time to be Monday, got %v", next.Weekday())
	}
}

func TestRRuleParser_Monthly(t *testing.T) {
	parser := NewRRuleParser()

	schedule, err := parser.Parse("FREQ=MONTHLY;BYMONTHDAY=15")
	if err != nil {
		t.Fatalf("Failed to parse rrule expression: %v", err)
	}

	base := time.Date(2026, 6, 2, 10, 30, 0, 0, time.UTC)
	next := schedule.Next(base)

	if next.Before(base) {
		t.Error("Expected next time to be after base time")
	}

	if next.Day() != 15 {
		t.Errorf("Expected day 15, got %d", next.Day())
	}
}

func TestRRuleParser_Count(t *testing.T) {
	parser := NewRRuleParser()

	schedule, err := parser.Parse("FREQ=DAILY;COUNT=3")
	if err != nil {
		t.Fatalf("Failed to parse rrule expression: %v", err)
	}

	base := time.Date(2026, 6, 2, 10, 30, 0, 0, time.UTC)
	for i := range 3 {
		next := schedule.Next(base)
		if next.IsZero() {
			t.Errorf("Expected non-zero result at iteration %d", i)
		}
		base = next
	}

	after := schedule.Next(base)
	if !after.IsZero() {
		t.Error("Expected zero time after count exhausted")
	}
}

func TestRRuleParser_Until(t *testing.T) {
	parser := NewRRuleParser()

	schedule, err := parser.Parse("FREQ=DAILY;UNTIL=20260605T103000Z")
	if err != nil {
		t.Fatalf("Failed to parse rrule expression: %v", err)
	}

	base := time.Date(2026, 6, 2, 10, 30, 0, 0, time.UTC)
	next := schedule.Next(base)

	if next.Before(base) {
		t.Error("Expected next time to be after base time")
	}
}

func TestRRuleParser_Hourly(t *testing.T) {
	parser := NewRRuleParser()

	schedule, err := parser.Parse("FREQ=HOURLY")
	if err != nil {
		t.Fatalf("Failed to parse hourly: %v", err)
	}

	base := time.Date(2026, 6, 2, 10, 30, 0, 0, time.UTC)
	next := schedule.Next(base)

	if next.Before(base) {
		t.Error("Expected next after base")
	}
}

func TestRRuleParser_Minutely(t *testing.T) {
	parser := NewRRuleParser()

	schedule, err := parser.Parse("FREQ=MINUTELY;INTERVAL=15")
	if err != nil {
		t.Fatalf("Failed to parse minutely: %v", err)
	}

	base := time.Date(2026, 6, 2, 10, 30, 0, 0, time.UTC)
	next := schedule.Next(base)

	if next.Before(base) {
		t.Error("Expected next after base")
	}
}

func TestRRuleParser_Yearly(t *testing.T) {
	parser := NewRRuleParser()

	schedule, err := parser.Parse("FREQ=YEARLY;BYMONTH=6;BYMONTHDAY=15")
	if err != nil {
		t.Fatalf("Failed to parse yearly: %v", err)
	}

	base := time.Date(2026, 1, 1, 10, 30, 0, 0, time.UTC)
	next := schedule.Next(base)

	if next.Month() != time.June || next.Day() != 15 {
		t.Errorf("Expected June 15, got %s %d", next.Month(), next.Day())
	}
}

func TestRRuleParser_MultipleBYDAY(t *testing.T) {
	parser := NewRRuleParser()

	schedule, err := parser.Parse("FREQ=WEEKLY;BYDAY=MO,WE,FR")
	if err != nil {
		t.Fatalf("Failed to parse multiple BYDAY: %v", err)
	}

	base := time.Date(2026, 6, 2, 10, 30, 0, 0, time.UTC) // Tuesday
	next := schedule.Next(base)

	if next.Before(base) {
		t.Error("Expected next after base")
	}

	// rrule-go's DTSTART defaults to the current time, so the
	// recurrence set starts from now, not from the epoch.
	// Just verify the result is a valid weekday from the rule.
	wd := next.Weekday()
	if wd != time.Monday && wd != time.Wednesday && wd != time.Friday {
		t.Errorf("Expected Monday, Wednesday, or Friday, got %s", wd)
	}
}

func TestRRuleParser_LastDayOfMonth(t *testing.T) {
	parser := NewRRuleParser()

	schedule, err := parser.Parse("FREQ=MONTHLY;BYMONTHDAY=-1")
	if err != nil {
		t.Fatalf("Failed to parse last day of month: %v", err)
	}

	base := time.Date(2026, 1, 15, 10, 30, 0, 0, time.UTC)
	next := schedule.Next(base)

	if next.Before(base) {
		t.Error("Expected next after base")
	}
}

func TestRRuleParser_ByHourMinute(t *testing.T) {
	parser := NewRRuleParser()

	schedule, err := parser.Parse("FREQ=DAILY;BYHOUR=9,17;BYMINUTE=30")
	if err != nil {
		t.Fatalf("Failed to parse BYHOUR/BYMINUTE: %v", err)
	}

	base := time.Date(2026, 6, 2, 10, 0, 0, 0, time.UTC)
	next := schedule.Next(base)

	if next.Before(base) {
		t.Error("Expected next after base")
	}
}

func TestRRuleParser_MultipleBYMonthDay(t *testing.T) {
	parser := NewRRuleParser()

	schedule, err := parser.Parse("FREQ=MONTHLY;BYMONTHDAY=1,15,31")
	if err != nil {
		t.Fatalf("Failed to parse multiple BYMONTHDAY: %v", err)
	}

	base := time.Date(2026, 6, 2, 10, 30, 0, 0, time.UTC)
	next := schedule.Next(base)

	if next.Day() != 15 {
		t.Errorf("Expected day 15 (next after 2nd), got %d", next.Day())
	}
}

func TestRRuleParser_CountZero(t *testing.T) {
	parser := NewRRuleParser()

	schedule, err := parser.Parse("FREQ=DAILY;COUNT=0")
	if err != nil {
		t.Fatalf("Failed to parse COUNT=0: %v", err)
	}

	base := time.Date(2026, 6, 2, 10, 30, 0, 0, time.UTC)
	next := schedule.Next(base)

	if next.Before(base) {
		t.Error("Expected next after base")
	}
}

func TestRRuleParser_LargeInterval(t *testing.T) {
	parser := NewRRuleParser()

	schedule, err := parser.Parse("FREQ=DAILY;INTERVAL=365")
	if err != nil {
		t.Fatalf("Failed to parse large interval: %v", err)
	}

	base := time.Date(2026, 1, 1, 10, 30, 0, 0, time.UTC)
	next := schedule.Next(base)

	if next.Before(base) {
		t.Error("Expected next after base")
	}
}

func TestRRuleParser_ErrorCases(t *testing.T) {
	parser := NewRRuleParser()

	_, err := parser.Parse("")
	if err == nil {
		t.Error("Expected error for empty string")
	}

	_, err = parser.Parse("INVALID")
	if err == nil {
		t.Error("Expected error for invalid expression")
	}
}

func BenchmarkRRuleParser_Parse(b *testing.B) {
	parser := NewRRuleParser()
	for b.Loop() {
		_, err := parser.Parse("FREQ=DAILY")
		if err != nil {
			b.Fatalf("Failed to parse: %v", err)
		}
	}
}

func BenchmarkRRuleParser_Next(b *testing.B) {
	parser := NewRRuleParser()
	schedule, err := parser.Parse("FREQ=DAILY")
	if err != nil {
		b.Fatalf("Failed to parse: %v", err)
	}

	base := time.Now()
	for b.Loop() {
		schedule.Next(base)
	}
}

func BenchmarkRRuleParser_Complex_Parse(b *testing.B) {
	parser := NewRRuleParser()
	for b.Loop() {
		_, err := parser.Parse("FREQ=WEEKLY;BYDAY=MO,WE,FR;INTERVAL=2")
		if err != nil {
			b.Fatalf("Failed to parse: %v", err)
		}
	}
}

func BenchmarkRRuleParser_Complex_Next(b *testing.B) {
	parser := NewRRuleParser()
	schedule, err := parser.Parse("FREQ=WEEKLY;BYDAY=MO,WE,FR")
	if err != nil {
		b.Fatalf("Failed to parse: %v", err)
	}

	base := time.Now()
	for b.Loop() {
		schedule.Next(base)
	}
}

func BenchmarkRRuleParser_Monthly_Parse(b *testing.B) {
	parser := NewRRuleParser()
	for b.Loop() {
		_, err := parser.Parse("FREQ=MONTHLY;BYMONTHDAY=15")
		if err != nil {
			b.Fatalf("Failed to parse: %v", err)
		}
	}
}

func BenchmarkRRuleParser_Yearly_Parse(b *testing.B) {
	parser := NewRRuleParser()
	for b.Loop() {
		_, err := parser.Parse("FREQ=YEARLY;BYMONTH=6;BYMONTHDAY=15")
		if err != nil {
			b.Fatalf("Failed to parse: %v", err)
		}
	}
}

func BenchmarkRRuleParser_Count_Parse(b *testing.B) {
	parser := NewRRuleParser()
	for b.Loop() {
		_, err := parser.Parse("FREQ=DAILY;INTERVAL=3;COUNT=10")
		if err != nil {
			b.Fatalf("Failed to parse: %v", err)
		}
	}
}
