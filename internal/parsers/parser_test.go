package parsers

import (
	"testing"
	"time"
)

func TestParseSchedule_Cron(t *testing.T) {
	p := NewScheduleParser()

	schedule, err := p.Parse(RecurrenceCron, "0 * * * *")
	if err != nil {
		t.Fatalf("Failed to parse cron: %v", err)
	}

	base := time.Date(2026, 6, 2, 10, 30, 0, 0, time.UTC)
	next := schedule.Next(base)
	expected := time.Date(2026, 6, 2, 11, 0, 0, 0, time.UTC)
	if next != expected {
		t.Errorf("Expected %v, got %v", expected, next)
	}
}

func TestParseSchedule_RRule(t *testing.T) {
	p := NewScheduleParser()

	schedule, err := p.Parse(RecurrenceRrule, "FREQ=DAILY")
	if err != nil {
		t.Fatalf("Failed to parse rrule: %v", err)
	}

	base := time.Date(2026, 6, 2, 10, 30, 0, 0, time.UTC)
	next := schedule.Next(base)
	if next.Before(base) {
		t.Error("Expected next to be after base")
	}
}

func TestParseSchedule_InvalidType(t *testing.T) {
	p := NewScheduleParser()

	schedule, err := p.Parse(RecurrenceType(99), "")
	if schedule != nil {
		t.Error("Expected nil schedule for unknown type")
	}
	if err == nil {
		t.Error("Expected error for unknown type")
	}
}

func TestParseSchedule_InvalidCron(t *testing.T) {
	p := NewScheduleParser()

	_, err := p.Parse(RecurrenceCron, "invalid")
	if err == nil {
		t.Error("Expected error for invalid cron expression")
	}
}

func TestParseSchedule_InvalidRRule(t *testing.T) {
	p := NewScheduleParser()

	_, err := p.Parse(RecurrenceRrule, "INVALID")
	if err == nil {
		t.Error("Expected error for invalid rrule expression")
	}
}

func TestParseSchedule_EmptyCron(t *testing.T) {
	p := NewScheduleParser()

	_, err := p.Parse(RecurrenceCron, "")
	if err == nil {
		t.Error("Expected error for empty cron expression")
	}
}

func TestParseSchedule_EmptyRRule(t *testing.T) {
	p := NewScheduleParser()

	_, err := p.Parse(RecurrenceRrule, "")
	if err == nil {
		t.Error("Expected error for empty rrule expression")
	}
}

func TestParseSchedule_EdgeValues(t *testing.T) {
	p := NewScheduleParser()
	base := time.Date(2026, 6, 2, 10, 30, 0, 0, time.UTC)

	t.Run("cron with zero-minutes", func(t *testing.T) {
		schedule, err := p.Parse(RecurrenceCron, "0 0 * * *")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		next := schedule.Next(base)
		expected := time.Date(2026, 6, 3, 0, 0, 0, 0, time.UTC)
		if next != expected {
			t.Errorf("Expected %v, got %v", expected, next)
		}
	})

	t.Run("rrule daily", func(t *testing.T) {
		schedule, err := p.Parse(RecurrenceRrule, "FREQ=DAILY")
		if err != nil {
			t.Fatalf("Failed to parse: %v", err)
		}
		next := schedule.Next(base)
		if next.Before(base) {
			t.Error("Expected next to be after base")
		}
	})
}

func TestParseSchedule_UnknownType(t *testing.T) {
	p := NewScheduleParser()

	schedule, err := p.Parse(RecurrenceType(99), "")
	if schedule != nil {
		t.Error("Expected nil schedule for unknown type")
	}
	if err == nil {
		t.Error("Expected error for unknown type")
	}
}

func TestParseSchedule_CronOutOfRange(t *testing.T) {
	p := NewScheduleParser()

	tests := []struct {
		expr string
		desc string
	}{
		{"0 99 * * *", "hour 99"},
		{"0 0 99 * *", "day 99"},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			_, err := p.Parse(RecurrenceCron, tt.expr)
			if err == nil {
				t.Errorf("Expected error for %s", tt.expr)
			}
		})
	}
}

func BenchmarkParseSchedule_Parse_Cron(b *testing.B) {
	p := NewScheduleParser()
	for b.Loop() {
		_, err := p.Parse(RecurrenceCron, "*/30 * * * *")
		if err != nil {
			b.Fatalf("Failed to parse: %v", err)
		}
	}
}

func BenchmarkParseSchedule_Parse_RRule(b *testing.B) {
	p := NewScheduleParser()
	for b.Loop() {
		_, err := p.Parse(RecurrenceRrule, "FREQ=DAILY")
		if err != nil {
			b.Fatalf("Failed to parse: %v", err)
		}
	}
}
