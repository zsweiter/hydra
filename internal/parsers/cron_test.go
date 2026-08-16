package parsers

import (
	"slices"
	"testing"
	"time"
)

func TestCronParser_EveryHour(t *testing.T) {
	parser := NewCronParser(Minute | Hour | Dom | Month | Dow)

	schedule, err := parser.Parse("0 * * * *")
	if err != nil {
		t.Fatalf("Failed to parse cron expression: %v", err)
	}

	base := time.Date(2026, 6, 2, 10, 30, 0, 0, time.UTC)
	next := schedule.Next(base)
	expected := time.Date(2026, 6, 2, 11, 0, 0, 0, time.UTC)
	if next != expected {
		t.Errorf("Expected %v, got %v", expected, next)
	}
}

func TestCronParser_Daily(t *testing.T) {
	parser := NewCronParser(Minute | Hour | Dom | Month | Dow)

	schedule, err := parser.Parse("0 0 * * *")
	if err != nil {
		t.Fatalf("Failed to parse cron expression: %v", err)
	}

	base := time.Date(2026, 6, 2, 23, 30, 0, 0, time.UTC)
	next := schedule.Next(base)
	expected := time.Date(2026, 6, 3, 0, 0, 0, 0, time.UTC)
	if next != expected {
		t.Errorf("Expected %v, got %v", expected, next)
	}
}

func TestCronParser_Every30Min(t *testing.T) {
	parser := NewCronParser(Minute | Hour | Dom | Month | Dow)

	schedule, err := parser.Parse("*/30 * * * *")
	if err != nil {
		t.Fatalf("Failed to parse cron expression: %v", err)
	}

	base := time.Date(2026, 6, 2, 10, 10, 0, 0, time.UTC)
	next := schedule.Next(base)
	expected := time.Date(2026, 6, 2, 10, 30, 0, 0, time.UTC)
	if next != expected {
		t.Errorf("Expected %v, got %v", expected, next)
	}
}

func TestCronParser_Descriptor(t *testing.T) {
	parser := NewCronParser(Minute | Hour | Dom | Month | Dow | Descriptor)

	schedule, err := parser.Parse("@daily")
	if err != nil {
		t.Fatalf("Failed to parse descriptor: %v", err)
	}

	base := time.Date(2026, 6, 2, 10, 30, 0, 0, time.UTC)
	next := schedule.Next(base)
	expected := time.Date(2026, 6, 3, 0, 0, 0, 0, time.UTC)
	if next != expected {
		t.Errorf("Expected %v, got %v", expected, next)
	}
}

func TestCronParser_WithSeconds(t *testing.T) {
	parser := NewCronParser(Second | Minute | Hour | Dom | Month | Dow)

	schedule, err := parser.Parse("0 30 * * * *")
	if err != nil {
		t.Fatalf("Failed to parse cron expression: %v", err)
	}

	base := time.Date(2026, 6, 2, 10, 0, 0, 0, time.UTC)
	next := schedule.Next(base)
	expected := time.Date(2026, 6, 2, 10, 30, 0, 0, time.UTC)
	if next != expected {
		t.Errorf("Expected %v, got %v", expected, next)
	}
}

func TestCronParser_ConsecutiveCommas(t *testing.T) {
	parser := NewCronParser(Minute | Hour | Dom | Month | Dow)

	schedule, err := parser.Parse("1,,3 * * * *")
	if err != nil {
		t.Fatalf("Failed to parse consecutive commas: %v", err)
	}

	base := time.Date(2026, 6, 2, 10, 0, 0, 0, time.UTC)
	next := schedule.Next(base)
	if next.Minute() != 1 {
		t.Errorf("Expected minute 1, got %d (skipped consecutive commas)", next.Minute())
	}
}

func TestCronParser_QuestionMark(t *testing.T) {
	parser := NewCronParser(Minute | Hour | Dom | Month | Dow)

	_, err := parser.Parse("0 0 * * ?")
	if err != nil {
		t.Errorf("Question mark should be treated as wildcard: %v", err)
	}
}

func TestCronParser_MonthNames(t *testing.T) {
	parser := NewCronParser(Minute | Hour | Dom | Month | Dow)

	schedule, err := parser.Parse("0 0 1 JAN *")
	if err != nil {
		t.Fatalf("Failed to parse month name: %v", err)
	}

	base := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	next := schedule.Next(base)
	if next.Month() != time.January {
		t.Errorf("Expected January, got %s", next.Month())
	}
}

func TestCronParser_CaseInsensitiveMonth(t *testing.T) {
	parser := NewCronParser(Minute | Hour | Dom | Month | Dow)

	_, err := parser.Parse("0 0 1 jan *")
	if err != nil {
		t.Errorf("Lowercase month names should work: %v", err)
	}

	_, err = parser.Parse("0 0 1 Jan *")
	if err != nil {
		t.Errorf("Capitalized month names should work: %v", err)
	}

	_, err = parser.Parse("0 0 1 JAN *")
	if err != nil {
		t.Errorf("Uppercase month names should work: %v", err)
	}
}

func TestCronParser_WeekdayNames(t *testing.T) {
	parser := NewCronParser(Minute | Hour | Dom | Month | Dow)

	schedule, err := parser.Parse("0 0 * * MON")
	if err != nil {
		t.Fatalf("Failed to parse weekday name: %v", err)
	}

	base := time.Date(2026, 6, 2, 0, 0, 0, 0, time.UTC)
	next := schedule.Next(base)
	if next.Weekday() != time.Monday {
		t.Errorf("Expected Monday, got %s", next.Weekday())
	}
}

func TestCronParser_CaseInsensitiveWeekday(t *testing.T) {
	parser := NewCronParser(Minute | Hour | Dom | Month | Dow)

	_, err := parser.Parse("0 0 * * mon")
	if err != nil {
		t.Errorf("Lowercase weekday should work: %v", err)
	}

	_, err = parser.Parse("0 0 * * Mon")
	if err != nil {
		t.Errorf("Capitalized weekday should work: %v", err)
	}

	_, err = parser.Parse("0 0 * * MON")
	if err != nil {
		t.Errorf("Uppercase weekday should work: %v", err)
	}
}

func TestCronParser_ComplexCombined(t *testing.T) {
	parser := NewCronParser(Minute | Hour | Dom | Month | Dow)

	schedule, err := parser.Parse("1-10/2,15,20-30/5 * * * *")
	if err != nil {
		t.Fatalf("Failed to parse combined range/step/values: %v", err)
	}

	base := time.Date(2026, 6, 2, 10, 0, 0, 0, time.UTC)
	next := schedule.Next(base)

	validMinutes := []int{1, 3, 5, 7, 9, 15, 20, 25, 30}
	found := slices.Contains(validMinutes, next.Minute())
	if !found {
		t.Errorf("Expected minute in %v, got %d", validMinutes, next.Minute())
	}
}

func TestCronParser_AllDescriptors(t *testing.T) {
	parser := NewCronParser(Minute | Hour | Dom | Month | Dow | Descriptor)
	base := time.Date(2026, 6, 2, 10, 30, 0, 0, time.UTC)

	tests := []struct {
		expr     string
		desc     string
		expected func(time.Time) bool
	}{
		{"@yearly", "@yearly", func(t time.Time) bool {
			return t.Month() == time.January && t.Day() == 1 && t.Hour() == 0 && t.Minute() == 0
		}},
		{"@annually", "@annually", func(t time.Time) bool {
			return t.Month() == time.January && t.Day() == 1 && t.Hour() == 0 && t.Minute() == 0
		}},
		{"@monthly", "@monthly", func(t time.Time) bool { return t.Day() == 1 && t.Hour() == 0 && t.Minute() == 0 }},
		{"@weekly", "@weekly", func(t time.Time) bool { return t.Weekday() == time.Sunday && t.Hour() == 0 && t.Minute() == 0 }},
		{"@daily", "@daily", func(t time.Time) bool { return t.Hour() == 0 && t.Minute() == 0 }},
		{"@midnight", "@midnight", func(t time.Time) bool { return t.Hour() == 0 && t.Minute() == 0 }},
		{"@hourly", "@hourly", func(t time.Time) bool { return t.Minute() == 0 && t.Second() == 0 }},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			schedule, err := parser.Parse(tt.expr)
			if err != nil {
				t.Fatalf("Failed to parse %s: %v", tt.expr, err)
			}
			next := schedule.Next(base)
			if !tt.expected(next) {
				t.Errorf("%s: got %v, doesn't match expected pattern", tt.expr, next)
			}
		})
	}
}

func TestCronParser_EveryDescriptor(t *testing.T) {
	parser := NewCronParser(Minute | Hour | Dom | Month | Dow | Descriptor)
	base := time.Date(2026, 6, 2, 10, 30, 0, 0, time.UTC)

	schedule, err := parser.Parse("@every 5m")
	if err != nil {
		t.Fatalf("Failed to parse @every 5m: %v", err)
	}

	next := schedule.Next(base)
	diff := next.Sub(base)
	if diff < 4*time.Minute || diff > 6*time.Minute {
		t.Errorf("Expected ~5 minutes, got %v", diff)
	}
}

func TestCronParser_ErrorCases(t *testing.T) {
	parser := NewCronParser(Minute | Hour | Dom | Month | Dow)

	_, err := parser.Parse("")
	if err == nil {
		t.Error("Expected error for empty string")
	}

	_, err = parser.Parse("invalid")
	if err == nil {
		t.Error("Expected error for invalid expression")
	}

	_, err = parser.Parse("60 * * * *")
	if err == nil {
		t.Error("Expected error for invalid minute value")
	}
}

func TestCronParser_ErrorOutOfRange(t *testing.T) {
	parser := NewCronParser(Minute | Hour | Dom | Month | Dow)

	tests := []struct {
		expr string
		desc string
	}{
		{"-1 * * * *", "negative minute"},
		{"0 24 * * *", "hour 24"},
		{"0 0 32 * *", "day 32"},
		{"0 0 0 * *", "day 0"},
		{"0 0 * 13 *", "month 13"},
		{"0 0 * 0 *", "month 0"},
		{"0 0 * * 7", "day of week 7"},
		{"* * * * 7", "day of week 7 (wildcard)"},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			_, err := parser.Parse(tt.expr)
			if err == nil {
				t.Errorf("Expected error for %s (%s)", tt.expr, tt.desc)
			}
		})
	}
}

func TestCronParser_ErrorNegativeStep(t *testing.T) {
	parser := NewCronParser(Minute | Hour | Dom | Month | Dow)

	_, err := parser.Parse("*/ -1 * * *")
	if err == nil {
		t.Error("Expected error for negative step")
	}
}

func TestCronParser_ErrorZeroStep(t *testing.T) {
	parser := NewCronParser(Minute | Hour | Dom | Month | Dow)

	_, err := parser.Parse("*/0 * * * *")
	if err == nil {
		t.Error("Expected error for zero step")
	}
}

func TestCronParser_ErrorTooManyHyphens(t *testing.T) {
	parser := NewCronParser(Minute | Hour | Dom | Month | Dow)

	_, err := parser.Parse("1-5-10 * * * *")
	if err == nil {
		t.Error("Expected error for too many hyphens")
	}
}

func TestCronParser_ErrorTooManySlashes(t *testing.T) {
	parser := NewCronParser(Minute | Hour | Dom | Month | Dow)

	_, err := parser.Parse("1/2/3 * * * *")
	if err == nil {
		t.Error("Expected error for too many slashes")
	}
}

func TestCronParser_ErrorWrongFieldCount(t *testing.T) {
	parser := NewCronParser(Minute | Hour | Dom | Month | Dow)

	_, err := parser.Parse("* * *")
	if err == nil {
		t.Error("Expected error for too few fields")
	}

	_, err = parser.Parse("* * * * * *")
	if err == nil {
		t.Error("Expected error for too many fields")
	}
}

func TestCronParser_YearBoundary(t *testing.T) {
	parser := NewCronParser(Minute | Hour | Dom | Month | Dow)

	schedule, err := parser.Parse("0 0 1 1 *")
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	base := time.Date(2026, 12, 31, 23, 30, 0, 0, time.UTC)
	next := schedule.Next(base)
	expected := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	if next != expected {
		t.Errorf("Expected %v (next year), got %v", expected, next)
	}
}

func TestCronParser_FiveYearLimit(t *testing.T) {
	parser := NewCronParser(Minute | Hour | Dom | Month | Dow)

	schedule, err := parser.Parse("0 0 29 2 *")
	if err != nil {
		t.Fatalf("Failed to parse leap year: %v", err)
	}

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	next := schedule.Next(base)
	if next.IsZero() {
		t.Log("No leap day found within 5 years (only 2028 is valid)")
	} else if next.Year() != 2028 {
		t.Errorf("Expected next leap day in 2028, got %d", next.Year())
	}
}

func TestCronParser_EndOfMonth(t *testing.T) {
	parser := NewCronParser(Minute | Hour | Dom | Month | Dow)

	schedule, err := parser.Parse("0 0 31 * *")
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	next := schedule.Next(base)
	if next.Day() != 31 {
		t.Errorf("Expected day 31, got %d", next.Day())
	}

	next2 := schedule.Next(next.Add(24 * time.Hour))
	if next2.Month() != time.March || next2.Day() != 31 {
		t.Errorf("Expected March 31 (skip February), got %s %d", next2.Month(), next2.Day())
	}
}

func TestCronParser_StepWithSingleDigit(t *testing.T) {
	parser := NewCronParser(Minute | Hour | Dom | Month | Dow)

	schedule, err := parser.Parse("5/10 * * * *")
	if err != nil {
		t.Fatalf("Failed to parse N/step: %v", err)
	}

	base := time.Date(2026, 6, 2, 10, 0, 0, 0, time.UTC)
	next := schedule.Next(base)
	if next.Minute() != 5 {
		t.Errorf("Expected minute 5 (first step after 0), got %d", next.Minute())
	}
}

func TestCronParser_SecondOptional(t *testing.T) {
	parser := NewCronParser(SecondOptional | Minute | Hour | Dom | Month | Dow)

	schedule, err := parser.Parse("30 * * * *")
	if err != nil {
		t.Fatalf("Failed to parse with optional seconds: %v", err)
	}

	base := time.Date(2026, 6, 2, 10, 0, 0, 0, time.UTC)
	next := schedule.Next(base)
	if next.Minute() != 30 || next.Second() != 0 {
		t.Errorf("Expected :30 with 0 seconds, got %d:%d", next.Minute(), next.Second())
	}
}

func TestCronParser_DowOptional(t *testing.T) {
	parser := NewCronParser(Minute | Hour | Dom | Month | DowOptional)

	schedule, err := parser.Parse("0 0 1 *")
	if err != nil {
		t.Fatalf("Failed to parse with optional DOW: %v", err)
	}

	base := time.Date(2026, 6, 2, 0, 0, 0, 0, time.UTC)
	next := schedule.Next(base)
	expected := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	if next != expected {
		t.Errorf("Expected %v (July 1st), got %v", expected, next)
	}
}

func TestCronParser_HourBoundaryWrap(t *testing.T) {
	parser := NewCronParser(Minute | Hour | Dom | Month | Dow)

	schedule, err := parser.Parse("0 23 * * *")
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	base := time.Date(2026, 6, 2, 23, 30, 0, 0, time.UTC)
	next := schedule.Next(base)
	expected := time.Date(2026, 6, 3, 23, 0, 0, 0, time.UTC)
	if next != expected {
		t.Errorf("Expected %v, got %v", expected, next)
	}
}

func TestCronParser_MultipleOptionalsPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic for multiple optionals")
		}
	}()

	NewCronParser(SecondOptional | Minute | Hour | Dom | Month | DowOptional)
}

func TestCronParser_TimezoneError(t *testing.T) {
	parser := NewCronParser(Minute | Hour | Dom | Month | Dow)

	_, err := parser.Parse("CRON_TZ=Invalid/Timezone 0 0 * * *")
	if err == nil {
		t.Error("Expected error for invalid timezone")
	}
}

func TestCronParser_EveryInvalidDuration(t *testing.T) {
	parser := NewCronParser(Minute | Hour | Dom | Month | Dow | Descriptor)

	_, err := parser.Parse("@every invalid")
	if err == nil {
		t.Error("Expected error for invalid @every duration")
	}
}

func TestCronParser_DescriptorWithoutFlag(t *testing.T) {
	parser := NewCronParser(Minute | Hour | Dom | Month | Dow)

	_, err := parser.Parse("@daily")
	if err == nil {
		t.Error("Expected error for descriptor without Descriptor flag")
	}
}

func BenchmarkCronParser_Parse(b *testing.B) {
	parser := NewCronParser(Minute | Hour | Dom | Month | Dow)
	for b.Loop() {
		_, err := parser.Parse("*/30 * * * *")
		if err != nil {
			b.Fatalf("Failed to parse: %v", err)
		}
	}
}

func BenchmarkCronParser_Next(b *testing.B) {
	parser := NewCronParser(Minute | Hour | Dom | Month | Dow)
	schedule, err := parser.Parse("*/30 * * * *")
	if err != nil {
		b.Fatalf("Failed to parse: %v", err)
	}

	base := time.Now().UTC()
	for b.Loop() {
		schedule.Next(base)
	}
}

func BenchmarkCronParser_WithSeconds_Parse(b *testing.B) {
	parser := NewCronParser(Second | Minute | Hour | Dom | Month | Dow)
	for b.Loop() {
		_, err := parser.Parse("0 */30 * * * *")
		if err != nil {
			b.Fatalf("Failed to parse: %v", err)
		}
	}
}

func BenchmarkCronParser_Complex_Parse(b *testing.B) {
	parser := NewCronParser(Minute | Hour | Dom | Month | Dow)
	for b.Loop() {
		_, err := parser.Parse("0 0 1,15 * 3")
		if err != nil {
			b.Fatalf("Failed to parse: %v", err)
		}
	}
}

func BenchmarkCronParser_Every_Parse(b *testing.B) {
	parser := NewCronParser(Minute | Hour | Dom | Month | Dow | Descriptor)
	for b.Loop() {
		_, err := parser.Parse("@every 5m")
		if err != nil {
			b.Fatalf("Failed to parse: %v", err)
		}
	}
}

func BenchmarkCronParser_Descriptor_Parse(b *testing.B) {
	parser := NewCronParser(Minute | Hour | Dom | Month | Dow | Descriptor)
	for b.Loop() {
		_, err := parser.Parse("@daily")
		if err != nil {
			b.Fatalf("Failed to parse: %v", err)
		}
	}
}

func BenchmarkCronParser_Timezone_Parse(b *testing.B) {
	parser := NewCronParser(Minute | Hour | Dom | Month | Dow)
	for b.Loop() {
		_, err := parser.Parse("CRON_TZ=America/New_York 0 0 * * *")
		if err != nil {
			b.Fatalf("Failed to parse: %v", err)
		}
	}
}

func BenchmarkCronParser_Range_Parse(b *testing.B) {
	parser := NewCronParser(Minute | Hour | Dom | Month | Dow)
	for b.Loop() {
		_, err := parser.Parse("0-30 9-17 * * 1-5")
		if err != nil {
			b.Fatalf("Failed to parse: %v", err)
		}
	}
}

func BenchmarkCronParser_NamedMonths_Parse(b *testing.B) {
	parser := NewCronParser(Minute | Hour | Dom | Month | Dow)
	for b.Loop() {
		_, err := parser.Parse("0 0 1 jan,jun *")
		if err != nil {
			b.Fatalf("Failed to parse: %v", err)
		}
	}
}
