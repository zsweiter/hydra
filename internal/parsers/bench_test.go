package parsers

import (
	"testing"
	"time"
)

// ──────────────────────────────────────────────
//  Cron Parser Benchmarks
// ──────────────────────────────────────────────

func BenchmarkCronParse(b *testing.B) {
	p := NewCronParser(Minute | Hour | Dom | Month | Dow)
	for b.Loop() {
		_, err := p.Parse("*/30 * * * *")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCronNext(b *testing.B) {
	p := NewCronParser(Minute | Hour | Dom | Month | Dow)
	sched, err := p.Parse("*/30 * * * *")
	if err != nil {
		b.Fatal(err)
	}
	base := time.Now().UTC()
	for b.Loop() {
		sched.Next(base)
	}
}

func BenchmarkCronComplexParse(b *testing.B) {
	p := NewCronParser(Minute | Hour | Dom | Month | Dow)
	for b.Loop() {
		_, err := p.Parse("0 0 1 1,4,7,10 *")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCronDescriptorParse(b *testing.B) {
	p := NewCronParser(Minute | Hour | Dom | Month | Dow | Descriptor)
	for b.Loop() {
		_, err := p.Parse("@daily")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCronWithSecondsParse(b *testing.B) {
	p := NewCronParser(Second | Minute | Hour | Dom | Month | Dow)
	for b.Loop() {
		_, err := p.Parse("30 */5 * * * *")
		if err != nil {
			b.Fatal(err)
		}
	}
}

// ──────────────────────────────────────────────
//  RRULE Parser Benchmarks
// ──────────────────────────────────────────────

func BenchmarkRRuleDailyParse(b *testing.B) {
	p := NewRRuleParser()
	for b.Loop() {
		_, err := p.Parse("FREQ=DAILY")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRRuleDailyNext(b *testing.B) {
	p := NewRRuleParser()
	sched, err := p.Parse("FREQ=DAILY")
	if err != nil {
		b.Fatal(err)
	}
	base := time.Now().UTC()
	for b.Loop() {
		sched.Next(base)
	}
}

func BenchmarkRRuleComplexParse(b *testing.B) {
	p := NewRRuleParser()
	for b.Loop() {
		_, err := p.Parse("FREQ=WEEKLY;BYDAY=MO,WE,FR;INTERVAL=2")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRRuleComplexNext(b *testing.B) {
	p := NewRRuleParser()
	sched, err := p.Parse("FREQ=WEEKLY;BYDAY=MO,WE,FR")
	if err != nil {
		b.Fatal(err)
	}
	base := time.Now().UTC()
	for b.Loop() {
		sched.Next(base)
	}
}

// ──────────────────────────────────────────────
//  Unified Schedule Parser Benchmarks
// ──────────────────────────────────────────────

func BenchmarkScheduleParserCron(b *testing.B) {
	p := NewScheduleParser()
	for b.Loop() {
		_, err := p.Parse(RecurrenceCron, "0 * * * *")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkScheduleParserRRule(b *testing.B) {
	p := NewScheduleParser()
	for b.Loop() {
		_, err := p.Parse(RecurrenceRrule, "FREQ=DAILY")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkScheduleParserCronNext(b *testing.B) {
	p := NewScheduleParser()
	sched, err := p.Parse(RecurrenceCron, "*/30 * * * *")
	if err != nil {
		b.Fatal(err)
	}
	base := time.Now().UTC()
	for b.Loop() {
		sched.Next(base)
	}
}

func BenchmarkScheduleParserRRuleNext(b *testing.B) {
	p := NewScheduleParser()
	sched, err := p.Parse(RecurrenceRrule, "FREQ=DAILY")
	if err != nil {
		b.Fatal(err)
	}
	base := time.Now().UTC()
	for b.Loop() {
		sched.Next(base)
	}
}
