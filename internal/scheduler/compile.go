package scheduler

import (
	"fmt"
	"strings"
	"time"

	"github.com/zsweiter/hydra/domain"
	"github.com/zsweiter/hydra/internal/parsers"
)

// compiledAdapter wraps a parsers.Schedule to conform to
// domain.CompiledSchedule.
type compiledAdapter struct {
	inner parsers.Schedule
}

func (c compiledAdapter) Next(after time.Time) (time.Time, bool) {
	next := c.inner.Next(after)
	if next.IsZero() {
		return time.Time{}, false
	}
	return next, true
}

// intervalSchedule implements domain.CompiledSchedule for
// fixed-interval schedules.
type intervalSchedule struct {
	interval time.Duration
}

func (s intervalSchedule) Next(after time.Time) (time.Time, bool) {
	return after.Add(s.interval), true
}

// onceSchedule implements domain.CompiledSchedule for a
// single occurrence.
type onceSchedule struct {
	at time.Time
}

func (s onceSchedule) Next(after time.Time) (time.Time, bool) {
	if after.Before(s.at) {
		return s.at, true
	}

	return time.Time{}, false
}

// boundedSchedule restricts nominal occurrences to the
// [start, end] window of the rule.
//
// For interval rules the first occurrence is start itself;
// for cron and RRULE it is the first match at or after start.
type boundedSchedule struct {
	inner         domain.CompiledSchedule
	start         *time.Time
	end           *time.Time
	startIsFiring bool
}

func (s boundedSchedule) Next(after time.Time) (time.Time, bool) {
	if s.start != nil && after.Before(*s.start) {
		if s.startIsFiring {
			return s.checkEnd(*s.start)
		}
		after = s.start.Add(-time.Nanosecond)
	}

	next, ok := s.inner.Next(after)
	if !ok {
		return time.Time{}, false
	}

	return s.checkEnd(next)
}

func (s boundedSchedule) checkEnd(t time.Time) (time.Time, bool) {
	if s.end != nil && t.After(*s.end) {
		return time.Time{}, false
	}

	return t, true
}

// offsetSchedule shifts every occurrence of inner by offset.
//
// Next is evaluated in shifted time, so the scheduler's
// "after" arguments (which are firing times) map back to
// nominal time before querying inner.
type offsetSchedule struct {
	inner  domain.CompiledSchedule
	offset time.Duration
}

func (s offsetSchedule) Next(after time.Time) (time.Time, bool) {
	next, ok := s.inner.Next(after.Add(-s.offset))
	if !ok {
		return time.Time{}, false
	}

	return next.Add(s.offset), true
}

// shared parser instance for compiling schedules.
var parser parsers.ParseSchedule

func init() {
	parser = parsers.NewScheduleParser()
}

// Compile converts a ScheduleRule into a CompiledSchedule
// that can be used to efficiently compute future occurrences.
//
// The returned CompiledSchedule is safe for repeated use
// and should be cached by the Scheduler.
func Compile(rule domain.ScheduleRule) (domain.CompiledSchedule, error) {
	if err := rule.Validate(); err != nil {
		return nil, err
	}

	var c domain.CompiledSchedule

	switch rule.Type {
	case domain.ScheduleCron:
		expr := rule.Expression
		if rule.Timezone != "" && !strings.HasPrefix(expr, "TZ=") && !strings.HasPrefix(expr, "CRON_TZ=") {
			expr = "CRON_TZ=" + rule.Timezone + " " + expr
		}

		sched, err := parser.Parse(parsers.RecurrenceCron, expr)
		if err != nil {
			return nil, fmt.Errorf("compile cron: %w", err)
		}

		c = compiledAdapter{inner: sched}

	case domain.ScheduleRRule:
		sched, err := parser.Parse(parsers.RecurrenceRrule, rule.Expression)
		if err != nil {
			return nil, fmt.Errorf("compile rrule: %w", err)
		}

		if rs, ok := sched.(parsers.RRuleSchedule); ok && rule.StartAt != nil && !parsers.HasDTStart(rule.Expression) {
			start := *rule.StartAt
			if loc, err := parsers.LoadLocation(rule.Timezone); rule.Timezone != "" && err == nil {
				// BYHOUR and friends are evaluated in DTSTART's zone.
				start = start.In(loc)
			}
			sched = rs.WithDTStart(start)
		}

		c = compiledAdapter{inner: sched}

	case domain.ScheduleInterval:
		interval := time.Duration(rule.IntervalMs) * time.Millisecond
		if rule.IntervalNanos > 0 {
			interval = time.Duration(rule.IntervalNanos)
		}
		c = intervalSchedule{interval: interval}

	case domain.ScheduleOnce:
		// StartAt is the occurrence itself, not a bound.
		c = onceSchedule{at: *rule.StartAt}
		if rule.Offset != 0 {
			c = offsetSchedule{inner: c, offset: rule.Offset}
		}
		return c, nil

	default:
		return nil, fmt.Errorf("unsupported schedule type: %s", rule.Type)
	}

	if rule.StartAt != nil || rule.EndAt != nil {
		c = boundedSchedule{
			inner:         c,
			start:         rule.StartAt,
			end:           rule.EndAt,
			startIsFiring: rule.Type == domain.ScheduleInterval,
		}
	}

	if rule.Offset != 0 {
		c = offsetSchedule{inner: c, offset: rule.Offset}
	}

	return c, nil
}

// FirstRun returns the first firing time of a schedule that
// has never run. A once rule returns its occurrence even when
// it is already past, so the misfire policy decides whether
// it still fires; other rules start from now.
func FirstRun(rule domain.ScheduleRule, c domain.CompiledSchedule, now time.Time) (time.Time, bool) {
	if rule.Type == domain.ScheduleOnce {
		return c.Next(time.Time{})
	}

	return c.Next(now)
}
