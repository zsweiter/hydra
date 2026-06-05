package scheduler

import (
	"fmt"
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

	switch rule.Type {
	case domain.ScheduleCron:
		sched, err := parser.Parse(parsers.RecurrenceCron, rule.Expression)
		if err != nil {
			return nil, fmt.Errorf("compile cron: %w", err)
		}

		return compiledAdapter{inner: sched}, nil

	case domain.ScheduleRRule:
		sched, err := parser.Parse(parsers.RecurrenceRrule, rule.Expression)
		if err != nil {
			return nil, fmt.Errorf("compile rrule: %w", err)
		}

		return compiledAdapter{inner: sched}, nil

	case domain.ScheduleInterval:
		if rule.IntervalNanos > 0 {
			return intervalSchedule{
				interval: time.Duration(rule.IntervalNanos),
			}, nil
		}
		return intervalSchedule{
			interval: time.Duration(rule.IntervalMs) * time.Millisecond,
		}, nil

	default:
		return nil, fmt.Errorf("unsupported schedule type: %s", rule.Type)
	}
}
