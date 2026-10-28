package domain

import (
	"errors"
	"time"
)

// ScheduleType identifies the recurrence syntax used by a schedule.
type ScheduleType string

const (
	// ScheduleCron represents a cron expression.
	ScheduleCron ScheduleType = "cron"

	// ScheduleRRule represents an RFC5545 RRULE.
	ScheduleRRule ScheduleType = "rrule"

	// ScheduleInterval represents a fixed interval schedule.
	ScheduleInterval ScheduleType = "interval"

	// ScheduleOnce fires a single time, at StartAt.
	ScheduleOnce ScheduleType = "once"
)

// ScheduleRule is the public recurrence definition.
//
// Only one recurrence model can be active at a time.
//
// Cron schedules use Expression.
//
// RRule schedules use Expression.
//
// Interval schedules use IntervalNanos (or IntervalMs for
// backward compatibility).
//
// Once schedules fire a single time at StartAt.
type ScheduleRule struct {
	Type ScheduleType

	// Expression stores either a cron expression
	// or an RFC5545 RRULE string.
	Expression string

	// IntervalMs is used only when Type is ScheduleInterval.
	// Prefer IntervalNanos for sub-millisecond precision.
	// If IntervalNanos > 0, it takes precedence.
	IntervalMs int64

	// IntervalNanos provides nanosecond precision for
	// sub-millisecond schedule intervals.
	// When set (> 0), takes precedence over IntervalMs.
	IntervalNanos int64

	// Optional schedule start boundary.
	StartAt *time.Time

	// Optional schedule end boundary.
	EndAt *time.Time

	// IANA timezone name.
	//
	// Examples:
	//
	//   UTC
	//   America/New_York
	//   Europe/Madrid
	Timezone string

	// Offset shifts every occurrence by a fixed duration.
	//
	// Negative values fire before the nominal occurrence
	// (e.g. -5m for a reminder five minutes ahead), positive
	// values fire after it. StartAt and EndAt bound nominal
	// occurrences, not shifted ones.
	Offset time.Duration
}

func NewCronScheduleRule(cronExpr string) ScheduleRule {
	return ScheduleRule{
		Type:       ScheduleCron,
		Expression: cronExpr,
	}
}

func NewRRuleScheduleRule(rruleExpr string) ScheduleRule {
	return ScheduleRule{
		Type:       ScheduleRRule,
		Expression: rruleExpr,
	}
}

func NewOnceScheduleRule(at time.Time) ScheduleRule {
	return ScheduleRule{
		Type:    ScheduleOnce,
		StartAt: &at,
	}
}

func NewIntervalScheduleRule(interval time.Duration) ScheduleRule {
	return ScheduleRule{
		Type:          ScheduleInterval,
		IntervalMs:    int64(interval / time.Millisecond),
		IntervalNanos: int64(interval),
	}
}

// Validate validates the schedule definition.
func (r ScheduleRule) Validate() error {
	switch r.Type {
	case ScheduleCron, ScheduleRRule:
		if r.Expression == "" {
			return errors.New("expression is required")
		}

	case ScheduleInterval:
		if r.IntervalNanos <= 0 && r.IntervalMs <= 0 {
			return errors.New("interval must be greater than zero")
		}

	case ScheduleOnce:
		if r.StartAt == nil {
			return errors.New("once requires a start time")
		}

	default:
		return errors.New("unsupported schedule type")
	}

	if r.StartAt != nil &&
		r.EndAt != nil &&
		r.StartAt.After(*r.EndAt) {
		return errors.New("start time cannot be after end time")
	}

	return nil
}

func (r ScheduleRule) WithTimezone(tz string) ScheduleRule {
	r.Timezone = tz
	return r
}

func (r ScheduleRule) WithStartAt(t time.Time) ScheduleRule {
	r.StartAt = &t
	return r
}

func (r ScheduleRule) WithEndAt(t time.Time) ScheduleRule {
	r.EndAt = &t
	return r
}

func (r ScheduleRule) WithOffset(d time.Duration) ScheduleRule {
	r.Offset = d
	return r
}

// Equal reports whether two rules produce the same
// occurrences. Times are compared at millisecond precision,
// the precision stores persist them with.
func (r ScheduleRule) Equal(o ScheduleRule) bool {
	return r.Type == o.Type &&
		r.Expression == o.Expression &&
		r.interval() == o.interval() &&
		r.Timezone == o.Timezone &&
		r.Offset == o.Offset &&
		sameMillis(r.StartAt, o.StartAt) &&
		sameMillis(r.EndAt, o.EndAt)
}

func (r ScheduleRule) interval() time.Duration {
	if r.IntervalNanos > 0 {
		return time.Duration(r.IntervalNanos)
	}

	return time.Duration(r.IntervalMs) * time.Millisecond
}

func sameMillis(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}

	return a.UnixMilli() == b.UnixMilli()
}

// String renders the recurrence for logs and listings: the
// expression, the interval, or the time of a once rule.
func (r ScheduleRule) String() string {
	switch r.Type {
	case ScheduleInterval:
		return r.interval().String()
	case ScheduleOnce:
		if r.StartAt != nil {
			return r.StartAt.UTC().Format(time.RFC3339)
		}
		return ""
	default:
		return r.Expression
	}
}
