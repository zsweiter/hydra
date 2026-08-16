package domain

import (
	"errors"
	"fmt"
	"strings"
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
)

// String returns the text representation of the ScheduleType.
func (t ScheduleType) String() string {
	return string(t)
}

// IsValid reports whether the ScheduleType is supported.
func (t ScheduleType) IsValid() bool {
	switch t {
	case ScheduleCron, ScheduleRRule, ScheduleInterval:
		return true
	default:
		return false
	}
}

// ParseScheduleType converts a string into a ScheduleType.
func ParseScheduleType(s string) (ScheduleType, error) {
	parsed := ScheduleType(strings.ToLower(strings.TrimSpace(s)))
	if !parsed.IsValid() {
		return "", fmt.Errorf("unknown schedule type %q", s)
	}

	return parsed, nil
}

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
