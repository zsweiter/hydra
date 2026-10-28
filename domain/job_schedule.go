package domain

import "time"

type ScheduleStatus uint8

const (
	ScheduleActive ScheduleStatus = iota
	SchedulePaused
	ScheduleFinished

	// ScheduleCancelled is a tombstone: the schedule was
	// removed, but the record and its Version are kept so an
	// older definition arriving late cannot revive it.
	ScheduleCancelled

	// ScheduleArchived is a past revision of a schedule that
	// was replaced, kept as history under its own ID.
	ScheduleArchived
)

func (s ScheduleStatus) String() string {
	switch s {
	case ScheduleActive:
		return "active"
	case SchedulePaused:
		return "paused"
	case ScheduleFinished:
		return "finished"
	case ScheduleCancelled:
		return "cancelled"
	case ScheduleArchived:
		return "archived"
	default:
		return "unknown"
	}
}

// MisfirePolicy defines how the scheduler handles
// missed schedule occurrences (e.g. after downtime).
type MisfirePolicy uint8

const (
	// MisfireSkip ignores all missed occurrences and
	// advances to the next scheduled time.
	MisfireSkip MisfirePolicy = iota

	// MisfireRunOnce executes a single job immediately
	// for the most recent missed occurrence, then
	// resumes the normal schedule.
	MisfireRunOnce

	// MisfireCatchUp executes a job for every missed
	// occurrence up to the current time, then resumes
	// the normal schedule.
	MisfireCatchUp
)

// JobSchedule defines a recurring job template.
//
// A JobSchedule never executes work itself.
// Instead, it generates Job instances according
// to its recurrence rule.
type JobSchedule struct {
	ID            string
	Name          string
	Queue         QueueName
	Data          []byte
	Metadata      map[string]string
	Rule          ScheduleRule
	Status        ScheduleStatus
	MisfirePolicy MisfirePolicy
	LastRunAt     *time.Time
	NextRunAt     *time.Time
	Priority      int
	MaxJobs       int
	GeneratedJobs int

	// Version orders concurrent writes of the same schedule,
	// e.g. a broker sequence number. When non-zero, saving a
	// schedule whose Version is not greater than the stored
	// one is rejected, so an out-of-order update or a
	// redelivery cannot overwrite newer state. Zero disables
	// the check.
	Version int64

	CreatedAt time.Time
	UpdatedAt time.Time
}

func NewJobSchedule(id, name string, payload []byte, rule ScheduleRule) JobSchedule {
	now := time.Now().UTC()

	return JobSchedule{
		ID:            id,
		Name:          name,
		Data:          payload,
		Metadata:      make(map[string]string),
		Status:        ScheduleActive,
		MisfirePolicy: MisfireSkip,
		LastRunAt:     nil,
		NextRunAt:     nil,
		Priority:      0,
		MaxJobs:       0, // 0 = unlimited
		Rule:          rule,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
}

func (s *JobSchedule) SetStartTime(t time.Time) *JobSchedule {
	s.Rule.StartAt = &t
	return s
}

func (s *JobSchedule) SetEndTime(t time.Time) *JobSchedule {
	s.Rule.EndAt = &t
	return s
}

func (s *JobSchedule) SetTimezone(tz string) *JobSchedule {
	s.Rule.Timezone = tz
	return s
}

// IsFinished reports whether the schedule
// can generate additional jobs.
func (s *JobSchedule) ShouldFinish() bool {
	if s.Status == ScheduleFinished || s.Status == ScheduleCancelled || s.Status == ScheduleArchived {
		return true
	}

	if s.MaxJobs > 0 && s.GeneratedJobs >= s.MaxJobs {
		return true
	}

	if s.Rule.EndAt != nil &&
		time.Now().After(*s.Rule.EndAt) {
		return true
	}

	return false
}

// CanGenerate reports whether a new job
// can be produced.
func (s *JobSchedule) CanGenerate() bool {
	return !s.ShouldFinish() && s.Status == ScheduleActive
}

// CompiledSchedule is an optimized recurrence
// calculator used internally by the scheduler.
//
// Implementations should avoid reparsing cron,
// interval, or RRULE definitions on every call.
type CompiledSchedule interface {

	// Next returns the next occurrence after
	// the supplied timestamp.
	//
	// The boolean return value is false when
	// no future occurrence exists.
	Next(after time.Time) (time.Time, bool)
}
