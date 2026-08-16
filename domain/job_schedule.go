package domain

import (
	"database/sql/driver"
	"time"
)

type ScheduleStatus uint8

const (
	ScheduleActive ScheduleStatus = iota
	SchedulePaused
	ScheduleFinished
)

var scheduleStatusNames = [...]string{
	"active",
	"paused",
	"finished",
}

// String returns the text representation of the ScheduleStatus.
func (s ScheduleStatus) String() string {
	return enumString(s, scheduleStatusNames[:], "unknown")
}

// IsValid reports whether the ScheduleStatus is defined.
func (s ScheduleStatus) IsValid() bool {
	return enumIsValid(s, scheduleStatusNames[:])
}

// MarshalJSON encodes the ScheduleStatus as its text representation.
func (s ScheduleStatus) MarshalJSON() ([]byte, error) {
	return enumMarshalJSON(s, scheduleStatusNames[:], "unknown")
}

// UnmarshalJSON decodes a ScheduleStatus from its text representation.
func (s *ScheduleStatus) UnmarshalJSON(data []byte) error {
	return enumUnmarshalJSON(s, scheduleStatusNames[:], data)
}

// Value returns the text representation of the ScheduleStatus for persistence.
func (s ScheduleStatus) Value() (driver.Value, error) {
	return enumValue(s, scheduleStatusNames[:], "unknown")
}

// Scan reads a ScheduleStatus from its persisted form.
func (s *ScheduleStatus) Scan(value any) error {
	return enumScan(s, scheduleStatusNames[:], value)
}

// ParseScheduleStatus converts a string into a ScheduleStatus.
func ParseScheduleStatus(s string) (ScheduleStatus, error) {
	return enumParse[ScheduleStatus](scheduleStatusNames[:], s)
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

var misfirePolicyNames = [...]string{
	"skip",
	"run_once",
	"catch_up",
}

// String returns the text representation of the MisfirePolicy.
func (m MisfirePolicy) String() string {
	return enumString(m, misfirePolicyNames[:], "unknown")
}

// IsValid reports whether the MisfirePolicy is defined.
func (m MisfirePolicy) IsValid() bool {
	return enumIsValid(m, misfirePolicyNames[:])
}

// MarshalJSON encodes the MisfirePolicy as its text representation.
func (m MisfirePolicy) MarshalJSON() ([]byte, error) {
	return enumMarshalJSON(m, misfirePolicyNames[:], "unknown")
}

// UnmarshalJSON decodes a MisfirePolicy from its text representation.
func (m *MisfirePolicy) UnmarshalJSON(data []byte) error {
	return enumUnmarshalJSON(m, misfirePolicyNames[:], data)
}

// Value returns the text representation of the MisfirePolicy for persistence.
func (m MisfirePolicy) Value() (driver.Value, error) {
	return enumValue(m, misfirePolicyNames[:], "unknown")
}

// Scan reads a MisfirePolicy from its persisted form.
func (m *MisfirePolicy) Scan(value any) error {
	return enumScan(m, misfirePolicyNames[:], value)
}

// ParseMisfirePolicy converts a string into a MisfirePolicy.
func ParseMisfirePolicy(s string) (MisfirePolicy, error) {
	return enumParse[MisfirePolicy](misfirePolicyNames[:], s)
}

const UnlimitedJobSchedules = 0

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
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

func NewJobSchedule(id, name string, payload []byte, rule ScheduleRule) JobSchedule {
	nowUtc := utcTimeNow()

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
		MaxJobs:       UnlimitedJobSchedules,
		Rule:          rule,
		CreatedAt:     nowUtc,
		UpdatedAt:     nowUtc,
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
	if s.Status == ScheduleFinished {
		return true
	}

	if s.MaxJobs > 0 && s.GeneratedJobs >= s.MaxJobs {
		return true
	}

	if isExpired(s.Rule.EndAt) {
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

//go:inline
func utcTimeNow() time.Time {
	return time.Now().UTC()
}

//go:inline
func isExpired(endAt *time.Time) bool {
	return endAt != nil && utcTimeNow().After(*endAt)
}
