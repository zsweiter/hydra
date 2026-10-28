package scheduler

import (
	"fmt"
	"time"

	"github.com/zsweiter/hydra/domain"
)

// Prepare validates the rule of schedule and sets its progress
// before it is saved. prev is the schedule stored under the
// same ID, or nil.
//
// Saving an unchanged rule again (e.g. a broker redelivering a
// definition) keeps the stored progress instead of restarting
// the schedule, unless schedule sets where it continues
// (NextRunAt). Otherwise the first occurrence is computed and
// persisted, so every instance sharing the storage agrees on
// it. Cancelled and archived schedules are history and are left
// as they are.
func Prepare(schedule, prev *domain.JobSchedule, now time.Time) error {
	if schedule.Status == domain.ScheduleCancelled || schedule.Status == domain.ScheduleArchived {
		return nil
	}

	compiled, err := Compile(schedule.Rule)
	if err != nil {
		return fmt.Errorf("validate rule: %w", err)
	}

	if prev != nil && schedule.NextRunAt == nil && prev.Status != domain.ScheduleCancelled &&
		prev.Status != domain.ScheduleArchived && prev.Rule.Equal(schedule.Rule) {
		schedule.GeneratedJobs = prev.GeneratedJobs
		schedule.LastRunAt = prev.LastRunAt
		schedule.NextRunAt = prev.NextRunAt
		schedule.CreatedAt = prev.CreatedAt
		if prev.Status == domain.ScheduleFinished {
			schedule.Status = domain.ScheduleFinished
		}
	}

	if schedule.NextRunAt == nil && schedule.Status != domain.ScheduleFinished {
		next, ok := FirstRun(schedule.Rule, compiled, now.UTC())
		if ok {
			schedule.NextRunAt = &next
		} else {
			schedule.Status = domain.ScheduleFinished
		}
	}

	return nil
}
