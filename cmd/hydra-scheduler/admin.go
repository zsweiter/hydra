package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/zsweiter/hydra/bridge"
	"github.com/zsweiter/hydra/contract"
	"github.com/zsweiter/hydra/domain"
	"github.com/zsweiter/hydra/storage"
)

const adminUsage = `usage:
  hydra-scheduler                          run the scheduler
  hydra-scheduler version                  print the version
  hydra-scheduler schedules [--status s] [--limit n] [--offset n]
                                           list schedules; s is active, paused,
                                           finished, cancelled or archived (repeatable)
  hydra-scheduler schedule <subject> <recurrenceid>
  hydra-scheduler schedule <schedule-id>   show a schedule, then its archived
                                           revisions, newest first

Admin commands read the same HYDRA_STORE as the scheduler and
print one JSON object per line. They only read: schedules are
created, replaced, paused, resumed, cancelled and deleted by
publishing to a stream (recurrenceaction), so every change is
ordered and traced.`

// admin runs an inspection command against the store. It does
// not start the scheduler, so it can run beside a live one.
func admin(ctx context.Context, args []string, out io.Writer) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	store, err := openStore(cfg.store)
	if err != nil {
		return err
	}
	defer store.Close()
	enc := json.NewEncoder(out)

	switch args[0] {
	case "schedules":
		return adminSchedules(ctx, store, args[1:], enc)
	case "schedule":
		return adminSchedule(ctx, store, args[1:], enc)
	default:
		return fmt.Errorf("unknown command %q\n\n%s", args[0], adminUsage)
	}
}

type statusList []domain.ScheduleStatus

func (s *statusList) String() string { return fmt.Sprint(*s) }

func (s *statusList) Set(v string) error {
	for _, st := range []domain.ScheduleStatus{domain.ScheduleActive, domain.SchedulePaused, domain.ScheduleFinished, domain.ScheduleCancelled, domain.ScheduleArchived} {
		if st.String() == v {
			*s = append(*s, st)
			return nil
		}
	}
	return fmt.Errorf("unknown status %q", v)
}

func adminSchedules(ctx context.Context, store storage.ScheduleStore, args []string, enc *json.Encoder) error {
	fs := flag.NewFlagSet("schedules", flag.ContinueOnError)
	var statuses statusList
	fs.Var(&statuses, "status", "filter by status")
	limit := fs.Int("limit", 100, "page size")
	offset := fs.Int("offset", 0, "page offset")
	if err := fs.Parse(args); err != nil {
		return err
	}

	schedules, err := store.ListSchedules(ctx, storage.ScheduleQuery{
		Statuses: statuses,
		Limit:    *limit,
		Offset:   *offset,
	})
	if err != nil {
		return err
	}

	for _, s := range schedules {
		if err := enc.Encode(scheduleView(s)); err != nil {
			return err
		}
	}
	return nil
}

func scheduleView(s *domain.JobSchedule) map[string]any {
	rule := s.Rule.String()
	if s.Rule.Type == domain.ScheduleInterval {
		rule = contract.FormatDuration(time.Duration(s.Rule.IntervalNanos))
	}
	view := map[string]any{
		"id":          s.ID,
		"target":      s.Name,
		"status":      s.Status.String(),
		"revision":    bridge.Revision(s),
		"trace_id":    bridge.TraceID(s),
		"rule_type":   s.Rule.Type,
		"rule":        rule,
		"offset":      contract.FormatDuration(s.Rule.Offset),
		"occurrences": s.GeneratedJobs,
		"count":       s.MaxJobs,
		"next_run_at": s.NextRunAt,
		"last_run_at": s.LastRunAt,
		"version":     s.Version,
		"created_at":  s.CreatedAt,
		"updated_at":  s.UpdatedAt,
	}
	if prev := bridge.PreviousRevision(s); prev != "" {
		view["previous"] = prev
	}
	for k, v := range s.Metadata {
		if after, ok := strings.CutPrefix(k, "bridge.state_"); ok {
			view["state_"+after] = v
		}
		if after, ok := strings.CutPrefix(k, "bridge.superseded_"); ok {
			view["superseded_"+after] = v
		}
	}
	return view
}

// adminSchedule prints a schedule and walks back its archived
// revisions.
func adminSchedule(ctx context.Context, store storage.ScheduleStore, args []string, enc *json.Encoder) error {
	var id string
	switch len(args) {
	case 1:
		id = args[0]
	case 2:
		id = bridge.ScheduleKey(args[0], args[1])
	default:
		return errors.New(adminUsage)
	}

	for first := true; id != ""; first = false {
		s, err := store.LoadSchedule(ctx, id)
		if err != nil && !first {
			// Archived revisions older than HYDRA_HISTORY_TTL
			// are purged.
			return enc.Encode(map[string]any{"id": id, "purged": true})
		}
		if err != nil {
			return err
		}
		if err := enc.Encode(scheduleView(s)); err != nil {
			return err
		}
		id = bridge.PreviousRevision(s)
	}
	return nil
}
