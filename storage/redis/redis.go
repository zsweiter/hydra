// Package redis implements storage.Storage using Redis.
//
// Data structures:
//   - LIST hydra:q:{queue}:ready    — pending job IDs
//   - ZSET hydra:scheduled           — scheduled job IDs (score = runAt)
//   - HASH hydra:job:{id}           — job fields
//   - STRING hydra:lease:{id}       — worker ID (with TTL)
//   - HASH hydra:schedule:{id}      — schedule fields
//   - SET  hydra:schedules:active   — active schedule IDs
//   - LIST hydra:dlq                — dead letter entries
package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/zsweiter/hydra/domain"
	"github.com/zsweiter/hydra/storage"
)

const prefix = "hydra:"

func jobKey(id string) string      { return prefix + "job:" + id }
func readyKey(q string) string     { return prefix + "q:" + q + ":ready" }
func scheduleKey(id string) string { return prefix + "schedule:" + id }

var scheduledSet = prefix + "scheduled"
var activeSchedulesSet = prefix + "schedules:active"

type Options struct {
	Addr     string
	Password string
	Username string
	DB       int
}

// Storage implements storage.Storage backed by Redis.
type Storage struct {
	rdb redis.UniversalClient
}

// Open creates a new Redis-backed storage.
func Use(rdb redis.UniversalClient) *Storage {
	return &Storage{rdb: rdb}
}

// Create new client based in redis conf
func New(opt *Options) *Storage {
	return Use(redis.NewClient(&redis.Options{
		Addr:     opt.Addr,
		DB:       opt.DB,
		Username: opt.Username,
		Password: opt.Password,
	}))
}

// ──────────────────────────────────────────────
//  Job operations
// ──────────────────────────────────────────────

func (s *Storage) SaveJob(ctx context.Context, job *domain.Job) error {
	pipe := s.rdb.Pipeline()

	pipe.HSet(ctx, jobKey(job.ID), map[string]any{
		"id":              job.ID,
		"name":            job.Name,
		"queue":           job.Queue.String(),
		"data":            job.Data,
		"metadata":        marshalMap(job.Metadata),
		"run_at":          job.RunAt.UnixMilli(),
		"priority":        job.Priority,
		"status":          job.Status.String(),
		"attempts":        job.Attempts,
		"max_attempts":    job.MaxAttempts,
		"backoff_type":    job.Backoff.Type.String(),
		"backoff_base_ms": job.Backoff.BaseMs,
		"backoff_max_ms":  job.Backoff.MaxMs,
		"schedule_id":     nilStr(job.ScheduleID),
		"idempotency_key": job.IdempotencyKey,
		"created_at":      job.CreatedAt.UnixMilli(),
		"updated_at":      job.UpdatedAt.UnixMilli(),
	})

	if job.Status == domain.StatusScheduled {
		pipe.ZAdd(ctx, scheduledSet, redis.Z{Score: float64(job.RunAt.UnixMilli()), Member: job.ID})
	} else {
		pipe.LPush(ctx, readyKey(job.Queue.String()), job.ID)
	}

	_, err := pipe.Exec(ctx)
	return err
}

func (s *Storage) RemoveJob(ctx context.Context, jobID string) error {
	jobKey := jobKey(jobID)
	status, err := s.rdb.HGet(ctx, jobKey, "status").Result()
	if err == redis.Nil {
		return fmt.Errorf("%w: %s", storage.ErrNotFound, jobID)
	}
	if err != nil {
		return err
	}

	if status == "processing" || status == "completed" {
		return fmt.Errorf("%w: job is processing or completed: %s", storage.ErrConflict, jobID)
	}

	pipe := s.rdb.Pipeline()
	pipe.Del(ctx, jobKey)
	pipe.LRem(ctx, readyKeyWildcard(jobID), 0, jobID)
	pipe.ZRem(ctx, scheduledSet, jobID)

	_, err = pipe.Exec(ctx)
	return err
}

// readyKeyWildcard attempts removal from all ready queues.
// In practice the job knows its queue, but RemoveJob doesn't receive it.
func readyKeyWildcard(_ string) string { return prefix + "q:*:ready" }

func (s *Storage) ClaimJobs(ctx context.Context, queue domain.QueueName, workerID string, n int, ttl time.Duration) ([]storage.LeasedJob, error) {
	// Lua atomically pop N job IDs from ready list and set leases
	const script = `
		local ids = redis.call('LRANGE', KEYS[1], 0, ARGV[1] - 1)
		if #ids == 0 then return {} end
		redis.call('LTRIM', KEYS[1], ARGV[1], -1)
		local now = tonumber(ARGV[2])
		local expires = now + tonumber(ARGV[3])
		for _, id in ipairs(ids) do
			redis.call('HSET', 'hydra:lease:' .. id, 'worker', ARGV[4], 'expires_at', expires)
			redis.call('EXPIRE', 'hydra:lease:' .. id, ARGV[5])
			redis.call('HSET', 'hydra:job:' .. id, 'status', 'processing', 'updated_at', now)
		end
		return ids
	`
	now := time.Now().UTC().UnixMilli()
	expiresMs := int(ttl.Milliseconds())
	expireSec := int(math.Ceil(ttl.Seconds()))

	ids, err := s.rdb.Eval(ctx, script, []string{readyKey(queue.String())}, n, now, expiresMs, workerID, expireSec).Result()
	if err != nil {
		return nil, err
	}

	idSlice, ok := ids.([]any)
	if !ok || len(idSlice) == 0 {
		return nil, nil
	}

	result := make([]storage.LeasedJob, len(idSlice))
	for i, idIf := range idSlice {
		id, _ := idIf.(string)
		job, err := s.loadJob(ctx, id)
		if err != nil {
			return nil, err
		}

		job.Status = domain.StatusProcessing
		job.UpdatedAt = time.UnixMilli(now).UTC()
		result[i] = storage.LeasedJob{Job: job}
	}

	return result, nil
}

func (s *Storage) loadJob(ctx context.Context, id string) (*domain.Job, error) {
	data, err := s.rdb.HGetAll(ctx, jobKey(id)).Result()
	if err != nil {
		return nil, err
	}

	if len(data) == 0 {
		return nil, storage.ErrNotFound
	}

	job := &domain.Job{
		ID:             data["id"],
		Name:           data["name"],
		Queue:          domain.QueueName(data["queue"]),
		Data:           []byte(data["data"]),
		Metadata:       unmarshalMap(data["metadata"]),
		Priority:       int(parseInt(data["priority"])),
		Status:         domain.StatusFrom(data["status"]),
		Attempts:       int(parseInt(data["attempts"])),
		MaxAttempts:    int(parseInt(data["max_attempts"])),
		IdempotencyKey: data["idempotency_key"],
	}

	backoffType, err := domain.ParseBackoffType(data["backoff_type"])
	if err != nil {
		return nil, fmt.Errorf("load job: %w", err)
	}
	job.Backoff.Type = backoffType
	job.Backoff.BaseMs = parseInt(data["backoff_base_ms"])
	job.Backoff.MaxMs = parseInt(data["backoff_max_ms"])
	job.RunAt = time.UnixMilli(parseInt(data["run_at"])).UTC()
	job.CreatedAt = time.UnixMilli(parseInt(data["created_at"])).UTC()
	job.UpdatedAt = time.UnixMilli(parseInt(data["updated_at"])).UTC()

	if sid := data["schedule_id"]; sid != "" {
		job.ScheduleID = &sid
	}

	return job, nil
}

func (s *Storage) AckJob(ctx context.Context, jobID string) error {
	script := `
		local job_id = KEYS[1]
		local id_only = string.match(job_id, 'hydra:job:(.+)$')
		redis.call('DEL', 'hydra:lease:' .. id_only)
		redis.call('HSET', job_id, 'status', 'completed', 'updated_at', ARGV[1])
		return 1
	`
	_, err := s.rdb.Eval(ctx, script, []string{jobKey(jobID)}, time.Now().UTC().UnixMilli()).Result()
	return err
}

func (s *Storage) FailJob(ctx context.Context, job *domain.Job, failErr error, decision domain.FailDecision) (*storage.FailResult, error) {
	now := time.Now().UTC()
	failedAtStr := now.Format(time.RFC3339)

	script := `
		local retryable = tonumber(ARGV[1])
		local job_id = KEYS[1]
		local id_only = string.match(job_id, 'hydra:job:(.+)$')
		redis.call('DEL', 'hydra:lease:' .. id_only)
		if retryable == 1 then
			local next_run = tonumber(ARGV[2])
			redis.call('HSET', 'hydra:job:' .. id_only, 'status', 'pending', 'attempts', tonumber(ARGV[4]), 'run_at', next_run, 'updated_at', tonumber(ARGV[3]))
			redis.call('ZADD', 'hydra:scheduled', next_run, id_only)
			return {'retry', next_run}
		else
			redis.call('HSET', 'hydra:job:' .. id_only, 'status', 'dead_letter', 'attempts', tonumber(ARGV[4]), 'updated_at', tonumber(ARGV[3]))
			redis.call('HSET', 'hydra:dlq:' .. id_only, 'id', id_only, 'queue', ARGV[5], 'error', ARGV[6], 'failed_at', ARGV[7], 'attempts', tonumber(ARGV[4]))
			redis.call('SADD', 'hydra:dlq:index', id_only)
			return {'dead_letter', 0}
		end
	`

	var retryable int
	var nextRun int64
	if decision.Retry {
		retryable = 1
		nextRun = decision.NextRunAt.UnixMilli()
	}

	nowMs := now.UnixMilli()

	result, err := s.rdb.Eval(ctx, script, []string{jobKey(job.ID)},
		retryable, nextRun, nowMs, job.Attempts, job.Queue.String(), failErr.Error(), failedAtStr).Result()
	if err != nil {
		return nil, err
	}

	resSlice, ok := result.([]any)
	if !ok || len(resSlice) < 2 {
		return &storage.FailResult{Action: storage.FailActionDeadLetter}, nil
	}

	action, _ := resSlice[0].(string)
	if action == "retry" {
		job.RunAt = decision.NextRunAt
		return &storage.FailResult{Action: storage.FailActionRetry, NextRunAt: decision.NextRunAt}, nil
	}

	return &storage.FailResult{Action: storage.FailActionDeadLetter}, nil
}

// ──────────────────────────────────────────────
//  Queue operations
// ──────────────────────────────────────────────

func (s *Storage) ReadyQueues(ctx context.Context) ([]domain.QueueName, error) {
	// Scan keys matching hydra:q:*:ready and check which have items
	iter := s.rdb.Scan(ctx, 0, prefix+"q:*:ready", 100).Iterator()
	var queues []domain.QueueName
	for iter.Next(ctx) {
		key := iter.Val()
		len, err := s.rdb.LLen(ctx, key).Result()
		if err != nil {
			continue
		}
		if len > 0 {
			// Extract queue name from key: hydra:q:{name}:ready
			q := extractQueueName(key)
			queues = append(queues, domain.QueueName(q))
		}
	}

	return queues, iter.Err()
}

func extractQueueName(key string) string {
	// key = "hydra:q:{name}:ready"
	start := len(prefix) + 2 // "hydra:q:"
	end := len(key) - len(":ready")
	if start < 0 || end > len(key) || start >= end {
		return ""
	}

	return key[start:end]
}

func (s *Storage) PromoteScheduled(ctx context.Context, now time.Time, limit int) (int, error) {
	nowMs := now.UnixMilli()
	script := `
		local ids = redis.call('ZRANGEBYSCORE', KEYS[1], 0, ARGV[1], 'LIMIT', 0, ARGV[2])
		if #ids == 0 then return 0 end
		for _, id in ipairs(ids) do
			local queue = redis.call('HGET', 'hydra:job:' .. id, 'queue')
			redis.call('LPUSH', 'hydra:q:' .. queue .. ':ready', id)
			redis.call('HSET', 'hydra:job:' .. id, 'status', 'pending', 'updated_at', ARGV[3])
		end
		redis.call('ZREM', KEYS[1], unpack(ids))
		return #ids
	`
	n, err := s.rdb.Eval(ctx, script, []string{scheduledSet}, nowMs, limit, now.UnixMilli()).Int()
	if err != nil {
		return 0, err
	}

	return n, nil
}

func (s *Storage) RecoverOrphaned(ctx context.Context, before time.Time) (int, error) {
	beforeMs := before.UnixMilli()
	script := `
		local cursor = '0'
		local recovered = 0
		repeat
			local result = redis.call('SCAN', cursor, 'MATCH', 'hydra:lease:*', 'COUNT', 100)
			cursor = result[1]
			local keys = result[2]
			for _, key in ipairs(keys) do
				local expires = redis.call('HGET', key, 'expires_at')
				if expires and tonumber(expires) <= tonumber(ARGV[1]) then
					local job_id = key:match('hydra:lease:(.+)$')
					if job_id then
						local queue = redis.call('HGET', 'hydra:job:' .. job_id, 'queue')
						redis.call('LPUSH', 'hydra:q:' .. queue .. ':ready', job_id)
						redis.call('DEL', key)
						redis.call('HSET', 'hydra:job:' .. job_id, 'status', 'pending', 'updated_at', ARGV[1])
						recovered = recovered + 1
					end
				end
			end
		until cursor == '0'
		return recovered
	`
	n, err := s.rdb.Eval(ctx, script, []string{}, beforeMs).Int()
	if err != nil {
		return 0, err
	}

	return n, nil
}

// ──────────────────────────────────────────────
//  Schedule operations
// ──────────────────────────────────────────────

func (s *Storage) SaveSchedule(ctx context.Context, schedule *domain.JobSchedule) error {
	now := time.Now().UTC().UnixMilli()
	if schedule.CreatedAt.IsZero() {
		schedule.CreatedAt = time.UnixMilli(now).UTC()
	}
	schedule.UpdatedAt = time.UnixMilli(now).UTC()

	pipe := s.rdb.Pipeline()
	pipe.HSet(ctx, scheduleKey(schedule.ID), map[string]any{
		"id":                  schedule.ID,
		"name":                schedule.Name,
		"queue":               schedule.Queue.String(),
		"data":                schedule.Data,
		"metadata":            marshalMap(schedule.Metadata),
		"rule_type":           string(schedule.Rule.Type),
		"rule_expression":     schedule.Rule.Expression,
		"rule_interval_ms":    schedule.Rule.IntervalMs,
		"rule_interval_nanos": schedule.Rule.IntervalNanos,
		"start_at":            optMillis(schedule.Rule.StartAt),
		"end_at":              optMillis(schedule.Rule.EndAt),
		"timezone":            schedule.Rule.Timezone,
		"status":              schedule.Status.String(),
		"misfire_policy":      schedule.MisfirePolicy.String(),
		"last_run_at":         optMillis(schedule.LastRunAt),
		"next_run_at":         optMillis(schedule.NextRunAt),
		"priority":            schedule.Priority,
		"max_jobs":            schedule.MaxJobs,
		"generated_jobs":      schedule.GeneratedJobs,
		"created_at":          schedule.CreatedAt.UnixMilli(),
		"updated_at":          schedule.UpdatedAt.UnixMilli(),
	})
	pipe.SAdd(ctx, activeSchedulesSet, schedule.ID)

	_, err := pipe.Exec(ctx)
	return err
}

func (s *Storage) DeleteSchedule(ctx context.Context, scheduleID string) error {
	pipe := s.rdb.Pipeline()
	pipe.Del(ctx, scheduleKey(scheduleID))
	pipe.SRem(ctx, activeSchedulesSet, scheduleID)
	_, err := pipe.Exec(ctx)
	return err
}

func (s *Storage) LoadActiveSchedules(ctx context.Context) ([]*domain.JobSchedule, error) {
	ids, err := s.rdb.SMembers(ctx, activeSchedulesSet).Result()
	if err != nil {
		return nil, err
	}

	var result []*domain.JobSchedule
	for _, id := range ids {
		sch, err := s.LoadSchedule(ctx, id)
		if err != nil {
			continue
		}
		result = append(result, sch)
	}

	return result, nil
}

func (s *Storage) LoadSchedule(ctx context.Context, scheduleID string) (*domain.JobSchedule, error) {
	data, err := s.rdb.HGetAll(ctx, scheduleKey(scheduleID)).Result()
	if err != nil {
		return nil, err
	}

	if len(data) == 0 {
		return nil, fmt.Errorf("%w: %s", storage.ErrNotFound, scheduleID)
	}

	sch := &domain.JobSchedule{
		ID:            data["id"],
		Name:          data["name"],
		Queue:         domain.QueueName(data["queue"]),
		Data:          []byte(data["data"]),
		Metadata:      unmarshalMap(data["metadata"]),
		Priority:      int(parseInt(data["priority"])),
		MaxJobs:       int(parseInt(data["max_jobs"])),
		GeneratedJobs: int(parseInt(data["generated_jobs"])),
	}
	sch.Rule = domain.ScheduleRule{
		Type:          domain.ScheduleType(data["rule_type"]),
		Expression:    data["rule_expression"],
		IntervalMs:    parseInt(data["rule_interval_ms"]),
		IntervalNanos: parseInt(data["rule_interval_nanos"]),
		Timezone:      data["timezone"],
	}
	status, err := domain.ParseScheduleStatus(data["status"])
	if err != nil {
		return nil, fmt.Errorf("load schedule: %w", err)
	}
	sch.Status = status

	misfirePolicy, err := domain.ParseMisfirePolicy(data["misfire_policy"])
	if err != nil {
		return nil, fmt.Errorf("load schedule: %w", err)
	}
	sch.MisfirePolicy = misfirePolicy
	sch.CreatedAt = time.UnixMilli(parseInt(data["created_at"])).UTC()
	sch.UpdatedAt = time.UnixMilli(parseInt(data["updated_at"])).UTC()

	if t := parseInt(data["start_at"]); t > 0 {
		v := time.UnixMilli(t).UTC()
		sch.Rule.StartAt = &v
	}

	if t := parseInt(data["end_at"]); t > 0 {
		v := time.UnixMilli(t).UTC()
		sch.Rule.EndAt = &v
	}

	if t := parseInt(data["last_run_at"]); t > 0 {
		v := time.UnixMilli(t).UTC()
		sch.LastRunAt = &v
	}

	if t := parseInt(data["next_run_at"]); t > 0 {
		v := time.UnixMilli(t).UTC()
		sch.NextRunAt = &v
	}

	return sch, nil
}

func (s *Storage) UpdateScheduleStatus(ctx context.Context, scheduleID string, status domain.ScheduleStatus) error {
	_, err := s.rdb.HSet(ctx, scheduleKey(scheduleID), "status", status.String(), "updated_at", time.Now().UTC().UnixMilli()).Result()
	return err
}

func (s *Storage) IncrementGeneratedJobs(ctx context.Context, scheduleID string) (int, error) {
	n, err := s.rdb.HIncrBy(ctx, scheduleKey(scheduleID), "generated_jobs", 1).Result()
	if err != nil {
		return 0, err
	}
	_, err = s.rdb.HSet(ctx, scheduleKey(scheduleID), "updated_at", time.Now().UTC().UnixMilli()).Result()
	return int(n), err
}

func (s *Storage) UpdateNextRunAt(ctx context.Context, scheduleID string, nextRun time.Time, lastRun time.Time) error {
	_, err := s.rdb.HSet(ctx, scheduleKey(scheduleID), map[string]any{
		"last_run_at": lastRun.UnixMilli(),
		"next_run_at": nextRun.UnixMilli(),
		"updated_at":  time.Now().UTC().UnixMilli(),
	}).Result()

	return err
}

// ──────────────────────────────────────────────
//  DLQ operations
// ──────────────────────────────────────────────

func (s *Storage) ListDLQ(ctx context.Context, queue domain.QueueName) ([]storage.DeadLetter, error) {
	ids, err := s.rdb.SMembers(ctx, prefix+"dlq:index").Result()
	if err != nil {
		return nil, err
	}

	var result []storage.DeadLetter
	for _, id := range ids {
		data, err := s.rdb.HGetAll(ctx, prefix+"dlq:"+id).Result()
		if err != nil || len(data) == 0 {
			continue
		}
		dl := storage.DeadLetter{
			ID:       data["id"],
			Queue:    domain.QueueName(data["queue"]),
			Payload:  nil,
			Error:    data["error"],
			Attempts: int(parseInt(data["attempts"])),
		}
		if ft := data["failed_at"]; ft != "" {
			dl.FailedAt, _ = time.Parse(time.RFC3339, ft)
		}
		if queue == "" || dl.Queue == queue {
			result = append(result, dl)
		}
	}

	return result, nil
}

func (s *Storage) RequeueDLQ(ctx context.Context, jobID string) error {
	exists, err := s.rdb.Exists(ctx, prefix+"dlq:"+jobID).Result()
	if err != nil || exists == 0 {
		return fmt.Errorf("%w: %s", storage.ErrNotFound, jobID)
	}

	data, err := s.rdb.HGetAll(ctx, prefix+"dlq:"+jobID).Result()
	if err != nil {
		return err
	}

	queue := data["queue"]

	pipe := s.rdb.Pipeline()
	pipe.Del(ctx, prefix+"dlq:"+jobID)
	pipe.SRem(ctx, prefix+"dlq:index", jobID)
	now := time.Now().UTC().UnixMilli()
	pipe.HSet(ctx, jobKey(jobID), "status", "pending", "attempts", 0,
		"run_at", now, "updated_at", now)
	pipe.LPush(ctx, readyKey(queue), jobID)

	_, err = pipe.Exec(ctx)

	return err
}

func (s *Storage) DrainQueue(ctx context.Context, queue domain.QueueName) (int, error) {
	script := `
		local drained = 0

		local ready = redis.call('LRANGE', KEYS[1], 0, -1)
		if #ready > 0 then
			for _, id in ipairs(ready) do
				redis.call('DEL', 'hydra:job:' .. id)
			end
			redis.call('DEL', KEYS[1])
			drained = drained + #ready
		end

		local scheduled = redis.call('ZRANGE', KEYS[2], 0, -1)
		for _, id in ipairs(scheduled) do
			local queue = redis.call('HGET', 'hydra:job:' .. id, 'queue')
			if queue == ARGV[1] then
				redis.call('ZREM', KEYS[2], id)
				redis.call('DEL', 'hydra:job:' .. id)
				drained = drained + 1
			end
		end

		return drained
	`

	n, err := s.rdb.Eval(ctx, script, []string{readyKey(queue.String()), scheduledSet}, queue.String()).Int()
	if err != nil {
		return 0, err
	}

	return n, nil
}

// ──────────────────────────────────────────────
//  Stats & Close
// ──────────────────────────────────────────────

func (s *Storage) NextScheduledAt(ctx context.Context) (time.Time, error) {
	entries, err := s.rdb.ZRangeWithScores(ctx, scheduledSet, 0, 0).Result()
	if err != nil || len(entries) == 0 {
		return time.Time{}, err
	}

	return time.UnixMilli(int64(entries[0].Score)).UTC(), nil
}

func (s *Storage) Stats(ctx context.Context) (storage.Stats, error) {
	var st storage.Stats
	st.ReadyCount = make(map[domain.QueueName]int64)

	iter := s.rdb.Scan(ctx, 0, prefix+"q:*:ready", 100).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()
		len, err := s.rdb.LLen(ctx, key).Result()
		if err != nil {
			continue
		}
		if len > 0 {
			q := extractQueueName(key)
			st.ReadyCount[domain.QueueName(q)] = len
		}
	}

	schedCount, _ := s.rdb.ZCard(ctx, scheduledSet).Result()
	st.ScheduledCount = schedCount
	dlqCount, _ := s.rdb.SCard(ctx, prefix+"dlq:index").Result()
	st.DLQCount = dlqCount
	leaseCount, _ := s.rdb.Keys(ctx, prefix+"lease:*").Result()
	st.LeaseCount = int64(len(leaseCount))

	return st, nil
}

func (s *Storage) Close() error {
	return s.rdb.Close()
}

// ──────────────────────────────────────────────
//  Helpers
// ──────────────────────────────────────────────

func marshalMap(m map[string]string) string {
	b, _ := json.Marshal(m)
	return string(b)
}

func unmarshalMap(s string) map[string]string {
	m := make(map[string]string)
	json.Unmarshal([]byte(s), &m)
	return m
}

func nilStr(s *string) any {
	if s == nil {
		return ""
	}
	return *s
}

func optMillis(t *time.Time) any {
	if t == nil {
		return 0
	}
	return t.UnixMilli()
}

func parseInt(s string) int64 {
	if s == "" {
		return 0
	}
	var v int64
	// ignore error — defaults to 0
	fmt.Sscanf(s, "%d", &v)
	return v
}
