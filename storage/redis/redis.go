// Package redis implements storage.Storage using Redis, and
// the servers that speak its protocol: Valkey and Dragonfly.
//
// Every key starts with the prefix in braces, "{hydra}:" by
// default. The braces are a Redis Cluster hash tag: all keys of
// one prefix live in the same slot, so the multi-key scripts
// and transactions work on a cluster too. A deployment then
// lives on one shard of the cluster; separate deployments
// (WithPrefix) spread over the others.
//
// Data structures (P is the prefix):
//   - LIST P:q:{queue}:ready         — pending job IDs
//   - ZSET P:scheduled               — scheduled job IDs (score = runAt)
//   - HASH P:job:{id}                — job fields
//   - ZSET P:leases                  — leased job IDs (score = lease expiry)
//   - ZSET P:jobs:completed          — completed job IDs (score = updatedAt), for purging
//   - SET  P:queues                  — every queue name
//   - SET  P:queues:paused           — paused queue names
//   - HASH P:schedule:{id}           — schedule fields
//   - SET  P:schedules:active        — active and paused schedule IDs
//   - ZSET P:schedules:due:{n}       — active schedules of shard n (score = nextRunAt)
//   - ZSET P:schedules:changed       — active schedules (score = updatedAt), for syncs
//   - ZSET P:schedules:status:{s}    — finished/cancelled/archived history (score = updatedAt)
//   - ZSET P:schedule:{id}:jobs      — jobs of a schedule (score = createdAt)
//   - ZSET P:instances               — live instances (score = heartbeat expiry)
//   - HASH P:dlq:{id}                — dead letter entry
//   - SET  P:dlq:index               — dead letter IDs
//
// Lua scripts only touch the keys they declare, as Dragonfly
// requires.
package redis

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/zsweiter/hydra/domain"
	"github.com/zsweiter/hydra/storage"
)

// DefaultPrefix is the key prefix used unless WithPrefix sets
// another.
const DefaultPrefix = "hydra"

// keys builds the key names of one prefix.
type keys struct{ p string }

func newKeys(prefix string) keys { return keys{p: "{" + prefix + "}:"} }

func (k keys) job(id string) string          { return k.p + "job:" + id }
func (k keys) ready(q string) string         { return k.p + "q:" + q + ":ready" }
func (k keys) schedule(id string) string     { return k.p + "schedule:" + id }
func (k keys) dlq(id string) string          { return k.p + "dlq:" + id }
func (k keys) scheduleJobs(id string) string { return k.p + "schedule:" + id + ":jobs" }
func (k keys) due(shard int) string          { return k.p + "schedules:due:" + strconv.Itoa(shard) }
func (k keys) status(s domain.ScheduleStatus) string {
	return k.p + "schedules:status:" + strconv.Itoa(int(s))
}

func (k keys) dlqIndex() string        { return k.p + "dlq:index" }
func (k keys) scheduled() string       { return k.p + "scheduled" }
func (k keys) activeSchedules() string { return k.p + "schedules:active" }
func (k keys) changed() string         { return k.p + "schedules:changed" }
func (k keys) completed() string       { return k.p + "jobs:completed" }
func (k keys) instances() string       { return k.p + "instances" }
func (k keys) pausedQueues() string    { return k.p + "queues:paused" }

// leases holds leased job IDs scored by lease expiry, so
// expired leases are found with a range query instead of a
// keyspace scan, and can be extended in place.
func (k keys) leases() string { return k.p + "leases" }

// queues holds every queue name that received a job, so
// listing queues does not scan the keyspace.
func (k keys) queues() string { return k.p + "queues" }

var historyStatuses = []domain.ScheduleStatus{domain.ScheduleFinished, domain.ScheduleCancelled, domain.ScheduleArchived}

type Options struct {
	Addr     string
	Password string
	Username string
	DB       int
	Prefix   string
}

// Option configures a Storage.
type Option func(*Storage)

// WithPrefix sets the key prefix (DefaultPrefix otherwise).
// Deployments sharing a server or cluster use distinct ones.
func WithPrefix(prefix string) Option {
	return func(s *Storage) {
		if prefix != "" {
			s.k = newKeys(prefix)
		}
	}
}

// Storage implements storage.Storage backed by Redis.
type Storage struct {
	rdb redis.UniversalClient
	k   keys
}

var _ storage.Storage = (*Storage)(nil)

// Use creates a storage on a client: a single server, a
// sentinel group or a cluster (redis.NewUniversalClient).
func Use(rdb redis.UniversalClient, opts ...Option) *Storage {
	s := &Storage{rdb: rdb, k: newKeys(DefaultPrefix)}
	for _, opt := range opts {
		opt(s)
	}

	return s
}

// Create new client based in redis conf
func New(opt *Options) *Storage {
	return Use(redis.NewClient(&redis.Options{
		Addr:     opt.Addr,
		DB:       opt.DB,
		Username: opt.Username,
		Password: opt.Password,
	}), WithPrefix(opt.Prefix))
}

// ──────────────────────────────────────────────
//  Job operations
// ──────────────────────────────────────────────

func (s *Storage) SaveJob(ctx context.Context, job *domain.Job) error {
	pipe := s.rdb.Pipeline()
	s.queueSaveJob(ctx, pipe, job)
	_, err := pipe.Exec(ctx)
	return err
}

// jobFields returns the hash fields persisting job.
func jobFields(job *domain.Job) []any {
	return []any{
		"id", job.ID,
		"name", job.Name,
		"queue", job.Queue,
		"data", job.Data,
		"metadata", marshalMap(job.Metadata),
		"run_at", job.RunAt.UnixMilli(),
		"priority", job.Priority,
		"status", "pending",
		"attempts", job.Attempts,
		"max_attempts", job.MaxAttempts,
		"backoff_type", int(job.Backoff.Type),
		"backoff_base_ms", job.Backoff.BaseMs,
		"backoff_max_ms", job.Backoff.MaxMs,
		"schedule_id", nilStr(job.ScheduleID),
		"idempotency_key", job.IdempotencyKey,
		"created_at", job.CreatedAt.UnixMilli(),
		"updated_at", job.UpdatedAt.UnixMilli(),
	}
}

func stampJob(job *domain.Job, now time.Time) {
	if job.CreatedAt.IsZero() {
		job.CreatedAt = now
	}
	job.UpdatedAt = now
	job.Status = domain.StatusPending
}

// queueSaveJob queues the commands persisting job on pipe, so
// they can run in a plain pipeline or a MULTI transaction.
func (s *Storage) queueSaveJob(ctx context.Context, pipe redis.Pipeliner, job *domain.Job) {
	now := time.Now()
	stampJob(job, now)

	pipe.HSet(ctx, s.k.job(job.ID), jobFields(job)...)

	if job.RunAt.After(now) {
		pipe.ZAdd(ctx, s.k.scheduled(), redis.Z{Score: float64(job.RunAt.UnixMilli()), Member: job.ID})
	} else {
		pipe.LPush(ctx, s.k.ready(job.Queue), job.ID)
	}

	pipe.SAdd(ctx, s.k.queues(), job.Queue)
	if job.ScheduleID != nil {
		pipe.ZAdd(ctx, s.k.scheduleJobs(*job.ScheduleID), redis.Z{Score: float64(job.CreatedAt.UnixMilli()), Member: job.ID})
	}
}

func (s *Storage) RemoveJob(ctx context.Context, jobID string) error {
	fields, err := s.rdb.HMGet(ctx, s.k.job(jobID), "status", "queue", "schedule_id").Result()
	if err != nil {
		return err
	}
	if fields[0] == nil {
		return fmt.Errorf("job not found: %s", jobID)
	}

	status := fmt.Sprint(fields[0])
	if status == "processing" || status == "completed" {
		return fmt.Errorf("cannot remove processing/completed job: %s", jobID)
	}

	pipe := s.rdb.Pipeline()
	pipe.Del(ctx, s.k.job(jobID))
	pipe.LRem(ctx, s.k.ready(fmt.Sprint(fields[1])), 0, jobID)
	pipe.ZRem(ctx, s.k.scheduled(), jobID)
	if sid, ok := fields[2].(string); ok && sid != "" {
		pipe.ZRem(ctx, s.k.scheduleJobs(sid), jobID)
	}

	_, err = pipe.Exec(ctx)
	return err
}

// claimJobsScript pops up to ARGV[1] IDs and leases them until
// ARGV[2] in one atomic step. The job hashes are updated
// afterwards: a crash in between leaves a lease that expires,
// and the job is recovered.
var claimJobsScript = redis.NewScript(`
	local ids = redis.call('LRANGE', KEYS[1], 0, ARGV[1] - 1)
	if #ids == 0 then return {} end
	redis.call('LTRIM', KEYS[1], ARGV[1], -1)
	for _, id in ipairs(ids) do
		redis.call('ZADD', KEYS[2], ARGV[2], id)
	end
	return ids
`)

func (s *Storage) ClaimJobs(ctx context.Context, queue domain.QueueName, workerID string, n int, ttl time.Duration) ([]storage.LeasedJob, error) {
	now := time.Now().UnixMilli()
	ids, err := claimJobsScript.Run(ctx, s.rdb, []string{s.k.ready(queue), s.k.leases()}, n, now+ttl.Milliseconds()).StringSlice()
	if err != nil || len(ids) == 0 {
		return nil, err
	}

	pipe := s.rdb.TxPipeline()
	hashes := make([]*redis.MapStringStringCmd, len(ids))
	for i, id := range ids {
		pipe.HSet(ctx, s.k.job(id), "status", "processing", "updated_at", now)
		hashes[i] = pipe.HGetAll(ctx, s.k.job(id))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}

	result := make([]storage.LeasedJob, 0, len(ids))
	for _, h := range hashes {
		result = append(result, storage.LeasedJob{Job: parseJob(h.Val())})
	}

	return result, nil
}

func (s *Storage) loadJob(ctx context.Context, id string) (*domain.Job, error) {
	data, err := s.rdb.HGetAll(ctx, s.k.job(id)).Result()
	if err != nil {
		return nil, err
	}

	if len(data) == 0 {
		return nil, fmt.Errorf("job not found: %s", id)
	}

	return parseJob(data), nil
}

func parseJob(data map[string]string) *domain.Job {
	job := &domain.Job{
		ID:             data["id"],
		Name:           data["name"],
		Queue:          data["queue"],
		Data:           []byte(data["data"]),
		Metadata:       unmarshalMap(data["metadata"]),
		Priority:       int(parseInt(data["priority"])),
		Status:         parseStatus(data["status"]),
		Attempts:       int(parseInt(data["attempts"])),
		MaxAttempts:    int(parseInt(data["max_attempts"])),
		IdempotencyKey: data["idempotency_key"],
	}

	job.Backoff.Type = domain.BackoffType(parseInt(data["backoff_type"]))
	job.Backoff.BaseMs = parseInt(data["backoff_base_ms"])
	job.Backoff.MaxMs = parseInt(data["backoff_max_ms"])
	job.RunAt = time.UnixMilli(parseInt(data["run_at"]))
	job.CreatedAt = time.UnixMilli(parseInt(data["created_at"]))
	job.UpdatedAt = time.UnixMilli(parseInt(data["updated_at"]))

	if sid := data["schedule_id"]; sid != "" {
		job.ScheduleID = &sid
	}

	return job
}

func (s *Storage) AckJobs(ctx context.Context, jobIDs []string) error {
	if len(jobIDs) == 0 {
		return nil
	}

	now := time.Now().UnixMilli()
	members := make([]any, len(jobIDs))
	done := make([]redis.Z, len(jobIDs))
	for i, id := range jobIDs {
		members[i] = id
		done[i] = redis.Z{Score: float64(now), Member: id}
	}

	pipe := s.rdb.TxPipeline()
	pipe.ZRem(ctx, s.k.leases(), members...)
	for _, id := range jobIDs {
		pipe.HSet(ctx, s.k.job(id), "status", "completed", "updated_at", now)
	}
	pipe.ZAdd(ctx, s.k.completed(), done...)

	_, err := pipe.Exec(ctx)
	return err
}

func (s *Storage) ExtendLease(ctx context.Context, jobID string, ttl time.Duration) error {
	// XX only updates an existing lease; CH reports whether it did.
	n, err := s.rdb.ZAddArgs(ctx, s.k.leases(), redis.ZAddArgs{
		XX:      true,
		Ch:      true,
		Members: []redis.Z{{Score: float64(time.Now().Add(ttl).UnixMilli()), Member: jobID}},
	}).Result()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("lease not found: %s", jobID)
	}
	return nil
}

func (s *Storage) FailJob(ctx context.Context, job *domain.Job, failErr error) (storage.FailResult, error) {
	job.Attempts++
	now := time.Now()

	// Whether to retry is decided here, so a transaction is
	// enough: no script, no keys built at run time.
	pipe := s.rdb.TxPipeline()
	pipe.ZRem(ctx, s.k.leases(), job.ID)

	if job.IsRetryable() {
		nextRun := now.Add(job.Backoff.NextDelay(job.Attempts))
		pipe.HSet(ctx, s.k.job(job.ID), "status", "pending", "attempts", job.Attempts,
			"run_at", nextRun.UnixMilli(), "updated_at", now.UnixMilli())
		pipe.ZAdd(ctx, s.k.scheduled(), redis.Z{Score: float64(nextRun.UnixMilli()), Member: job.ID})
		if _, err := pipe.Exec(ctx); err != nil {
			return storage.FailResult{}, err
		}
		job.RunAt = nextRun
		return storage.FailResult{Action: storage.FailActionRetry, NextRunAt: nextRun}, nil
	}

	pipe.HSet(ctx, s.k.job(job.ID), "status", "dead_letter", "attempts", job.Attempts, "updated_at", now.UnixMilli())
	pipe.HSet(ctx, s.k.dlq(job.ID), "id", job.ID, "queue", job.Queue, "error", failErr.Error(),
		"failed_at", now.Format(time.RFC3339), "attempts", job.Attempts)
	pipe.SAdd(ctx, s.k.dlqIndex(), job.ID)
	if _, err := pipe.Exec(ctx); err != nil {
		return storage.FailResult{}, err
	}
	return storage.FailResult{Action: storage.FailActionDeadLetter}, nil
}

// ──────────────────────────────────────────────
//  Queue operations
// ──────────────────────────────────────────────

func (s *Storage) ReadyQueues(ctx context.Context) ([]domain.QueueName, error) {
	counts, err := s.readyCounts(ctx)
	if err != nil {
		return nil, err
	}

	queues := make([]domain.QueueName, 0, len(counts))
	for q := range counts {
		queues = append(queues, q)
	}
	return queues, nil
}

// readyCounts returns the ready-list length of every queue
// that has ready jobs.
func (s *Storage) readyCounts(ctx context.Context) (map[domain.QueueName]int64, error) {
	names, err := s.rdb.SMembers(ctx, s.k.queues()).Result()
	if err != nil {
		return nil, err
	}

	pipe := s.rdb.Pipeline()
	lens := make([]*redis.IntCmd, len(names))
	for i, q := range names {
		lens[i] = pipe.LLen(ctx, s.k.ready(q))
	}
	if len(names) > 0 {
		if _, err := pipe.Exec(ctx); err != nil {
			return nil, err
		}
	}

	counts := make(map[domain.QueueName]int64)
	for i, q := range names {
		if n := lens[i].Val(); n > 0 {
			counts[q] = n
		}
	}
	return counts, nil
}

// requeueScript moves job IDs onto their ready lists if each is
// still in the set KEYS[1] with a score up to ARGV[1]. KEYS holds,
// per ID in ARGV[2..], its job hash and ready list, so the
// script touches only declared keys. Checking membership makes
// it safe against instances doing the same concurrently.
var requeueScript = redis.NewScript(`
	local n = 0
	for i = 2, #ARGV do
		local id = ARGV[i]
		local score = redis.call('ZSCORE', KEYS[1], id)
		if score and tonumber(score) <= tonumber(ARGV[1]) then
			redis.call('ZREM', KEYS[1], id)
			redis.call('LPUSH', KEYS[2 * i - 1], id)
			redis.call('HSET', KEYS[2 * i - 2], 'status', 'pending', 'updated_at', ARGV[1])
			n = n + 1
		end
	end
	return n
`)

// requeue moves the given IDs of set back to their ready
// lists, as requeueScript describes. IDs whose job is gone are
// just removed from the set.
func (s *Storage) requeue(ctx context.Context, set string, ids []string, upTo int64) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}

	pipe := s.rdb.Pipeline()
	queues := make([]*redis.StringCmd, len(ids))
	for i, id := range ids {
		queues[i] = pipe.HGet(ctx, s.k.job(id), "queue")
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return 0, err
	}

	keys := []string{set}
	args := []any{upTo}
	var gone []any
	for i, id := range ids {
		queue, err := queues[i].Result()
		if err != nil {
			gone = append(gone, id)
			continue
		}
		keys = append(keys, s.k.job(id), s.k.ready(queue))
		args = append(args, id)
	}
	if len(gone) > 0 {
		if err := s.rdb.ZRem(ctx, set, gone...).Err(); err != nil {
			return 0, err
		}
	}
	if len(args) == 1 {
		return 0, nil
	}
	return requeueScript.Run(ctx, s.rdb, keys, args...).Int()
}

func (s *Storage) PromoteScheduled(ctx context.Context, now time.Time, limit int) (int, error) {
	nowMs := now.UnixMilli()
	ids, err := s.rdb.ZRangeByScore(ctx, s.k.scheduled(), &redis.ZRangeBy{
		Min: "-inf", Max: strconv.FormatInt(nowMs, 10), Count: int64(limit),
	}).Result()
	if err != nil {
		return 0, err
	}
	return s.requeue(ctx, s.k.scheduled(), ids, nowMs)
}

func (s *Storage) RecoverOrphaned(ctx context.Context, before time.Time) (int, error) {
	beforeMs := before.UnixMilli()
	// The script re-checks each lease, so one extended or acked
	// meanwhile is left alone.
	ids, err := s.rdb.ZRangeByScore(ctx, s.k.leases(), &redis.ZRangeBy{
		Min: "-inf", Max: strconv.FormatInt(beforeMs, 10),
	}).Result()
	if err != nil {
		return 0, err
	}
	return s.requeue(ctx, s.k.leases(), ids, beforeMs)
}

// ──────────────────────────────────────────────
//  Schedule operations
// ──────────────────────────────────────────────

func (s *Storage) SaveSchedule(ctx context.Context, schedule *domain.JobSchedule) error {
	return storage.SaveOne(ctx, s, schedule)
}

// saveScript writes a schedule and its indexes, unless the
// stored one has a version at least as new. Comparing and
// writing in one script needs no WATCH, so a batch of saves
// goes in one pipeline.
//
// KEYS: 1 schedule hash, 2 active set, 3 its due index,
// 4 changed index, 5-7 finished, cancelled and archived
// indexes.
//
// ARGV: 1 version, 2 status, 3 updated at, 4 next run (0:
// unset), 5 ID, then the hash fields and values.
var saveScript = redis.NewScript(fmt.Sprintf(`
	local version = tonumber(ARGV[1])
	if version > 0 then
		local stored = tonumber(redis.call('HGET', KEYS[1], 'version') or '0') or 0
		if stored >= version then return 0 end
	end
	redis.call('HSET', KEYS[1], unpack(ARGV, 6, #ARGV))

	local status, id = tonumber(ARGV[2]), ARGV[5]
	local history = {%[3]d, %[4]d, %[5]d}
	local ended = false
	for i, st in ipairs(history) do
		if st == status then
			ended = true
			redis.call('ZADD', KEYS[4 + i], ARGV[3], id)
		else
			redis.call('ZREM', KEYS[4 + i], id)
		end
	end
	if ended then
		redis.call('SREM', KEYS[2], id)
	else
		redis.call('SADD', KEYS[2], id)
	end
	if status == %[1]d then
		redis.call('ZADD', KEYS[3], ARGV[4], id)
		redis.call('ZADD', KEYS[4], ARGV[3], id)
	else
		redis.call('ZREM', KEYS[3], id)
		redis.call('ZREM', KEYS[4], id)
	end
	return 1
`, int(domain.ScheduleActive), int(domain.SchedulePaused),
	int(domain.ScheduleFinished), int(domain.ScheduleCancelled), int(domain.ScheduleArchived)))

func (s *Storage) SaveSchedules(ctx context.Context, schedules []*domain.JobSchedule) ([]error, error) {
	errs := make([]error, len(schedules))
	if len(schedules) == 0 {
		return errs, nil
	}

	now := time.Now().UnixMilli()
	calls := make([]scriptCall, len(schedules))
	for i, schedule := range schedules {
		if schedule.CreatedAt.IsZero() {
			schedule.CreatedAt = time.UnixMilli(now)
		}
		schedule.UpdatedAt = time.UnixMilli(now)

		args := []any{schedule.Version, int(schedule.Status), now, millisOrZero(schedule.NextRunAt), schedule.ID,
			"id", schedule.ID,
			"name", schedule.Name,
			"queue", schedule.Queue,
			"data", schedule.Data,
			"metadata", marshalMap(schedule.Metadata),
			"rule_type", string(schedule.Rule.Type),
			"rule_expression", schedule.Rule.Expression,
			"rule_interval_ms", schedule.Rule.IntervalMs,
			"rule_interval_nanos", schedule.Rule.IntervalNanos,
			"start_at", optMillis(schedule.Rule.StartAt),
			"end_at", optMillis(schedule.Rule.EndAt),
			"timezone", schedule.Rule.Timezone,
			"rule_offset_nanos", int64(schedule.Rule.Offset),
			"status", int(schedule.Status),
			"misfire_policy", int(schedule.MisfirePolicy),
			"last_run_at", optMillis(schedule.LastRunAt),
			"next_run_at", optMillis(schedule.NextRunAt),
			"priority", schedule.Priority,
			"max_jobs", schedule.MaxJobs,
			"generated_jobs", schedule.GeneratedJobs,
			"version", schedule.Version,
			"created_at", schedule.CreatedAt.UnixMilli(),
			"updated_at", now,
		}
		calls[i] = scriptCall{
			keys: []string{
				s.k.schedule(schedule.ID), s.k.activeSchedules(), s.k.due(domain.ScheduleShard(schedule.ID)), s.k.changed(),
				s.k.status(domain.ScheduleFinished), s.k.status(domain.ScheduleCancelled), s.k.status(domain.ScheduleArchived),
			},
			args: args,
		}
	}

	results, err := s.runScripts(ctx, saveScript, calls)
	if err != nil {
		return nil, err
	}
	for i, n := range results {
		if n == 0 {
			errs[i] = storage.ErrStaleVersion
		}
	}
	return errs, nil
}

// scriptCall is one run of a script in a pipeline.
type scriptCall struct {
	keys []string
	args []any
}

// runScripts runs script once per call in one pipeline and
// returns their integer results.
func (s *Storage) runScripts(ctx context.Context, script *redis.Script, calls []scriptCall) ([]int64, error) {
	run := func(load bool) ([]redis.Cmder, error) {
		pipe := s.rdb.Pipeline()
		for _, c := range calls {
			if load {
				script.Eval(ctx, pipe, c.keys, c.args...)
			} else {
				script.EvalSha(ctx, pipe, c.keys, c.args...)
			}
		}
		return pipe.Exec(ctx)
	}

	cmds, err := run(false)
	if err != nil && redis.HasErrorPrefix(err, "NOSCRIPT") {
		// The server lost its script cache (restart, failover):
		// send the script itself once, which caches it again.
		cmds, err = run(true)
	}
	if err != nil {
		return nil, err
	}

	results := make([]int64, len(cmds))
	for i, cmd := range cmds {
		n, err := cmd.(*redis.Cmd).Int64()
		if err != nil {
			return nil, err
		}
		results[i] = n
	}
	return results, nil
}

// index keeps a schedule in the indexes of its status: an
// active one in the active set, its shard's due index and the
// changed index; a paused one in the active set only; any
// other in its history status index.
func (s *Storage) index(ctx context.Context, pipe redis.Pipeliner, id string, status domain.ScheduleStatus, updatedMs, nextMs int64) {
	for _, st := range historyStatuses {
		if st != status {
			pipe.ZRem(ctx, s.k.status(st), id)
		}
	}
	if slices.Contains(historyStatuses, status) {
		pipe.SRem(ctx, s.k.activeSchedules(), id)
		pipe.ZAdd(ctx, s.k.status(status), redis.Z{Score: float64(updatedMs), Member: id})
	} else {
		pipe.SAdd(ctx, s.k.activeSchedules(), id)
	}

	due := s.k.due(domain.ScheduleShard(id))
	if status == domain.ScheduleActive {
		pipe.ZAdd(ctx, due, redis.Z{Score: float64(nextMs), Member: id})
		pipe.ZAdd(ctx, s.k.changed(), redis.Z{Score: float64(updatedMs), Member: id})
	} else {
		pipe.ZRem(ctx, due, id)
		pipe.ZRem(ctx, s.k.changed(), id)
	}
}

// claimScript claims one occurrence (see ClaimOccurrences) and
// stores its jobs, atomically, without WATCH.
//
// KEYS: 1 schedule hash, 2 its due index, 3 finished index,
// 4 active set, 5 changed index, 6 scheduled jobs, 7 queues,
// 8 the schedule's jobs index, then per job its hash and ready
// list.
//
// ARGV: 1 expected, 2 next (0: none), 3 now, 4 generated,
// 5 schedule ID, 6 job count, then per job: ID, queue, run at,
// created at, field count, fields and values.
var claimScript = redis.NewScript(fmt.Sprintf(`
	local st = redis.call('HMGET', KEYS[1], 'status', 'next_run_at', 'generated_jobs', 'max_jobs')
	if not st[1] or tonumber(st[1]) ~= %d then return 0 end
	local current = tonumber(st[2]) or 0
	if current ~= 0 and current ~= tonumber(ARGV[1]) then return 0 end

	local now = tonumber(ARGV[3])
	local generated = (tonumber(st[3]) or 0) + tonumber(ARGV[4])
	local maxJobs = tonumber(st[4]) or 0
	local nextRun = tonumber(ARGV[2])

	redis.call('HSET', KEYS[1], 'next_run_at', ARGV[2], 'last_run_at', ARGV[3], 'generated_jobs', generated)
	if nextRun == 0 or (maxJobs > 0 and generated >= maxJobs) then
		redis.call('HSET', KEYS[1], 'status', %d, 'updated_at', ARGV[3])
		redis.call('ZREM', KEYS[2], ARGV[5])
		redis.call('ZREM', KEYS[5], ARGV[5])
		redis.call('SREM', KEYS[4], ARGV[5])
		redis.call('ZADD', KEYS[3], ARGV[3], ARGV[5])
	else
		redis.call('ZADD', KEYS[2], ARGV[2], ARGV[5])
	end

	local a = 7
	for j = 1, tonumber(ARGV[6]) do
		local id, queue, runAt, created, n = ARGV[a], ARGV[a + 1], tonumber(ARGV[a + 2]), ARGV[a + 3], tonumber(ARGV[a + 4])
		redis.call('HSET', KEYS[7 + 2 * j], unpack(ARGV, a + 5, a + 4 + 2 * n))
		if runAt > now then
			redis.call('ZADD', KEYS[6], runAt, id)
		else
			redis.call('LPUSH', KEYS[8 + 2 * j], id)
		end
		redis.call('SADD', KEYS[7], queue)
		redis.call('ZADD', KEYS[8], created, id)
		a = a + 5 + 2 * n
	end
	return 1
`, int(domain.ScheduleActive), int(domain.ScheduleFinished)))

// ClaimOccurrences runs one claimScript per claim, all in one
// pipeline: one round trip for the batch.
func (s *Storage) ClaimOccurrences(ctx context.Context, claims []storage.Claim) ([]bool, error) {
	result := make([]bool, len(claims))
	if len(claims) == 0 {
		return result, nil
	}

	now := time.Now()
	calls := make([]scriptCall, len(claims))
	for i, c := range claims {
		var nextMs int64
		if !c.Next.IsZero() {
			nextMs = c.Next.UnixMilli()
		}
		keys := []string{
			s.k.schedule(c.ScheduleID), s.k.due(domain.ScheduleShard(c.ScheduleID)),
			s.k.status(domain.ScheduleFinished), s.k.activeSchedules(), s.k.changed(),
			s.k.scheduled(), s.k.queues(), s.k.scheduleJobs(c.ScheduleID),
		}
		args := []any{c.Expected.UnixMilli(), nextMs, now.UnixMilli(), c.Generated, c.ScheduleID, len(c.Jobs)}
		for _, job := range c.Jobs {
			stampJob(job, now)
			fields := jobFields(job)
			keys = append(keys, s.k.job(job.ID), s.k.ready(job.Queue))
			args = append(args, job.ID, job.Queue, job.RunAt.UnixMilli(), job.CreatedAt.UnixMilli(), len(fields)/2)
			args = append(args, fields...)
		}
		calls[i] = scriptCall{keys, args}
	}

	results, err := s.runScripts(ctx, claimScript, calls)
	if err != nil {
		return nil, err
	}
	for i, n := range results {
		result[i] = n == 1
	}
	return result, nil
}

func (s *Storage) DeleteSchedule(ctx context.Context, scheduleID string) error {
	pipe := s.rdb.TxPipeline()
	pipe.Del(ctx, s.k.schedule(scheduleID), s.k.scheduleJobs(scheduleID))
	pipe.SRem(ctx, s.k.activeSchedules(), scheduleID)
	pipe.ZRem(ctx, s.k.due(domain.ScheduleShard(scheduleID)), scheduleID)
	pipe.ZRem(ctx, s.k.changed(), scheduleID)
	for _, st := range historyStatuses {
		pipe.ZRem(ctx, s.k.status(st), scheduleID)
	}
	_, err := pipe.Exec(ctx)
	return err
}

// lightFields are the schedule fields the scheduler's window
// needs: all but the payload (data, metadata).
var lightFields = []string{
	"id", "name", "queue", "rule_type", "rule_expression", "rule_interval_ms", "rule_interval_nanos",
	"start_at", "end_at", "timezone", "rule_offset_nanos", "status", "misfire_policy",
	"last_run_at", "next_run_at", "priority", "max_jobs", "generated_jobs", "version",
	"created_at", "updated_at",
}

func (s *Storage) LoadDueSchedules(ctx context.Context, q storage.DueQuery) ([]*domain.JobSchedule, error) {
	var owned *[domain.ShardCount]bool
	shards := q.Shards
	if shards == nil {
		shards = make([]int, domain.ShardCount)
		for i := range shards {
			shards[i] = i
		}
	} else {
		owned = new([domain.ShardCount]bool)
		for _, sh := range shards {
			owned[sh] = true
		}
	}

	var due []redis.Z
	if !q.ChangedSince.IsZero() {
		// Few schedules change between syncs: read them from the
		// changed index rather than the whole window.
		ids, err := s.rdb.ZRangeByScore(ctx, s.k.changed(), &redis.ZRangeBy{
			Min: "(" + strconv.FormatInt(q.ChangedSince.UnixMilli(), 10), Max: "+inf",
		}).Result()
		if err != nil {
			return nil, err
		}
		for _, id := range ids {
			if owned == nil || owned[domain.ScheduleShard(id)] {
				due = append(due, redis.Z{Member: id})
			}
		}
	} else {
		min := "-inf"
		if !q.After.IsZero() {
			min = "(" + strconv.FormatInt(q.After.UnixMilli(), 10)
		}
		rng := &redis.ZRangeBy{Min: min, Max: strconv.FormatInt(q.Before.UnixMilli(), 10)}
		if q.Limit > 0 {
			rng.Count = int64(q.Limit)
		}
		pipe := s.rdb.Pipeline()
		ranges := make([]*redis.ZSliceCmd, len(shards))
		for i, sh := range shards {
			ranges[i] = pipe.ZRangeByScoreWithScores(ctx, s.k.due(sh), rng)
		}
		if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
			return nil, err
		}
		// Each shard returned its earliest; merge them.
		for _, r := range ranges {
			for _, z := range r.Val() {
				// An unset NextRunAt is scored 0: not "after".
				if q.After.IsZero() || z.Score > 0 {
					due = append(due, z)
				}
			}
		}
	}

	pipe := s.rdb.Pipeline()
	hashes := make([]*redis.SliceCmd, len(due))
	for i, z := range due {
		hashes[i] = pipe.HMGet(ctx, s.k.schedule(z.Member.(string)), lightFields...)
	}
	if len(due) > 0 {
		if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
			return nil, err
		}
	}

	result := make([]*domain.JobSchedule, 0, len(due))
	for _, h := range hashes {
		values := h.Val()
		if len(values) == 0 || values[0] == nil {
			continue
		}
		data := make(map[string]string, len(lightFields))
		for i, f := range lightFields {
			if v, ok := values[i].(string); ok {
				data[f] = v
			}
		}
		sch := parseSchedule(data)
		if sch.Status != domain.ScheduleActive ||
			(sch.NextRunAt != nil && sch.NextRunAt.After(q.Before)) ||
			(!q.After.IsZero() && (sch.NextRunAt == nil || !sch.NextRunAt.After(q.After))) {
			continue
		}
		result = append(result, sch)
	}

	slices.SortFunc(result, func(a, b *domain.JobSchedule) int {
		if c := cmp.Compare(millisOrZero(a.NextRunAt), millisOrZero(b.NextRunAt)); c != 0 {
			return c
		}
		return cmp.Compare(a.ID, b.ID)
	})
	return page(result, 0, q.Limit), nil
}

func (s *Storage) LoadSchedules(ctx context.Context, ids []string) ([]*domain.JobSchedule, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	pipe := s.rdb.Pipeline()
	hashes := make([]*redis.MapStringStringCmd, len(ids))
	for i, id := range ids {
		hashes[i] = pipe.HGetAll(ctx, s.k.schedule(id))
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}

	result := make([]*domain.JobSchedule, 0, len(ids))
	for _, h := range hashes {
		if data := h.Val(); len(data) > 0 {
			result = append(result, parseSchedule(data))
		}
	}
	return result, nil
}

func (s *Storage) PurgeHistory(ctx context.Context, before time.Time, limit int) (int, error) {
	max := "(" + strconv.FormatInt(before.UnixMilli(), 10)

	// Completed jobs, with their entry in their schedule's index.
	jobs, err := s.rdb.ZRangeByScore(ctx, s.k.completed(), &redis.ZRangeBy{Min: "-inf", Max: max, Count: int64(limit)}).Result()
	if err != nil {
		return 0, err
	}
	if len(jobs) > 0 {
		pipe := s.rdb.Pipeline()
		owners := make([]*redis.StringCmd, len(jobs))
		for i, id := range jobs {
			owners[i] = pipe.HGet(ctx, s.k.job(id), "schedule_id")
		}
		if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
			return 0, err
		}

		pipe = s.rdb.TxPipeline()
		for i, id := range jobs {
			pipe.Del(ctx, s.k.job(id))
			pipe.ZRem(ctx, s.k.completed(), id)
			if sid := owners[i].Val(); sid != "" {
				pipe.ZRem(ctx, s.k.scheduleJobs(sid), id)
			}
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return 0, err
		}
	}

	// History schedules, with their jobs index.
	schedules := 0
	for _, st := range historyStatuses {
		if schedules >= limit {
			break
		}
		ids, err := s.rdb.ZRangeByScore(ctx, s.k.status(st), &redis.ZRangeBy{
			Min: "-inf", Max: max, Count: int64(limit - schedules),
		}).Result()
		if err != nil {
			return 0, err
		}
		if len(ids) == 0 {
			continue
		}
		pipe := s.rdb.TxPipeline()
		for _, id := range ids {
			pipe.Del(ctx, s.k.schedule(id), s.k.scheduleJobs(id))
			pipe.ZRem(ctx, s.k.status(st), id)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return 0, err
		}
		schedules += len(ids)
	}

	return len(jobs) + schedules, nil
}

func (s *Storage) Heartbeat(ctx context.Context, instanceID string, ttl time.Duration) ([]string, error) {
	now := time.Now()
	pipe := s.rdb.TxPipeline()
	if ttl > 0 {
		pipe.ZAdd(ctx, s.k.instances(), redis.Z{Score: float64(now.Add(ttl).UnixMilli()), Member: instanceID})
	} else {
		pipe.ZRem(ctx, s.k.instances(), instanceID)
	}
	pipe.ZRemRangeByScore(ctx, s.k.instances(), "-inf", strconv.FormatInt(now.UnixMilli(), 10))
	members := pipe.ZRange(ctx, s.k.instances(), 0, -1)
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}

	live := members.Val()
	slices.Sort(live)
	return live, nil
}

func (s *Storage) LoadSchedule(ctx context.Context, scheduleID string) (*domain.JobSchedule, error) {
	data, err := s.rdb.HGetAll(ctx, s.k.schedule(scheduleID)).Result()
	if err != nil {
		return nil, err
	}

	if len(data) == 0 {
		return nil, fmt.Errorf("schedule not found: %s", scheduleID)
	}
	return parseSchedule(data), nil
}

func parseSchedule(data map[string]string) *domain.JobSchedule {
	sch := &domain.JobSchedule{
		ID:            data["id"],
		Name:          data["name"],
		Queue:         data["queue"],
		Priority:      int(parseInt(data["priority"])),
		MaxJobs:       int(parseInt(data["max_jobs"])),
		GeneratedJobs: int(parseInt(data["generated_jobs"])),
		Version:       parseInt(data["version"]),
	}
	if v, ok := data["data"]; ok {
		sch.Data = []byte(v)
	}
	if v, ok := data["metadata"]; ok {
		sch.Metadata = unmarshalMap(v)
	}
	sch.Rule = domain.ScheduleRule{
		Type:          domain.ScheduleType(data["rule_type"]),
		Expression:    data["rule_expression"],
		IntervalMs:    parseInt(data["rule_interval_ms"]),
		IntervalNanos: parseInt(data["rule_interval_nanos"]),
		Timezone:      data["timezone"],
		Offset:        time.Duration(parseInt(data["rule_offset_nanos"])),
	}
	sch.Status = domain.ScheduleStatus(int(parseInt(data["status"])))
	sch.MisfirePolicy = domain.MisfirePolicy(int(parseInt(data["misfire_policy"])))
	sch.CreatedAt = time.UnixMilli(parseInt(data["created_at"]))
	sch.UpdatedAt = time.UnixMilli(parseInt(data["updated_at"]))

	if t := parseInt(data["start_at"]); t > 0 {
		v := time.UnixMilli(t)
		sch.Rule.StartAt = &v
	}

	if t := parseInt(data["end_at"]); t > 0 {
		v := time.UnixMilli(t)
		sch.Rule.EndAt = &v
	}

	if t := parseInt(data["last_run_at"]); t > 0 {
		v := time.UnixMilli(t)
		sch.LastRunAt = &v
	}

	if t := parseInt(data["next_run_at"]); t > 0 {
		v := time.UnixMilli(t)
		sch.NextRunAt = &v
	}

	return sch
}

func (s *Storage) UpdateScheduleStatus(ctx context.Context, scheduleID string, status domain.ScheduleStatus) error {
	next, err := s.rdb.HGet(ctx, s.k.schedule(scheduleID), "next_run_at").Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return err
	}

	now := time.Now().UnixMilli()
	pipe := s.rdb.TxPipeline()
	pipe.HSet(ctx, s.k.schedule(scheduleID), "status", int(status), "updated_at", now)
	s.index(ctx, pipe, scheduleID, status, now, parseInt(next))
	_, err = pipe.Exec(ctx)
	return err
}

// loadActive returns all active and paused schedules.
func (s *Storage) loadActive(ctx context.Context) ([]*domain.JobSchedule, error) {
	ids, err := s.rdb.SMembers(ctx, s.k.activeSchedules()).Result()
	if err != nil {
		return nil, err
	}
	return s.LoadSchedules(ctx, ids)
}

func (s *Storage) ListSchedules(ctx context.Context, q storage.ScheduleQuery) ([]*domain.JobSchedule, error) {
	statuses := q.Statuses
	if len(statuses) == 0 {
		statuses = append([]domain.ScheduleStatus{domain.ScheduleActive, domain.SchedulePaused}, historyStatuses...)
	}
	matches := func(sch *domain.JobSchedule) bool {
		return slices.Contains(statuses, sch.Status) && (q.Queue == "" || sch.Queue == q.Queue)
	}

	var result []*domain.JobSchedule

	if slices.Contains(statuses, domain.ScheduleActive) || slices.Contains(statuses, domain.SchedulePaused) {
		active, err := s.loadActive(ctx)
		if err != nil {
			return nil, err
		}
		for _, sch := range active {
			if matches(sch) {
				result = append(result, sch)
			}
		}
	}

	// History can be large: read each status index newest first,
	// only as deep as the requested page needs.
	want := q.Offset + q.Limit
	for _, st := range historyStatuses {
		if !slices.Contains(statuses, st) {
			continue
		}
		found := 0
		for start := int64(0); q.Limit <= 0 || found < want; start += 100 {
			ids, err := s.rdb.ZRevRange(ctx, s.k.status(st), start, start+99).Result()
			if err != nil {
				return nil, err
			}
			page, err := s.LoadSchedules(ctx, ids)
			if err != nil {
				return nil, err
			}
			for _, sch := range page {
				if matches(sch) {
					result = append(result, sch)
					found++
				}
			}
			if len(ids) < 100 {
				break
			}
		}
	}

	sort.Slice(result, func(i, j int) bool {
		if result[i].UpdatedAt.Equal(result[j].UpdatedAt) {
			return result[i].ID < result[j].ID
		}
		return result[i].UpdatedAt.After(result[j].UpdatedAt)
	})
	return page(result, q.Offset, q.Limit), nil
}

func (s *Storage) ListJobs(ctx context.Context, q storage.JobQuery) ([]*domain.Job, error) {
	before := q.Before
	if before.IsZero() {
		before = time.Now().Add(time.Millisecond)
	}

	var result []*domain.Job
	for start := int64(0); ; start += 100 {
		ids, err := s.rdb.ZRevRange(ctx, s.k.scheduleJobs(q.ScheduleID), start, start+99).Result()
		if err != nil {
			return nil, err
		}
		for _, id := range ids {
			job, err := s.loadJob(ctx, id)
			if err != nil || !job.UpdatedAt.Before(before) ||
				(len(q.Statuses) > 0 && !slices.Contains(q.Statuses, job.Status)) {
				continue
			}
			result = append(result, job)
		}
		if len(ids) < 100 || (q.Limit > 0 && len(result) >= q.Limit) {
			break
		}
	}

	sort.Slice(result, func(i, j int) bool { return result[i].UpdatedAt.After(result[j].UpdatedAt) })
	return page(result, 0, q.Limit), nil
}

// ──────────────────────────────────────────────
//  Queue pause
// ──────────────────────────────────────────────

func (s *Storage) SetQueuePaused(ctx context.Context, queue domain.QueueName, paused bool) error {
	if paused {
		return s.rdb.SAdd(ctx, s.k.pausedQueues(), queue).Err()
	}
	return s.rdb.SRem(ctx, s.k.pausedQueues(), queue).Err()
}

func (s *Storage) PausedQueues(ctx context.Context) ([]domain.QueueName, error) {
	return s.rdb.SMembers(ctx, s.k.pausedQueues()).Result()
}

// ──────────────────────────────────────────────
//  DLQ operations
// ──────────────────────────────────────────────

func (s *Storage) ListDLQ(ctx context.Context, queue domain.QueueName) ([]storage.DeadLetter, error) {
	ids, err := s.rdb.SMembers(ctx, s.k.dlqIndex()).Result()
	if err != nil {
		return nil, err
	}

	var result []storage.DeadLetter
	for _, id := range ids {
		data, err := s.rdb.HGetAll(ctx, s.k.dlq(id)).Result()
		if err != nil || len(data) == 0 {
			continue
		}
		dl := storage.DeadLetter{
			ID:       data["id"],
			Queue:    data["queue"],
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
	exists, err := s.rdb.Exists(ctx, s.k.dlq(jobID)).Result()
	if err != nil || exists == 0 {
		return fmt.Errorf("dead letter not found: %s", jobID)
	}

	data, err := s.rdb.HGetAll(ctx, s.k.dlq(jobID)).Result()
	if err != nil {
		return err
	}

	queue := data["queue"]

	pipe := s.rdb.Pipeline()
	pipe.Del(ctx, s.k.dlq(jobID))
	pipe.SRem(ctx, s.k.dlqIndex(), jobID)
	now := time.Now().UnixMilli()
	pipe.HSet(ctx, s.k.job(jobID), "status", "pending", "attempts", 0,
		"run_at", now, "updated_at", now)
	pipe.LPush(ctx, s.k.ready(queue), jobID)

	_, err = pipe.Exec(ctx)

	return err
}

// ──────────────────────────────────────────────
//  Stats & Close
// ──────────────────────────────────────────────

func (s *Storage) NextScheduledAt(ctx context.Context) (time.Time, error) {
	entries, err := s.rdb.ZRangeWithScores(ctx, s.k.scheduled(), 0, 0).Result()
	if err != nil || len(entries) == 0 {
		return time.Time{}, err
	}

	return time.UnixMilli(int64(entries[0].Score)), nil
}

func (s *Storage) Stats(ctx context.Context) (storage.Stats, error) {
	var st storage.Stats
	st.ReadyCount = make(map[domain.QueueName]int64)

	counts, err := s.readyCounts(ctx)
	if err != nil {
		return st, err
	}
	st.ReadyCount = counts

	st.ScheduledCount, _ = s.rdb.ZCard(ctx, s.k.scheduled()).Result()
	st.DLQCount, _ = s.rdb.SCard(ctx, s.k.dlqIndex()).Result()
	st.LeaseCount, _ = s.rdb.ZCard(ctx, s.k.leases()).Result()

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

func millisOrZero(t *time.Time) int64 {
	if t == nil {
		return 0
	}
	return t.UnixMilli()
}

func optMillis(t *time.Time) any {
	if t == nil {
		return 0
	}
	return t.UnixMilli()
}

func parseInt(s string) int64 {
	v, _ := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	return v
}

func page[T any](items []T, offset, limit int) []T {
	if offset >= len(items) {
		return nil
	}
	items = items[offset:]
	if limit > 0 && limit < len(items) {
		items = items[:limit]
	}
	return items
}

func parseStatus(s string) domain.JobStatus {
	switch s {
	case "processing":
		return domain.StatusProcessing
	case "completed":
		return domain.StatusCompleted
	case "failed":
		return domain.StatusFailed
	case "dead_letter":
		return domain.StatusDeadLetter
	default:
		return domain.StatusPending
	}
}
