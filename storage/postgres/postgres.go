// Package postgres implements storage.Storage using PostgreSQL.
//
// It uses github.com/jackc/pgx natively (pure Go, no CGo): a
// connection pool, statements prepared once per connection, and
// batches that send several statements in one round trip.
// Several hydra instances can share one database: jobs are
// claimed with SELECT ... FOR UPDATE SKIP LOCKED, and schedule
// occurrences with a conditional UPDATE, so each is processed
// once.
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/zsweiter/hydra/domain"
	"github.com/zsweiter/hydra/storage"
)

// Storage implements storage.Storage backed by PostgreSQL.
type Storage struct {
	pool *pgxpool.Pool

	// relaxed commits bookkeeping without waiting for the WAL
	// flush (see WithRelaxedCommit).
	relaxed bool
}

var _ storage.Storage = (*Storage)(nil)

// Option configures a Storage.
type Option func(*Storage)

// WithRelaxedCommit commits writes that can be redone without
// waiting for the WAL to reach disk (synchronous_commit = off):
// job leases, acknowledgements, retries, lease recovery,
// promotions, heartbeats and history purges. A database crash
// may then lose the last few hundred milliseconds of them, so
// some jobs run again (at least once, as after an instance
// crash); a process crash loses nothing. Schedules, claims and
// new jobs always wait for the flush.
func WithRelaxedCommit() Option {
	return func(s *Storage) { s.relaxed = true }
}

// New connects to the database at dsn, e.g.
// "postgres://user:pass@host:5432/hydra?sslmode=disable", and
// creates the schema if needed. Pool settings such as
// pool_max_conns go in the DSN.
func New(dsn string, opts ...Option) (*Storage, error) {
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres open: %w", err)
	}

	return Use(pool, opts...)
}

// Use wraps an open pool and creates the schema if needed.
// Tables are prefixed with ha_ to avoid colliding with the
// application's own.
func Use(pool *pgxpool.Pool, opts ...Option) (*Storage, error) {
	s := &Storage{pool: pool}
	for _, opt := range opts {
		opt(s)
	}

	if err := s.migrate(context.Background()); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres migrate: %w", err)
	}

	return s, nil
}

// migrationLock serialises migrations of instances starting
// together; CREATE ... IF NOT EXISTS alone can race.
const migrationLock = 0x6879647261 // "hydra"

// schemaVersion is bumped whenever migrate changes the schema.
// An instance finding it current runs no DDL: even a no-op
// CREATE INDEX IF NOT EXISTS locks its table, and could
// deadlock with instances already running.
const schemaVersion = 2

// migrate creates or upgrades the schema, retrying when it
// deadlocks with running instances (an upgrade while older
// ones keep working).
func (s *Storage) migrate(ctx context.Context) error {
	for attempt := 1; ; attempt++ {
		err := s.migrateOnce(ctx)
		var pgErr *pgconn.PgError
		if err == nil || attempt == 5 || !errors.As(err, &pgErr) ||
			(pgErr.Code != "40P01" && pgErr.Code != "55P03") { // deadlock, lock not available
			return err
		}
		time.Sleep(time.Duration(attempt) * 200 * time.Millisecond)
	}
}

func (s *Storage) migrateOnce(ctx context.Context) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, migrationLock); err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS ha_schema (version INTEGER NOT NULL)`); err != nil {
		return err
	}

	var version int
	if err := tx.QueryRow(ctx, `SELECT COALESCE(MAX(version), 0) FROM ha_schema`).Scan(&version); err != nil {
		return err
	}

	if version >= schemaVersion {
		return tx.Commit(ctx)
	}

	// Fail fast rather than queue behind running instances;
	// migrate retries.
	if _, err := tx.Exec(ctx, `SET LOCAL lock_timeout = '5s'`); err != nil {
		return err
	}

	// Finished and cancelled schedules and completed jobs stay
	// as history (until purged). The indexes on hot paths are
	// partial, so they hold only the pending jobs or active
	// schedules however large the history grows:
	//
	//   - idx_jobs_claim matches ClaimJobs' ORDER BY run_at,
	//     priority, so claiming never sorts jobs due together.
	//   - idx_jobs_scheduled serves promotion and the
	//     dispatcher's next wake-up, idx_jobs_completed purges.
	//   - idx_schedules_window serves window loads and
	//     idx_schedules_changed the syncs between instances.
	_, err = tx.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS ha_jobs (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL DEFAULT '',
			queue TEXT NOT NULL,
			data BYTEA,
			metadata TEXT NOT NULL DEFAULT '{}',
			run_at BIGINT NOT NULL,
			priority INTEGER NOT NULL DEFAULT 0,
			status TEXT NOT NULL DEFAULT 'pending',
			attempts INTEGER NOT NULL DEFAULT 0,
			max_attempts INTEGER NOT NULL DEFAULT 3,
			backoff_type INTEGER NOT NULL DEFAULT 1,
			backoff_base_ms BIGINT NOT NULL DEFAULT 100,
			backoff_max_ms BIGINT NOT NULL DEFAULT 10000,
			schedule_id TEXT,
			idempotency_key TEXT NOT NULL DEFAULT '',
			created_at BIGINT NOT NULL,
			updated_at BIGINT NOT NULL
		);
		DROP INDEX IF EXISTS idx_jobs_queue_status;
		CREATE INDEX IF NOT EXISTS idx_jobs_claim ON ha_jobs(queue, run_at, priority) WHERE status = 'pending';
		CREATE INDEX IF NOT EXISTS idx_jobs_scheduled ON ha_jobs(run_at) WHERE status = 'scheduled';
		CREATE INDEX IF NOT EXISTS idx_jobs_completed ON ha_jobs(updated_at) WHERE status = 'completed';
		CREATE INDEX IF NOT EXISTS idx_jobs_schedule ON ha_jobs(schedule_id, updated_at);

		CREATE TABLE IF NOT EXISTS ha_leases (
			job_id TEXT PRIMARY KEY REFERENCES ha_jobs(id) ON DELETE CASCADE,
			worker_id TEXT NOT NULL,
			expires_at BIGINT NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_leases_expires ON ha_leases(expires_at);

		CREATE TABLE IF NOT EXISTS ha_schedules (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL DEFAULT '',
			queue TEXT NOT NULL,
			data BYTEA,
			metadata TEXT NOT NULL DEFAULT '{}',
			rule_type TEXT NOT NULL DEFAULT '',
			rule_expression TEXT NOT NULL DEFAULT '',
			rule_interval_ms BIGINT NOT NULL DEFAULT 0,
			rule_interval_nanos BIGINT NOT NULL DEFAULT 0,
			start_at BIGINT,
			end_at BIGINT,
			timezone TEXT NOT NULL DEFAULT 'UTC',
			rule_offset_nanos BIGINT NOT NULL DEFAULT 0,
			status INTEGER NOT NULL DEFAULT 0,
			misfire_policy INTEGER NOT NULL DEFAULT 0,
			last_run_at BIGINT,
			next_run_at BIGINT,
			priority INTEGER NOT NULL DEFAULT 0,
			max_jobs INTEGER NOT NULL DEFAULT 0,
			generated_jobs INTEGER NOT NULL DEFAULT 0,
			version BIGINT NOT NULL DEFAULT 0,
			shard INTEGER NOT NULL DEFAULT 0,
			created_at BIGINT NOT NULL,
			updated_at BIGINT NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_schedules_status ON ha_schedules(status, updated_at);
		DROP INDEX IF EXISTS idx_schedules_due;
		CREATE INDEX IF NOT EXISTS idx_schedules_window ON ha_schedules(next_run_at NULLS FIRST, shard) WHERE status = 0;
		CREATE INDEX IF NOT EXISTS idx_schedules_changed ON ha_schedules(updated_at) WHERE status = 0;

		CREATE TABLE IF NOT EXISTS ha_paused_queues (
			queue TEXT PRIMARY KEY
		);

		CREATE TABLE IF NOT EXISTS ha_instances (
			id TEXT PRIMARY KEY,
			expires_at BIGINT NOT NULL
		);

		CREATE TABLE IF NOT EXISTS ha_dead_letters (
			id TEXT PRIMARY KEY,
			queue TEXT NOT NULL,
			payload BYTEA,
			error TEXT NOT NULL DEFAULT '',
			failed_at BIGINT NOT NULL,
			attempts INTEGER NOT NULL DEFAULT 0
		);

		DELETE FROM ha_schema;
	`)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO ha_schema(version) VALUES ($1)`, schemaVersion); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// relaxedBatch wraps sql in a transaction that does not wait
// for the WAL flush, to send in a single round trip. Its
// results are BEGIN, SET, sql, COMMIT.
func relaxedBatch(sql string, args ...any) *pgx.Batch {
	b := &pgx.Batch{}
	b.Queue(`BEGIN`)
	b.Queue(`SET LOCAL synchronous_commit = off`)
	b.Queue(sql, args...)
	b.Queue(`COMMIT`)
	return b
}

// exec runs one statement, relaxed (see WithRelaxedCommit)
// when asked and enabled.
func (s *Storage) exec(ctx context.Context, relaxed bool, sql string, args ...any) (pgconn.CommandTag, error) {
	if !relaxed || !s.relaxed {
		return s.pool.Exec(ctx, sql, args...)
	}

	br := s.pool.SendBatch(ctx, relaxedBatch(sql, args...))
	defer br.Close()

	var tag pgconn.CommandTag
	for i := range 4 {
		t, err := br.Exec()
		if err != nil {
			return tag, err
		}
		if i == 2 {
			tag = t
		}
	}

	return tag, br.Close()
}

// query runs one statement returning rows, relaxed as exec.
func (s *Storage) query(ctx context.Context, relaxed bool, scan func(pgx.Rows) error, sql string, args ...any) error {
	if !relaxed || !s.relaxed {
		rows, err := s.pool.Query(ctx, sql, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		if err := scan(rows); err != nil {
			return err
		}
		return rows.Err()
	}

	br := s.pool.SendBatch(ctx, relaxedBatch(sql, args...))
	defer br.Close()

	for range 2 {
		if _, err := br.Exec(); err != nil {
			return err
		}
	}
	if err := scanAll(br, scan); err != nil {
		return err
	}
	if _, err := br.Exec(); err != nil {
		return err
	}

	return br.Close()
}

// scanAll reads the next result of a batch with scan.
func scanAll(br pgx.BatchResults, scan func(pgx.Rows) error) error {
	rows, err := br.Query()
	if err != nil {
		return err
	}
	if err := scan(rows); err != nil {
		rows.Close()
		return err
	}

	rows.Close()
	return rows.Err()
}

// ──────────────────────────────────────────────
//  Job operations
// ──────────────────────────────────────────────

const jobColumns = `id, name, queue, data, metadata, run_at, priority, status,
	attempts, max_attempts, backoff_type, backoff_base_ms, backoff_max_ms,
	schedule_id, idempotency_key, created_at, updated_at`

// qualified prefixes every column of a column list.
func qualified(alias, columns string) string {
	parts := strings.Split(columns, ",")
	for i, p := range parts {
		parts[i] = alias + "." + strings.TrimSpace(p)
	}

	return strings.Join(parts, ", ")
}

const upsertJob = `
	INSERT INTO ha_jobs (` + jobColumns + `)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
	ON CONFLICT (id) DO UPDATE SET
		name = EXCLUDED.name,
		queue = EXCLUDED.queue,
		data = EXCLUDED.data,
		metadata = EXCLUDED.metadata,
		run_at = EXCLUDED.run_at,
		priority = EXCLUDED.priority,
		status = EXCLUDED.status,
		attempts = EXCLUDED.attempts,
		max_attempts = EXCLUDED.max_attempts,
		backoff_type = EXCLUDED.backoff_type,
		backoff_base_ms = EXCLUDED.backoff_base_ms,
		backoff_max_ms = EXCLUDED.backoff_max_ms,
		schedule_id = EXCLUDED.schedule_id,
		idempotency_key = EXCLUDED.idempotency_key,
		updated_at = EXCLUDED.updated_at`

// jobRow stamps job for saving and returns its column values,
// in jobColumns order.
func jobRow(job *domain.Job, now time.Time) []any {
	if job.CreatedAt.IsZero() {
		job.CreatedAt = now
	}
	job.UpdatedAt = now
	job.Status = domain.StatusPending

	status := "pending"
	if job.RunAt.After(now) {
		status = "scheduled"
	}

	return []any{
		job.ID,
		job.Name,
		string(job.Queue),
		job.Data,
		marshalMap(job.Metadata),
		job.RunAt.UnixMilli(),
		int32(job.Priority),
		status,
		int32(job.Attempts),
		int32(job.MaxAttempts),
		int32(job.Backoff.Type),
		job.Backoff.BaseMs,
		job.Backoff.MaxMs,
		job.ScheduleID,
		job.IdempotencyKey,
		job.CreatedAt.UnixMilli(),
		job.UpdatedAt.UnixMilli(),
	}
}

// jobArrays holds jobs column by column, typed, for unnest.
type jobArrays struct {
	id, name, queue, metadata, status, idempotency []string
	data                                           [][]byte
	runAt, baseMs, maxMs, createdAt, updatedAt     []int64
	priority, attempts, maxAttempts, backoffType   []int32
	scheduleID                                     []*string

	// claim is the schedule of the claim each job comes with.
	claim []string
}

// add appends a row in jobRow order, of the claim of schedule.
func (a *jobArrays) add(row []any, schedule string) {
	a.claim = append(a.claim, schedule)
	a.id = append(a.id, row[0].(string))
	a.name = append(a.name, row[1].(string))
	a.queue = append(a.queue, row[2].(string))
	a.data = append(a.data, row[3].([]byte))
	a.metadata = append(a.metadata, row[4].(string))
	a.runAt = append(a.runAt, row[5].(int64))
	a.priority = append(a.priority, row[6].(int32))
	a.status = append(a.status, row[7].(string))
	a.attempts = append(a.attempts, row[8].(int32))
	a.maxAttempts = append(a.maxAttempts, row[9].(int32))
	a.backoffType = append(a.backoffType, row[10].(int32))
	a.baseMs = append(a.baseMs, row[11].(int64))
	a.maxMs = append(a.maxMs, row[12].(int64))
	a.scheduleID = append(a.scheduleID, row[13].(*string))
	a.idempotency = append(a.idempotency, row[14].(string))
	a.createdAt = append(a.createdAt, row[15].(int64))
	a.updatedAt = append(a.updatedAt, row[16].(int64))
}

// args returns the arrays in jobColumns order, then the claims.
// Empty arrays are
// sent as such, not NULL, so unnest yields no rows.
func (a *jobArrays) args() []any {
	return []any{
		nonNil(a.id), nonNil(a.name), nonNil(a.queue), nonNil(a.data), nonNil(a.metadata),
		nonNil(a.runAt), nonNil(a.priority), nonNil(a.status), nonNil(a.attempts), nonNil(a.maxAttempts),
		nonNil(a.backoffType), nonNil(a.baseMs), nonNil(a.maxMs), nonNil(a.scheduleID), nonNil(a.idempotency),
		nonNil(a.createdAt), nonNil(a.updatedAt), nonNil(a.claim),
	}
}

func nonNil[T any](v []T) []T {
	if v == nil {
		return []T{}
	}
	return v
}

func (s *Storage) SaveJob(ctx context.Context, job *domain.Job) error {
	_, err := s.pool.Exec(ctx, upsertJob, jobRow(job, time.Now())...)
	return err
}

func (s *Storage) RemoveJob(ctx context.Context, jobID string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM ha_jobs WHERE id = $1 AND status NOT IN ('processing', 'completed')`, jobID)
	if err != nil {
		return err
	}

	if tag.RowsAffected() == 0 {
		return fmt.Errorf("job not found or already processing: %s", jobID)
	}

	return nil
}

func scanJob(row pgx.Row) (*domain.Job, error) {
	job := &domain.Job{}
	var queue, meta, statusStr string
	var runAt, createdAt, updatedAt int64
	var backoffType int

	if err := row.Scan(
		&job.ID, &job.Name, &queue, &job.Data, &meta, &runAt, &job.Priority, &statusStr,
		&job.Attempts, &job.MaxAttempts, &backoffType, &job.Backoff.BaseMs, &job.Backoff.MaxMs,
		&job.ScheduleID, &job.IdempotencyKey, &createdAt, &updatedAt,
	); err != nil {
		return nil, err
	}

	job.Queue = domain.QueueName(queue)
	job.Status = parseStatus(statusStr)
	job.Backoff.Type = domain.BackoffType(backoffType)
	job.RunAt = time.UnixMilli(runAt)
	job.CreatedAt = time.UnixMilli(createdAt)
	job.UpdatedAt = time.UnixMilli(updatedAt)
	job.Metadata = unmarshalMap(meta)
	return job, nil
}

func collectJobs(out *[]*domain.Job) func(pgx.Rows) error {
	return func(rows pgx.Rows) error {
		for rows.Next() {
			job, err := scanJob(rows)
			if err != nil {
				return err
			}
			*out = append(*out, job)
		}
		return nil
	}
}

func (s *Storage) ListJobs(ctx context.Context, q storage.JobQuery) ([]*domain.Job, error) {
	before := q.Before
	if before.IsZero() {
		before = time.Now().Add(time.Millisecond)
	}

	var statuses []string
	for _, st := range q.Statuses {
		statuses = append(statuses, jobStatusText(st))
	}

	var jobs []*domain.Job
	err := s.query(ctx, false, collectJobs(&jobs), `SELECT `+jobColumns+` FROM ha_jobs
		WHERE schedule_id = $1 AND updated_at < $2 AND ($3::text[] IS NULL OR status = ANY($3))
		ORDER BY updated_at DESC LIMIT $4`,
		q.ScheduleID, before.UnixMilli(), statuses, limitOrAll(q.Limit))
	return jobs, err
}

// claimJobs leases jobs in one statement: pick them (skipping
// rows other instances hold), mark them processing and insert
// their leases.
var claimJobs = `
	WITH picked AS (
		SELECT id FROM ha_jobs
		WHERE queue = $1 AND status = 'pending'
		ORDER BY run_at, priority
		LIMIT $2
		FOR UPDATE SKIP LOCKED
	), claimed AS (
		UPDATE ha_jobs j SET status = 'processing', updated_at = $3
		FROM picked WHERE j.id = picked.id
		RETURNING ` + qualified("j", jobColumns) + `
	), leased AS (
		INSERT INTO ha_leases(job_id, worker_id, expires_at)
		SELECT id, $4, $5 FROM claimed
		ON CONFLICT (job_id) DO UPDATE SET worker_id = EXCLUDED.worker_id, expires_at = EXCLUDED.expires_at
	)
	SELECT ` + jobColumns + ` FROM claimed`

func (s *Storage) ClaimJobs(ctx context.Context, queue domain.QueueName, workerID string, n int, ttl time.Duration) ([]storage.LeasedJob, error) {
	now := time.Now()
	var jobs []*domain.Job
	err := s.query(ctx, true, collectJobs(&jobs), claimJobs,
		string(queue), n, now.UnixMilli(), workerID, now.Add(ttl).UnixMilli())
	if err != nil || len(jobs) == 0 {
		return nil, err
	}

	// RETURNING does not keep the subquery's order.
	slices.SortFunc(jobs, func(a, b *domain.Job) int {
		if c := a.RunAt.Compare(b.RunAt); c != 0 {
			return c
		}
		return a.Priority - b.Priority
	})

	result := make([]storage.LeasedJob, len(jobs))
	for i, job := range jobs {
		result[i] = storage.LeasedJob{Job: job}
	}

	return result, nil
}

func (s *Storage) AckJobs(ctx context.Context, jobIDs []string) error {
	if len(jobIDs) == 0 {
		return nil
	}
	_, err := s.exec(ctx, true, `
		WITH released AS (DELETE FROM ha_leases WHERE job_id = ANY($1))
		UPDATE ha_jobs SET status = 'completed', updated_at = $2 WHERE id = ANY($1)`,
		jobIDs, time.Now().UnixMilli())
	return err
}

func (s *Storage) ExtendLease(ctx context.Context, jobID string, ttl time.Duration) error {
	tag, err := s.exec(ctx, true, `UPDATE ha_leases SET expires_at = $1 WHERE job_id = $2`,
		time.Now().Add(ttl).UnixMilli(), jobID)
	if err != nil {
		return err
	}

	if tag.RowsAffected() == 0 {
		return fmt.Errorf("lease not found: %s", jobID)
	}

	return nil
}

func (s *Storage) FailJob(ctx context.Context, job *domain.Job, failErr error) (storage.FailResult, error) {
	job.Attempts++
	now := time.Now()
	job.UpdatedAt = now

	if job.Attempts < job.MaxAttempts {
		nextRun := now.Add(job.Backoff.NextDelay(job.Attempts))
		job.RunAt = nextRun
		job.Status = domain.StatusPending

		// A retry lost to a database crash leaves the lease,
		// which expires and recovers the job: relaxed is safe.
		if _, err := s.exec(ctx, true, `
			WITH released AS (DELETE FROM ha_leases WHERE job_id = $4)
			UPDATE ha_jobs SET status = 'pending', attempts = $1, run_at = $2, updated_at = $3 WHERE id = $4`,
			job.Attempts, nextRun.UnixMilli(), now.UnixMilli(), job.ID); err != nil {
			return storage.FailResult{}, err
		}
		return storage.FailResult{Action: storage.FailActionRetry, NextRunAt: nextRun}, nil
	}

	job.Status = domain.StatusDeadLetter
	if _, err := s.pool.Exec(ctx, `
		WITH released AS (DELETE FROM ha_leases WHERE job_id = $1),
		dead AS (UPDATE ha_jobs SET status = 'dead_letter', attempts = $2, updated_at = $3 WHERE id = $1)
		INSERT INTO ha_dead_letters(id, queue, payload, error, failed_at, attempts)
		VALUES ($1, $4, $5, $6, $3, $2)
		ON CONFLICT (id) DO UPDATE SET error = EXCLUDED.error, failed_at = EXCLUDED.failed_at, attempts = EXCLUDED.attempts`,
		job.ID, job.Attempts, now.UnixMilli(), string(job.Queue), job.Data, failErr.Error()); err != nil {
		return storage.FailResult{}, err
	}

	return storage.FailResult{Action: storage.FailActionDeadLetter}, nil
}

// ──────────────────────────────────────────────
//  Queue operations
// ──────────────────────────────────────────────

func (s *Storage) ReadyQueues(ctx context.Context) ([]domain.QueueName, error) {
	var queues []domain.QueueName
	err := s.query(ctx, false, collectQueues(&queues),
		`SELECT DISTINCT queue FROM ha_jobs WHERE status = 'pending' AND run_at <= $1`, time.Now().UnixMilli())
	return queues, err
}

func collectQueues(out *[]domain.QueueName) func(pgx.Rows) error {
	return func(rows pgx.Rows) error {
		for rows.Next() {
			var q string
			if err := rows.Scan(&q); err != nil {
				return err
			}
			*out = append(*out, domain.QueueName(q))
		}
		return nil
	}
}

func (s *Storage) PromoteScheduled(ctx context.Context, now time.Time, limit int) (int, error) {
	tag, err := s.exec(ctx, true, `
		UPDATE ha_jobs SET status = 'pending', updated_at = $1
		WHERE id IN (
			SELECT id FROM ha_jobs
			WHERE status = 'scheduled' AND run_at <= $1
			ORDER BY run_at
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)`, now.UnixMilli(), limit)
	if err != nil {
		return 0, err
	}

	return int(tag.RowsAffected()), nil
}

func (s *Storage) RecoverOrphaned(ctx context.Context, before time.Time) (int, error) {
	tag, err := s.exec(ctx, true, `
		WITH expired AS (
			DELETE FROM ha_leases WHERE expires_at <= $1 RETURNING job_id
		)
		UPDATE ha_jobs SET status = 'pending', updated_at = $1
		WHERE id IN (SELECT job_id FROM expired)`, before.UnixMilli())
	if err != nil {
		return 0, err
	}

	return int(tag.RowsAffected()), nil
}

// ──────────────────────────────────────────────
//  Schedule operations
// ──────────────────────────────────────────────

const scheduleColumns = `id, name, queue, data, metadata,
	rule_type, rule_expression, rule_interval_ms, rule_interval_nanos,
	start_at, end_at, timezone, rule_offset_nanos,
	status, misfire_policy, last_run_at, next_run_at,
	priority, max_jobs, generated_jobs, version, created_at, updated_at`

// lightColumns are scheduleColumns without the payload (data,
// metadata), for the scheduler's window loads.
const lightColumns = `id, name, queue, NULL::bytea, NULL::text,
	rule_type, rule_expression, rule_interval_ms, rule_interval_nanos,
	start_at, end_at, timezone, rule_offset_nanos,
	status, misfire_policy, last_run_at, next_run_at,
	priority, max_jobs, generated_jobs, version, created_at, updated_at`

func scanSchedule(row pgx.Row) (*domain.JobSchedule, error) {
	sch := &domain.JobSchedule{}
	var queue, ruleType string
	var meta *string
	var startAt, endAt, lastRun, nextRun *int64
	var offsetNanos int64
	var statusInt, misfireInt int
	var createdAtMillis, updatedAtMillis int64

	if err := row.Scan(
		&sch.ID, &sch.Name, &queue, &sch.Data, &meta,
		&ruleType, &sch.Rule.Expression, &sch.Rule.IntervalMs, &sch.Rule.IntervalNanos,
		&startAt, &endAt, &sch.Rule.Timezone, &offsetNanos,
		&statusInt, &misfireInt, &lastRun, &nextRun,
		&sch.Priority, &sch.MaxJobs, &sch.GeneratedJobs, &sch.Version,
		&createdAtMillis, &updatedAtMillis,
	); err != nil {
		return nil, err
	}

	sch.Queue = domain.QueueName(queue)
	sch.Rule.Type = domain.ScheduleType(ruleType)
	if meta != nil {
		sch.Metadata = unmarshalMap(*meta)
	}
	sch.Rule.Offset = time.Duration(offsetNanos)
	sch.Rule.StartAt = optTime(startAt)
	sch.Rule.EndAt = optTime(endAt)
	sch.Status = domain.ScheduleStatus(statusInt)
	sch.MisfirePolicy = domain.MisfirePolicy(misfireInt)
	sch.LastRunAt = optTime(lastRun)
	sch.NextRunAt = optTime(nextRun)
	sch.CreatedAt = time.UnixMilli(createdAtMillis)
	sch.UpdatedAt = time.UnixMilli(updatedAtMillis)
	return sch, nil
}

func (s *Storage) querySchedules(ctx context.Context, columns, where string, args ...any) ([]*domain.JobSchedule, error) {
	var result []*domain.JobSchedule
	err := s.query(ctx, false, func(rows pgx.Rows) error {
		for rows.Next() {
			sch, err := scanSchedule(rows)
			if err != nil {
				return err
			}
			result = append(result, sch)
		}
		return nil
	}, `SELECT `+columns+` FROM ha_schedules `+where, args...)
	return result, err
}

func (s *Storage) SaveSchedule(ctx context.Context, schedule *domain.JobSchedule) error {
	return storage.SaveOne(ctx, s, schedule)
}

// upsertSchedule writes a schedule unless the stored one has a
// version at least as new: the WHERE of the upsert makes that
// a no-op, detected through the affected row count.
const upsertSchedule = `INSERT INTO ha_schedules(` + scheduleColumns + `, shard)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24)
		ON CONFLICT (id) DO UPDATE SET
			name = EXCLUDED.name,
			queue = EXCLUDED.queue,
			data = EXCLUDED.data,
			metadata = EXCLUDED.metadata,
			rule_type = EXCLUDED.rule_type,
			rule_expression = EXCLUDED.rule_expression,
			rule_interval_ms = EXCLUDED.rule_interval_ms,
			rule_interval_nanos = EXCLUDED.rule_interval_nanos,
			start_at = EXCLUDED.start_at,
			end_at = EXCLUDED.end_at,
			timezone = EXCLUDED.timezone,
			rule_offset_nanos = EXCLUDED.rule_offset_nanos,
			status = EXCLUDED.status,
			misfire_policy = EXCLUDED.misfire_policy,
			last_run_at = EXCLUDED.last_run_at,
			next_run_at = EXCLUDED.next_run_at,
			priority = EXCLUDED.priority,
			max_jobs = EXCLUDED.max_jobs,
			generated_jobs = EXCLUDED.generated_jobs,
			version = EXCLUDED.version,
			updated_at = EXCLUDED.updated_at
		WHERE EXCLUDED.version = 0 OR EXCLUDED.version > ha_schedules.version`

// scheduleArgs stamps schedule for saving and returns the
// values of upsertSchedule.
func scheduleArgs(schedule *domain.JobSchedule, now int64) []any {
	if schedule.CreatedAt.IsZero() {
		schedule.CreatedAt = time.UnixMilli(now)
	}
	schedule.UpdatedAt = time.UnixMilli(now)

	return []any{
		schedule.ID,
		schedule.Name,
		string(schedule.Queue),
		schedule.Data,
		marshalMap(schedule.Metadata),
		string(schedule.Rule.Type),
		schedule.Rule.Expression,
		schedule.Rule.IntervalMs,
		schedule.Rule.IntervalNanos,
		optMillis(schedule.Rule.StartAt),
		optMillis(schedule.Rule.EndAt),
		schedule.Rule.Timezone,
		int64(schedule.Rule.Offset),
		int32(schedule.Status),
		int32(schedule.MisfirePolicy),
		optMillis(schedule.LastRunAt),
		optMillis(schedule.NextRunAt),
		int32(schedule.Priority),
		int32(schedule.MaxJobs),
		int32(schedule.GeneratedJobs),
		schedule.Version,
		schedule.CreatedAt.UnixMilli(),
		schedule.UpdatedAt.UnixMilli(),
		int32(domain.ScheduleShard(schedule.ID)),
	}
}

// SaveSchedules writes the batch in one transaction, sent in
// one round trip.
func (s *Storage) SaveSchedules(ctx context.Context, schedules []*domain.JobSchedule) ([]error, error) {
	errs := make([]error, len(schedules))
	if len(schedules) == 0 {
		return errs, nil
	}

	now := time.Now().UnixMilli()
	b := &pgx.Batch{}
	b.Queue(`BEGIN`)
	for _, schedule := range schedules {
		b.Queue(upsertSchedule, scheduleArgs(schedule, now)...)
	}
	b.Queue(`COMMIT`)
	br := s.pool.SendBatch(ctx, b)
	defer br.Close()

	if _, err := br.Exec(); err != nil {
		return nil, err
	}
	for i := range schedules {
		tag, err := br.Exec()
		if err != nil {
			return nil, err
		}
		if tag.RowsAffected() == 0 {
			errs[i] = storage.ErrStaleVersion
		}
	}
	if _, err := br.Exec(); err != nil {
		return nil, err
	}

	return errs, br.Close()
}

// lockSchedules locks the claimed schedules in ID order first,
// so batches of instances claiming overlapping schedules
// queue up instead of deadlocking.
const lockSchedules = `SELECT id FROM ha_schedules WHERE id = ANY($1) ORDER BY id FOR UPDATE`

// claimOccurrences claims a batch in one statement: each claim
// moves its schedule's NextRunAt if it still is the expected
// one, and the jobs of the claims that succeeded are inserted.
var claimOccurrences = fmt.Sprintf(`
	WITH c AS (
		SELECT * FROM unnest($1::text[], $2::bigint[], $3::bigint[], $4::int[]) AS c(id, expected, next, generated)
	), claimed AS (
		UPDATE ha_schedules s SET
			next_run_at = c.next,
			last_run_at = $5,
			generated_jobs = s.generated_jobs + c.generated,
			status = CASE
				WHEN c.next IS NULL OR (s.max_jobs > 0 AND s.generated_jobs + c.generated >= s.max_jobs) THEN %[1]d
				ELSE s.status
			END,
			updated_at = CASE
				WHEN c.next IS NULL OR (s.max_jobs > 0 AND s.generated_jobs + c.generated >= s.max_jobs) THEN $5
				ELSE s.updated_at
			END
		FROM c
		WHERE s.id = c.id AND s.status = %[2]d AND (s.next_run_at = c.expected OR s.next_run_at IS NULL)
		RETURNING s.id
	), jobs AS (
		INSERT INTO ha_jobs (`+jobColumns+`)
		SELECT `+jobColumns+` FROM unnest(
			$6::text[], $7::text[], $8::text[], $9::bytea[], $10::text[], $11::bigint[], $12::int[], $13::text[],
			$14::int[], $15::int[], $16::int[], $17::bigint[], $18::bigint[], $19::text[], $20::text[],
			$21::bigint[], $22::bigint[], $23::text[]
		) AS j(`+jobColumns+`, claim)
		WHERE j.claim IN (SELECT id FROM claimed)
		ON CONFLICT (id) DO NOTHING
	)
	SELECT id FROM claimed`, int(domain.ScheduleFinished), int(domain.ScheduleActive))

// ClaimOccurrences claims a batch in one round trip: the lock,
// the claims and their jobs go in one transaction, one batch.
// Under READ COMMITTED a claim racing another instance's waits
// for the row lock, then re-checks NextRunAt against the
// committed value, so only one of them wins.
func (s *Storage) ClaimOccurrences(ctx context.Context, claims []storage.Claim) ([]bool, error) {
	result := make([]bool, len(claims))
	if len(claims) == 0 {
		return result, nil
	}

	now := time.Now()
	ids := make([]string, len(claims))
	expected := make([]int64, len(claims))
	next := make([]*int64, len(claims))
	generated := make([]int32, len(claims))
	var jobs jobArrays
	for i, c := range claims {
		ids[i] = c.ScheduleID
		expected[i] = c.Expected.UnixMilli()
		if !c.Next.IsZero() {
			ms := c.Next.UnixMilli()
			next[i] = &ms
		}
		generated[i] = int32(c.Generated)
		for _, job := range c.Jobs {
			jobs.add(jobRow(job, now), c.ScheduleID)
		}
	}

	args := append([]any{ids, expected, next, generated, now.UnixMilli()}, jobs.args()...)

	b := &pgx.Batch{}
	b.Queue(`BEGIN`)
	b.Queue(lockSchedules, ids)
	b.Queue(claimOccurrences, args...)
	b.Queue(`COMMIT`)
	br := s.pool.SendBatch(ctx, b)
	defer br.Close()

	for range 2 {
		if _, err := br.Exec(); err != nil {
			return nil, err
		}
	}
	won := make(map[string]bool, len(claims))
	err := scanAll(br, func(rows pgx.Rows) error {
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				return err
			}
			won[id] = true
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if _, err := br.Exec(); err != nil {
		return nil, err
	}
	if err := br.Close(); err != nil {
		return nil, err
	}

	for i, c := range claims {
		result[i] = won[c.ScheduleID]
	}
	return result, nil
}

func (s *Storage) DeleteSchedule(ctx context.Context, scheduleID string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM ha_schedules WHERE id = $1`, scheduleID)
	return err
}

func (s *Storage) LoadDueSchedules(ctx context.Context, q storage.DueQuery) ([]*domain.JobSchedule, error) {
	var after, changed *int64
	if !q.After.IsZero() {
		ms := q.After.UnixMilli()
		after = &ms
	}
	if !q.ChangedSince.IsZero() {
		ms := q.ChangedSince.UnixMilli()
		changed = &ms
	}
	var shards []int32
	if q.Shards != nil {
		shards = make([]int32, len(q.Shards))
		for i, sh := range q.Shards {
			shards[i] = int32(sh)
		}
	}

	// status is a literal so the partial indexes apply. An
	// unset NextRunAt is due now, hence NULLS FIRST.
	return s.querySchedules(ctx, lightColumns, fmt.Sprintf(`
		WHERE status = %d AND (next_run_at IS NULL OR next_run_at <= $1)
			AND ($2::bigint IS NULL OR next_run_at > $2)
			AND ($3::bigint IS NULL OR updated_at > $3)
			AND ($4::int[] IS NULL OR shard = ANY($4))
		ORDER BY next_run_at NULLS FIRST, id
		LIMIT $5`, int(domain.ScheduleActive)),
		q.Before.UnixMilli(), after, changed, shards, limitOrAll(q.Limit))
}

func (s *Storage) LoadSchedules(ctx context.Context, ids []string) ([]*domain.JobSchedule, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	return s.querySchedules(ctx, scheduleColumns, `WHERE id = ANY($1)`, ids)
}

func (s *Storage) PurgeHistory(ctx context.Context, before time.Time, limit int) (int, error) {
	jobs, err := s.exec(ctx, true, `DELETE FROM ha_jobs WHERE id IN (
		SELECT id FROM ha_jobs WHERE status = 'completed' AND updated_at < $1 LIMIT $2)`,
		before.UnixMilli(), limit)
	if err != nil {
		return 0, err
	}
	schedules, err := s.exec(ctx, true, `DELETE FROM ha_schedules WHERE id IN (
		SELECT id FROM ha_schedules WHERE status = ANY($1) AND updated_at < $2 LIMIT $3)`,
		[]int32{int32(domain.ScheduleFinished), int32(domain.ScheduleCancelled), int32(domain.ScheduleArchived)},
		before.UnixMilli(), limit)
	if err != nil {
		return 0, err
	}

	return int(jobs.RowsAffected() + schedules.RowsAffected()), nil
}

func (s *Storage) Heartbeat(ctx context.Context, instanceID string, ttl time.Duration) ([]string, error) {
	now := time.Now()
	var expires *int64
	if ttl > 0 {
		ms := now.Add(ttl).UnixMilli()
		expires = &ms
	}

	// One statement: drop expired members (and this one when
	// leaving), renew this one, list the others alive.
	var live []string
	err := s.query(ctx, true, func(rows pgx.Rows) error {
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				return err
			}
			live = append(live, id)
		}
		return nil
	}, `
		WITH expired AS (
			DELETE FROM ha_instances WHERE expires_at <= $3 OR (id = $1 AND $2::bigint IS NULL)
		), renewed AS (
			INSERT INTO ha_instances(id, expires_at) SELECT $1, $2 WHERE $2::bigint IS NOT NULL
			ON CONFLICT (id) DO UPDATE SET expires_at = EXCLUDED.expires_at
			RETURNING id
		)
		SELECT id FROM ha_instances WHERE expires_at > $3 AND id <> $1
		UNION SELECT id FROM renewed
		ORDER BY id`,
		instanceID, expires, now.UnixMilli())
	return live, err
}

func (s *Storage) LoadSchedule(ctx context.Context, scheduleID string) (*domain.JobSchedule, error) {
	sch, err := scanSchedule(s.pool.QueryRow(ctx, `SELECT `+scheduleColumns+` FROM ha_schedules WHERE id = $1`, scheduleID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("schedule not found: %s", scheduleID)
	}
	if err != nil {
		return nil, fmt.Errorf("load schedule: %w", err)
	}
	return sch, nil
}

func (s *Storage) ListSchedules(ctx context.Context, q storage.ScheduleQuery) ([]*domain.JobSchedule, error) {
	var statuses []int32
	for _, st := range q.Statuses {
		statuses = append(statuses, int32(st))
	}

	return s.querySchedules(ctx, scheduleColumns, `
		WHERE ($1::int[] IS NULL OR status = ANY($1)) AND ($2 = '' OR queue = $2)
		ORDER BY updated_at DESC, id
		LIMIT $3 OFFSET $4`,
		statuses, string(q.Queue), limitOrAll(q.Limit), q.Offset)
}

func (s *Storage) UpdateScheduleStatus(ctx context.Context, scheduleID string, status domain.ScheduleStatus) error {
	_, err := s.pool.Exec(ctx, `UPDATE ha_schedules SET status = $1, updated_at = $2 WHERE id = $3`,
		int32(status), time.Now().UnixMilli(), scheduleID)
	return err
}

// ──────────────────────────────────────────────
//  Queue pause
// ──────────────────────────────────────────────

func (s *Storage) SetQueuePaused(ctx context.Context, queue domain.QueueName, paused bool) error {
	var err error
	if paused {
		_, err = s.pool.Exec(ctx, `INSERT INTO ha_paused_queues(queue) VALUES ($1) ON CONFLICT DO NOTHING`, string(queue))
	} else {
		_, err = s.pool.Exec(ctx, `DELETE FROM ha_paused_queues WHERE queue = $1`, string(queue))
	}

	return err
}

func (s *Storage) PausedQueues(ctx context.Context) ([]domain.QueueName, error) {
	var queues []domain.QueueName
	err := s.query(ctx, false, collectQueues(&queues), `SELECT queue FROM ha_paused_queues`)
	return queues, err
}

// ──────────────────────────────────────────────
//  DLQ operations
// ──────────────────────────────────────────────

func (s *Storage) ListDLQ(ctx context.Context, queue domain.QueueName) ([]storage.DeadLetter, error) {
	var result []storage.DeadLetter
	err := s.query(ctx, false, func(rows pgx.Rows) error {
		for rows.Next() {
			var dl storage.DeadLetter
			var q string
			var failedAt int64
			if err := rows.Scan(&dl.ID, &q, &dl.Payload, &dl.Error, &failedAt, &dl.Attempts); err != nil {
				return err
			}
			dl.Queue = domain.QueueName(q)
			dl.FailedAt = time.UnixMilli(failedAt)
			result = append(result, dl)
		}
		return nil
	}, `SELECT id, queue, payload, error, failed_at, attempts FROM ha_dead_letters
		WHERE $1 = '' OR queue = $1 ORDER BY failed_at DESC`, string(queue))
	return result, err
}

func (s *Storage) RequeueDLQ(ctx context.Context, jobID string) error {
	// The job row is kept as dead_letter; put it back as ready.
	tag, err := s.pool.Exec(ctx, `
		WITH requeued AS (DELETE FROM ha_dead_letters WHERE id = $1 RETURNING id)
		UPDATE ha_jobs SET status = 'pending', attempts = 0, run_at = $2, updated_at = $2
		WHERE id IN (SELECT id FROM requeued)`, jobID, time.Now().UnixMilli())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("dead letter not found: %s", jobID)
	}
	return nil
}

// ──────────────────────────────────────────────
//  Stats & Close
// ──────────────────────────────────────────────

func (s *Storage) NextScheduledAt(ctx context.Context) (time.Time, error) {
	var runAt int64
	err := s.pool.QueryRow(ctx, `SELECT run_at FROM ha_jobs WHERE status = 'scheduled' ORDER BY run_at LIMIT 1`).Scan(&runAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, err
	}
	return time.UnixMilli(runAt), nil
}

func (s *Storage) Stats(ctx context.Context) (storage.Stats, error) {
	st := storage.Stats{ReadyCount: make(map[domain.QueueName]int64)}

	err := s.query(ctx, false, func(rows pgx.Rows) error {
		for rows.Next() {
			var q string
			var c int64
			if err := rows.Scan(&q, &c); err != nil {
				return err
			}
			st.ReadyCount[domain.QueueName(q)] = c
		}
		return nil
	}, `SELECT queue, COUNT(*) FROM ha_jobs WHERE status = 'pending' AND run_at <= $1 GROUP BY queue`, time.Now().UnixMilli())
	if err != nil {
		return st, err
	}

	err = s.pool.QueryRow(ctx, `SELECT
		(SELECT COUNT(*) FROM ha_jobs WHERE status = 'scheduled'),
		(SELECT COUNT(*) FROM ha_dead_letters),
		(SELECT COUNT(*) FROM ha_leases)`).Scan(&st.ScheduledCount, &st.DLQCount, &st.LeaseCount)
	return st, err
}

func (s *Storage) Close() error {
	s.pool.Close()
	return nil
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

func optMillis(t *time.Time) *int64 {
	if t == nil {
		return nil
	}
	ms := t.UnixMilli()
	return &ms
}

func optTime(v *int64) *time.Time {
	if v == nil {
		return nil
	}
	t := time.UnixMilli(*v)
	return &t
}

// limitOrAll maps a non-positive limit to NULL, which LIMIT
// reads as no limit.
func limitOrAll(limit int) *int64 {
	if limit <= 0 {
		return nil
	}
	l := int64(limit)
	return &l
}

func jobStatusText(st domain.JobStatus) string {
	switch st {
	case domain.StatusProcessing:
		return "processing"
	case domain.StatusCompleted:
		return "completed"
	case domain.StatusFailed:
		return "failed"
	case domain.StatusDeadLetter:
		return "dead_letter"
	default:
		return "pending"
	}
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
