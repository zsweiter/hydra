// Package sqlite implements storage.Storage using SQLite in WAL mode.
//
// This driver uses github.com/mattn/go-sqlite3 (CGo). Builds require
// a C compiler and CGO_ENABLED=1.
package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/zsweiter/hydra/domain"
	"github.com/zsweiter/hydra/storage"
)

// Storage implements storage.Storage backed by SQLite.
type Storage struct {
	db *sql.DB
}

// Open opens a SQLite database and initialises the schema.
// The DSN is opened with WAL journal mode and a busy timeout.
func Use(db *sql.DB) (*Storage, error) {
	db.SetMaxOpenConns(1) // SQLite serialises writes anyway

	s := &Storage{db: db}

	// run migration on startup
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, fmt.Errorf("sqlite migrate: %w", err)
	}

	return s, nil
}

// Open opens a SQLite database and initialises the schema.
// The DSN is opened with WAL journal mode and a busy timeout.
func New(path string) (*Storage, error) {
	connStr := path
	if connStr == "" {
		connStr = "file:hydra.db"
	}
	// Append query params
	sep := "?"
	if contains(connStr, "?") {
		sep = "&"
	}
	connStr += sep + "_journal_mode=WAL&_busy_timeout=5000&_txlock=immediate"

	db, err := sql.Open("sqlite3", connStr)
	if err != nil {
		return nil, fmt.Errorf("sqlite open: %w", err)
	}
	db.SetMaxOpenConns(1) // SQLite serialises writes anyway

	s := &Storage{db: db}

	// run migration on startup
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, fmt.Errorf("sqlite migrate: %w", err)
	}

	return s, nil
}

// use ha_* prefix for prevent name colision if user provided external conection
func (s *Storage) migrate() error {
	_, err := s.db.ExecContext(context.Background(), `
		CREATE TABLE IF NOT EXISTS ha_jobs (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL DEFAULT '',
			queue TEXT NOT NULL,
			data BLOB,
			metadata TEXT DEFAULT '{}',
			run_at INTEGER NOT NULL,
			priority INTEGER DEFAULT 0,
			status TEXT NOT NULL DEFAULT 'pending',
			attempts INTEGER DEFAULT 0,
			max_attempts INTEGER DEFAULT 3,
			backoff_type INTEGER DEFAULT 1,
			backoff_base_ms INTEGER DEFAULT 100,
			backoff_max_ms INTEGER DEFAULT 10000,
			schedule_id TEXT,
			idempotency_key TEXT DEFAULT '',
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_jobs_queue_status ON ha_jobs(queue, status, run_at);
		
		CREATE TABLE IF NOT EXISTS ha_leases (
			job_id TEXT PRIMARY KEY REFERENCES ha_jobs(id),
			worker_id TEXT NOT NULL,
			expires_at INTEGER NOT NULL
		);
		
		CREATE TABLE IF NOT EXISTS ha_schedules (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL DEFAULT '',
			queue TEXT NOT NULL,
			data BLOB,
			metadata TEXT DEFAULT '{}',
			rule_type TEXT NOT NULL DEFAULT '',
			rule_expression TEXT DEFAULT '',
			rule_interval_ms INTEGER DEFAULT 0,
			rule_interval_nanos INTEGER DEFAULT 0,
			start_at INTEGER,
			end_at INTEGER,
			timezone TEXT DEFAULT 'UTC',
			status INTEGER NOT NULL DEFAULT 0,
			misfire_policy INTEGER NOT NULL DEFAULT 0,
			last_run_at INTEGER,
			next_run_at INTEGER,
			priority INTEGER DEFAULT 0,
			max_jobs INTEGER DEFAULT 0,
			generated_jobs INTEGER DEFAULT 0,
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL
		);
		
		CREATE TABLE IF NOT EXISTS ha_dead_letters (
			id TEXT PRIMARY KEY,
			queue TEXT NOT NULL,
			payload BLOB,
			error TEXT DEFAULT '',
			failed_at INTEGER NOT NULL,
			attempts INTEGER DEFAULT 0
		);
	`)

	return err
}

func (s *Storage) SaveJob(ctx context.Context, job *domain.Job) error {
	now := time.Now()
	meta := marshalMap(job.Metadata)

	if job.CreatedAt.IsZero() {
		job.CreatedAt = now
	}

	job.UpdatedAt = now
	job.Status = domain.StatusPending

	status := "pending"

	if job.RunAt.After(now) {
		status = "scheduled"
	}

	query := `
		INSERT INTO ha_jobs (
			id,
			name,
			queue,
			data,
			metadata,
			run_at,
			priority,
			status,
			attempts,
			max_attempts,
			backoff_type,
			backoff_base_ms,
			backoff_max_ms,
			schedule_id,
			idempotency_key,
			created_at,
			updated_at
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			name = excluded.name,
			queue = excluded.queue,
			data = excluded.data,
			metadata = excluded.metadata,
			run_at = excluded.run_at,
			priority = excluded.priority,
			status = excluded.status,
			attempts = excluded.attempts,
			max_attempts = excluded.max_attempts,
			backoff_type = excluded.backoff_type,
			backoff_base_ms = excluded.backoff_base_ms,
			backoff_max_ms = excluded.backoff_max_ms,
			schedule_id = excluded.schedule_id,
			idempotency_key = excluded.idempotency_key,
			updated_at = excluded.updated_at
	`

	_, err := s.db.ExecContext(
		ctx,
		query,
		job.ID,
		job.Name,
		job.Queue,
		job.Data,
		meta,
		job.RunAt.UnixMilli(),
		job.Priority,
		status,
		job.Attempts,
		job.MaxAttempts,
		int(job.Backoff.Type),
		job.Backoff.BaseMs,
		job.Backoff.MaxMs,
		nilStr(job.ScheduleID),
		job.IdempotencyKey,
		job.CreatedAt.UnixMilli(),
		job.UpdatedAt.UnixMilli(),
	)

	return err
}

func (s *Storage) RemoveJob(ctx context.Context, jobID string) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM ha_jobs WHERE id = ? AND status NOT IN ('processing','completed')`, jobID)
	if err != nil {
		return err
	}

	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("job not found or already processing: %s", jobID)
	}

	return nil
}

func (s *Storage) ClaimJobs(ctx context.Context, queue domain.QueueName, workerID string, n int, ttl time.Duration) ([]storage.LeasedJob, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// Select pending jobs for this queue
	query := `SELECT 
		id,
		name,
		queue,
		data,
		metadata,
		run_at,
		priority,
		status,
		attempts,
		max_attempts,
		backoff_type,
		backoff_base_ms,
		backoff_max_ms,
		schedule_id,
		idempotency_key,
		created_at,
		updated_at
	FROM ha_jobs WHERE queue = ? AND status = 'pending' ORDER BY run_at,priority LIMIT ?`

	rows, err := tx.QueryContext(ctx, query, queue, n)
	if err != nil {
		return nil, err
	}

	var jobs []*domain.Job
	for rows.Next() {
		job := &domain.Job{}
		var meta string
		var runAt int64
		var createdAt, updatedAt int64
		var schedID sql.NullString
		var backoffType int
		var statusStr string
		err := rows.Scan(&job.ID,
			&job.Name,
			&job.Queue,
			&job.Data,
			&meta,
			&runAt,
			&job.Priority,
			&statusStr,
			&job.Attempts,
			&job.MaxAttempts,
			&backoffType,
			&job.Backoff.BaseMs,
			&job.Backoff.MaxMs,
			&schedID,
			&job.IdempotencyKey,
			&createdAt,
			&updatedAt,
		)

		if err != nil {
			rows.Close()
			return nil, err
		}

		job.Status = parseStatus(statusStr)
		job.Backoff.Type = domain.BackoffType(backoffType)
		job.RunAt = time.UnixMilli(runAt)
		job.CreatedAt = time.UnixMilli(createdAt)
		job.UpdatedAt = time.UnixMilli(updatedAt)
		if schedID.Valid {
			job.ScheduleID = &schedID.String
		}

		job.Metadata = unmarshalMap(meta)
		jobs = append(jobs, job)
	}
	rows.Close()

	if len(jobs) == 0 {
		return nil, nil
	}

	// Mark as processing and insert leases
	now := time.Now()
	expires := now.Add(ttl)
	result := make([]storage.LeasedJob, len(jobs))
	for i, job := range jobs {
		job.Status = domain.StatusProcessing
		job.UpdatedAt = now
		if _, err := tx.ExecContext(ctx, `UPDATE ha_jobs SET status = ?,updated_at = ? WHERE id = ?`, "processing", now.UnixMilli(), job.ID); err != nil {
			return nil, err
		}
		if _, err := tx.ExecContext(ctx, `INSERT OR REPLACE INTO ha_leases(job_id, worker_id, expires_at) VALUES(?,?,?)`, job.ID, workerID, expires.UnixMilli()); err != nil {
			return nil, err
		}
		result[i] = storage.LeasedJob{Job: job}
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return result, nil
}

func (s *Storage) AckJob(ctx context.Context, jobID string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM ha_leases WHERE job_id = ?`, jobID)
	return err
}

// !TODO: considering moving to transaction or atomic operation
func (s *Storage) FailJob(ctx context.Context, job *domain.Job, failErr error) (storage.FailResult, error) {
	job.Attempts++
	job.UpdatedAt = time.Now()

	if job.Attempts < job.MaxAttempts {
		delay := job.Backoff.NextDelay(job.Attempts)
		nextRun := time.Now().Add(delay)
		job.RunAt = nextRun
		job.Status = domain.StatusPending

		_, err := s.db.ExecContext(ctx, `DELETE FROM ha_leases WHERE job_id = ?`, job.ID)
		if err != nil {
			return storage.FailResult{}, err
		}

		query := `UPDATE ha_jobs SET status = ?, attempts = ?, run_at = ?,updated_at = ? WHERE id = ?`
		_, err = s.db.ExecContext(ctx, query, "pending", job.Attempts, nextRun.UnixMilli(), job.UpdatedAt.UnixMilli(), job.ID)
		if err != nil {
			return storage.FailResult{}, err
		}

		return storage.FailResult{Action: storage.FailActionRetry, NextRunAt: nextRun}, nil
	}

	job.Status = domain.StatusDeadLetter
	_, err := s.db.ExecContext(ctx, `DELETE FROM ha_leases WHERE job_id = ?`, job.ID)
	if err != nil {
		return storage.FailResult{}, err
	}

	_, err = s.db.ExecContext(ctx, `UPDATE ha_jobs SET status = ?, attempts = ?, updated_at = ? WHERE id = ?`,
		"dead_letter", job.Attempts, job.UpdatedAt.UnixMilli(), job.ID)
	if err != nil {
		return storage.FailResult{}, err
	}

	_, err = s.db.ExecContext(ctx, `INSERT INTO ha_dead_letters(id, queue, payload, error, failed_at, attempts) VALUES(?,?,?,?,?,?)`,
		job.ID, job.Queue, job.Data, failErr.Error(), time.Now().UnixMilli(), job.Attempts)
	if err != nil {
		return storage.FailResult{}, err
	}

	return storage.FailResult{Action: storage.FailActionDeadLetter}, nil
}

// ──────────────────────────────────────────────
//  Queue operations
// ──────────────────────────────────────────────

func (s *Storage) ReadyQueues(ctx context.Context) ([]domain.QueueName, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT queue FROM ha_jobs WHERE status = 'pending' AND run_at <= ?`, time.Now().UnixMilli())
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var queues []domain.QueueName
	for rows.Next() {
		var q domain.QueueName
		if err := rows.Scan(&q); err != nil {
			return nil, err
		}
		queues = append(queues, q)
	}

	return queues, rows.Err()
}

func (s *Storage) PromoteScheduled(ctx context.Context, now time.Time, limit int) (int, error) {
	// SQLite does not support LIMIT in UPDATE directly.
	// Use a subquery with rowid to apply the limit.
	res, err := s.db.ExecContext(ctx, `UPDATE ha_jobs SET status = 'pending', updated_at = ? WHERE rowid IN (SELECT rowid FROM ha_jobs WHERE status = 'scheduled' AND run_at <= ? ORDER BY run_at LIMIT ?)`, now.UnixMilli(), now.UnixMilli(), limit)
	if err != nil {
		return 0, err
	}

	n, _ := res.RowsAffected()

	return int(n), nil
}

func (s *Storage) RecoverOrphaned(ctx context.Context, before time.Time) (int, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `SELECT l.job_id FROM ha_leases l WHERE l.expires_at <= ?`, before.UnixMilli())
	if err != nil {
		return 0, err
	}

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
	}

	rows.Close()

	for _, id := range ids {
		if _, err := tx.ExecContext(ctx, `DELETE FROM ha_leases WHERE job_id = ?`, id); err != nil {
			return 0, err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE ha_jobs SET status = 'pending', updated_at = ? WHERE id = ?`, before.UnixMilli(), id); err != nil {
			return 0, err
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}

	return len(ids), nil
}

// ──────────────────────────────────────────────
//  Schedule operations
// ──────────────────────────────────────────────

func (s *Storage) SaveSchedule(ctx context.Context, schedule *domain.JobSchedule) error {
	meta := marshalMap(schedule.Metadata)
	now := time.Now().UnixMilli()
	if schedule.CreatedAt.IsZero() {
		schedule.CreatedAt = time.UnixMilli(now)
	}

	schedule.UpdatedAt = time.UnixMilli(now)

	query := `INSERT INTO ha_schedules(id,
		name,
		queue,
		data,
		metadata,
		rule_type,
		rule_expression,
		rule_interval_ms,
		rule_interval_nanos,
		start_at,
		end_at,
		timezone,
		status,
		misfire_policy,
		last_run_at,
		next_run_at,
		priority,
		max_jobs,
		generated_jobs,
		created_at,
		updated_at
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) 
	 	ON CONFLICT(id) DO UPDATE SET 
		name = excluded.name,
		queue = excluded.queue,
		data = excluded.data,
		metadata = excluded.metadata,
		rule_type = excluded.rule_type,
		rule_expression = excluded.rule_expression,
		rule_interval_ms = excluded.rule_interval_ms,
		rule_interval_nanos = excluded.rule_interval_nanos,
		start_at = excluded.start_at,
		end_at = excluded.end_at,
		timezone = excluded.timezone,
		status = excluded.status,
		misfire_policy = excluded.misfire_policy,
		last_run_at = excluded.last_run_at,
		next_run_at = excluded.next_run_at,
		priority = excluded.priority,
		max_jobs = excluded.max_jobs,
		generated_jobs = excluded.generated_jobs,
		updated_at = excluded.updated_at`

	_, err := s.db.ExecContext(
		ctx,
		query,

		schedule.ID,
		schedule.Name,
		schedule.Queue,
		schedule.Data,
		meta,
		string(schedule.Rule.Type),
		schedule.Rule.Expression,
		schedule.Rule.IntervalMs,
		schedule.Rule.IntervalNanos,
		optMillis(schedule.Rule.StartAt),
		optMillis(schedule.Rule.EndAt),
		schedule.Rule.Timezone,
		int(schedule.Status),
		int(schedule.MisfirePolicy),
		optMillis(schedule.LastRunAt),
		optMillis(schedule.NextRunAt),
		schedule.Priority,
		schedule.MaxJobs,
		schedule.GeneratedJobs,
		schedule.CreatedAt.UnixMilli(),
		schedule.UpdatedAt.UnixMilli())

	return err
}

func (s *Storage) DeleteSchedule(ctx context.Context, scheduleID string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM ha_schedules WHERE id = ?`, scheduleID)
	return err
}

func (s *Storage) LoadActiveSchedules(ctx context.Context) ([]*domain.JobSchedule, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT 
		id,
		name,
		queue,
		data,
		metadata,
		rule_type,
		rule_expression,
		rule_interval_ms,
		rule_interval_nanos,
		start_at,
		end_at,
		timezone,
		status,
		misfire_policy,
		last_run_at,
		next_run_at,
		priority,
		max_jobs,
		generated_jobs,
		created_at,
		updated_at 
	FROM ha_schedules WHERE status IN (0,1)`)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var result []*domain.JobSchedule
	for rows.Next() {
		sch := &domain.JobSchedule{}
		var meta string
		var startAt, endAt, lastRun, nextRun sql.NullInt64
		var statusInt, misfireInt int
		var createdAtMillis, updatedAtMillis int64
		if err := rows.Scan(&sch.ID, &sch.Name, &sch.Queue, &sch.Data, &meta,
			&sch.Rule.Type, &sch.Rule.Expression, &sch.Rule.IntervalMs, &sch.Rule.IntervalNanos,
			&startAt, &endAt, &sch.Rule.Timezone,
			&statusInt, &misfireInt,
			&lastRun, &nextRun,
			&sch.Priority, &sch.MaxJobs, &sch.GeneratedJobs,
			&createdAtMillis, &updatedAtMillis); err != nil {
			return nil, err
		}
		sch.Metadata = unmarshalMap(meta)
		sch.Status = domain.ScheduleStatus(statusInt)
		sch.MisfirePolicy = domain.MisfirePolicy(misfireInt)
		sch.CreatedAt = time.UnixMilli(createdAtMillis)
		sch.UpdatedAt = time.UnixMilli(updatedAtMillis)
		if startAt.Valid {
			t := time.UnixMilli(startAt.Int64)
			sch.Rule.StartAt = &t
		}
		if endAt.Valid {
			t := time.UnixMilli(endAt.Int64)
			sch.Rule.EndAt = &t
		}
		if lastRun.Valid {
			t := time.UnixMilli(lastRun.Int64)
			sch.LastRunAt = &t
		}
		if nextRun.Valid {
			t := time.UnixMilli(nextRun.Int64)
			sch.NextRunAt = &t
		}
		result = append(result, sch)
	}

	return result, rows.Err()
}

func (s *Storage) LoadSchedule(ctx context.Context, scheduleID string) (*domain.JobSchedule, error) {
	sch := &domain.JobSchedule{}
	var meta string
	var startAt, endAt, lastRun, nextRun sql.NullInt64
	var statusInt, misfireInt int
	var createdAtMillis, updatedAtMillis int64

	row := s.db.QueryRowContext(ctx,
		`SELECT 
			id,
			name,
			queue,
			data,
			metadata,
			rule_type,
			rule_expression,
			rule_interval_ms,
			rule_interval_nanos,
			start_at,
			end_at,
			timezone,
			status,
			misfire_policy,
			last_run_at,
			next_run_at,
			priority,
			max_jobs,
			generated_jobs,
			created_at,
			updated_at 
		FROM ha_schedules WHERE id = ?`, scheduleID,
	)

	err := row.Scan(
		&sch.ID, &sch.Name, &sch.Queue, &sch.Data, &meta,
		&sch.Rule.Type, &sch.Rule.Expression, &sch.Rule.IntervalMs, &sch.Rule.IntervalNanos,
		&startAt, &endAt, &sch.Rule.Timezone,
		&statusInt, &misfireInt,
		&lastRun, &nextRun,
		&sch.Priority, &sch.MaxJobs, &sch.GeneratedJobs,
		&createdAtMillis, &updatedAtMillis)
	if err != nil {
		return nil, fmt.Errorf("load schedule: %w", err)
	}

	sch.Metadata = unmarshalMap(meta)
	sch.Status = domain.ScheduleStatus(statusInt)
	sch.MisfirePolicy = domain.MisfirePolicy(misfireInt)
	sch.CreatedAt = time.UnixMilli(createdAtMillis)
	sch.UpdatedAt = time.UnixMilli(updatedAtMillis)

	if startAt.Valid {
		t := time.UnixMilli(startAt.Int64)
		sch.Rule.StartAt = &t
	}

	if endAt.Valid {
		t := time.UnixMilli(endAt.Int64)
		sch.Rule.EndAt = &t
	}

	if lastRun.Valid {
		t := time.UnixMilli(lastRun.Int64)
		sch.LastRunAt = &t
	}

	if nextRun.Valid {
		t := time.UnixMilli(nextRun.Int64)
		sch.NextRunAt = &t
	}

	return sch, nil
}

func (s *Storage) UpdateScheduleStatus(ctx context.Context, scheduleID string, status domain.ScheduleStatus) error {
	_, err := s.db.ExecContext(ctx, `UPDATE ha_schedules SET status = ?,updated_at = ? WHERE id = ?`, int(status), time.Now().UnixMilli(), scheduleID)
	return err
}

func (s *Storage) IncrementGeneratedJobs(ctx context.Context, scheduleID string) (int, error) {
	var gen int
	err := s.db.QueryRowContext(ctx, `UPDATE ha_schedules SET generated_jobs = generated_jobs + 1, updated_at = ? WHERE id = ? RETURNING generated_jobs`, time.Now().UnixMilli(), scheduleID).Scan(&gen)
	if err != nil {
		return 0, err
	}

	return gen, nil
}

func (s *Storage) UpdateNextRunAt(ctx context.Context, scheduleID string, nextRun time.Time, lastRun time.Time) error {
	_, err := s.db.ExecContext(ctx, `UPDATE ha_schedules SET last_run_at = ?, next_run_at = ?, updated_at = ? WHERE id = ?`, lastRun.UnixMilli(), nextRun.UnixMilli(), time.Now().UnixMilli(), scheduleID)
	return err
}

// ──────────────────────────────────────────────
//  DLQ operations
// ──────────────────────────────────────────────

func (s *Storage) ListDLQ(ctx context.Context, queue domain.QueueName) ([]storage.DeadLetter, error) {
	var rows *sql.Rows
	var err error
	if queue == "" {
		rows, err = s.db.QueryContext(ctx, `SELECT id, queue, payload, error, failed_at, attempts FROM ha_dead_letters ORDER BY failed_at DESC`)
	} else {
		rows, err = s.db.QueryContext(ctx, `SELECT id, queue, payload, error, failed_at, attempts FROM ha_dead_letters WHERE queue = ? ORDER BY failed_at DESC`, queue)
	}

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var result []storage.DeadLetter
	for rows.Next() {
		var dl storage.DeadLetter
		var failedAt int64
		if err := rows.Scan(&dl.ID, &dl.Queue, &dl.Payload, &dl.Error, &failedAt, &dl.Attempts); err != nil {
			return nil, err
		}
		dl.FailedAt = time.UnixMilli(failedAt)
		result = append(result, dl)
	}

	return result, rows.Err()
}

func (s *Storage) RequeueDLQ(ctx context.Context, jobID string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	defer tx.Rollback()

	var dl storage.DeadLetter
	var failedAt int64
	err = tx.QueryRowContext(ctx, `SELECT id, queue, payload, error, failed_at, attempts FROM ha_dead_letters WHERE id = ?`, jobID).Scan(&dl.ID, &dl.Queue, &dl.Payload, &dl.Error, &failedAt, &dl.Attempts)
	if err != nil {
		return fmt.Errorf("dead letter not found: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM ha_dead_letters WHERE id=?`, jobID); err != nil {
		return err
	}

	now := time.Now()
	// Job may still exist in ha_jobs table (as dead_letter status).
	// Update it back to pending.
	if _, err := tx.ExecContext(ctx, `UPDATE ha_jobs SET status = 'pending',attempts = 0, run_at = ?, updated_at = ? WHERE id = ?`,
		now.UnixMilli(), now.UnixMilli(), dl.ID); err != nil {
		return err
	}

	return tx.Commit()
}

// ──────────────────────────────────────────────
//  Stats & Close
// ──────────────────────────────────────────────

func (s *Storage) NextScheduledAt(ctx context.Context) (time.Time, error) {
	var runAt int64
	err := s.db.QueryRowContext(ctx, `SELECT run_at FROM ha_jobs WHERE status = 'scheduled' ORDER BY run_at LIMIT 1`).Scan(&runAt)
	if err == sql.ErrNoRows {
		return time.Time{}, nil
	}

	if err != nil {
		return time.Time{}, err
	}

	return time.UnixMilli(runAt), nil
}

func (s *Storage) Stats(ctx context.Context) (storage.Stats, error) {
	var st storage.Stats
	st.ReadyCount = make(map[domain.QueueName]int64)

	rows, err := s.db.QueryContext(ctx, `SELECT queue, COUNT(*) FROM ha_jobs WHERE status = 'pending' AND run_at <= ? GROUP BY queue`, time.Now().UnixMilli())
	if err != nil {
		return st, err
	}

	defer rows.Close()

	for rows.Next() {
		var q domain.QueueName
		var c int64
		if err := rows.Scan(&q, &c); err != nil {
			return st, err
		}
		st.ReadyCount[q] = c
	}

	_ = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM ha_jobs WHERE status='scheduled'`).Scan(&st.ScheduledCount)
	_ = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM ha_dead_letters`).Scan(&st.DLQCount)
	_ = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM ha_leases`).Scan(&st.LeaseCount)

	return st, nil
}

func (s *Storage) Close() error {
	return s.db.Close()
}

// ──────────────────────────────────────────────
//  Helpers
// ──────────────────────────────────────────────

func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}

	return false
}

func marshalMap(m map[string]string) string {
	b, _ := json.Marshal(m)
	return string(b)
}

func unmarshalMap(s string) map[string]string {
	m := make(map[string]string)
	json.Unmarshal([]byte(s), &m)

	return m
}

func nilStr(s *string) interface{} {
	if s == nil {
		return nil
	}

	return *s
}

func optMillis(t *time.Time) interface{} {
	if t == nil {
		return nil
	}

	return t.UnixMilli()
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
