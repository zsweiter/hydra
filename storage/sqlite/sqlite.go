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
	"runtime"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/zsweiter/hydra/domain"
	"github.com/zsweiter/hydra/storage"
)

// Storage implements storage.Storage backed by SQLite.
type Storage struct {
	// db is the single writer connection: SQLite serialises
	// writes anyway, and one connection avoids busy retries.
	db *sql.DB

	// reads is a pool of read-only connections. In WAL mode
	// readers do not wait for the writer, so window loads and
	// payload lookups run while claims are written. It is db
	// itself for an in-memory or caller-provided database.
	reads *sql.DB
}

var _ storage.Storage = (*Storage)(nil)

// Use wraps an open database and initialises the schema. Reads
// and writes share it.
func Use(db *sql.DB) (*Storage, error) {
	db.SetMaxOpenConns(1) // SQLite serialises writes anyway

	s := &Storage{db: db, reads: db}

	// run migration on startup
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, fmt.Errorf("sqlite migrate: %w", err)
	}

	return s, nil
}

// New opens a SQLite database file and initialises the schema.
//
// It runs in WAL mode with synchronous=FULL: every commit is on
// disk before it returns, so neither a process crash nor a
// power loss loses an acknowledged write. One connection
// writes; a pool of read-only connections reads concurrently.
func New(path string) (*Storage, error) {
	connStr := path
	if connStr == "" {
		connStr = "file:hydra.db"
	}
	sep := "?"
	if contains(connStr, "?") {
		sep = "&"
	}
	params := "_journal_mode=WAL&_synchronous=FULL&_busy_timeout=5000"

	db, err := sql.Open("sqlite3", connStr+sep+params+"&_txlock=immediate")
	if err != nil {
		return nil, fmt.Errorf("sqlite open: %w", err)
	}
	s, err := Use(db)
	if err != nil {
		return nil, err
	}

	// An in-memory database is private to its connection: a
	// second pool would see another, empty one.
	if strings.Contains(connStr, "mode=memory") || strings.Contains(connStr, ":memory:") {
		return s, nil
	}

	reads, err := sql.Open("sqlite3", connStr+sep+params+"&mode=ro")
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("sqlite open: %w", err)
	}

	reads.SetMaxOpenConns(max(runtime.GOMAXPROCS(0), 4))
	s.reads = reads
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
			rule_offset_nanos INTEGER DEFAULT 0,
			status INTEGER NOT NULL DEFAULT 0,
			misfire_policy INTEGER NOT NULL DEFAULT 0,
			last_run_at INTEGER,
			next_run_at INTEGER,
			priority INTEGER DEFAULT 0,
			max_jobs INTEGER DEFAULT 0,
			generated_jobs INTEGER DEFAULT 0,
			version INTEGER NOT NULL DEFAULT 0,
			shard INTEGER NOT NULL DEFAULT 0,
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL
		);

		CREATE TABLE IF NOT EXISTS ha_paused_queues (
			queue TEXT PRIMARY KEY
		);

		CREATE TABLE IF NOT EXISTS ha_instances (
			id TEXT PRIMARY KEY,
			expires_at INTEGER NOT NULL
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
	if err != nil {
		return err
	}

	if _, err := s.addColumnIfMissing("ha_schedules", "rule_offset_nanos", "INTEGER DEFAULT 0"); err != nil {
		return err
	}
	if _, err := s.addColumnIfMissing("ha_schedules", "version", "INTEGER NOT NULL DEFAULT 0"); err != nil {
		return err
	}
	if added, err := s.addColumnIfMissing("ha_schedules", "shard", "INTEGER NOT NULL DEFAULT 0"); err != nil {
		return err
	} else if added {
		if err := s.backfillShards(); err != nil {
			return err
		}
	}

	// Finished and cancelled schedules and completed jobs stay
	// as history; these indexes keep the hot paths (active
	// schedules, a schedule's jobs, expired leases) from
	// scanning it.
	//
	// idx_jobs_claim matches ClaimJobs' ORDER BY run_at,
	// priority, so claiming never sorts jobs due at the same
	// time; it replaces idx_jobs_queue_status.
	_, err = s.db.ExecContext(context.Background(), `
		DROP INDEX IF EXISTS idx_jobs_queue_status;
		CREATE INDEX IF NOT EXISTS idx_jobs_claim ON ha_jobs(queue, status, run_at, priority);
		CREATE INDEX IF NOT EXISTS idx_jobs_status_updated ON ha_jobs(status, updated_at);
		CREATE INDEX IF NOT EXISTS idx_schedules_status ON ha_schedules(status, updated_at);
		CREATE INDEX IF NOT EXISTS idx_schedules_due ON ha_schedules(status, next_run_at, shard);
		CREATE INDEX IF NOT EXISTS idx_jobs_schedule ON ha_jobs(schedule_id, updated_at);
		CREATE INDEX IF NOT EXISTS idx_leases_expires ON ha_leases(expires_at);
	`)
	return err
}

// addColumnIfMissing upgrades tables created by older
// versions, which CREATE TABLE IF NOT EXISTS leaves as is.
func (s *Storage) addColumnIfMissing(table, column, definition string) (bool, error) {
	var count int
	err := s.db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM pragma_table_info(?) WHERE name = ?`, table, column,
	).Scan(&count)
	if err != nil || count > 0 {
		return false, err
	}

	_, err = s.db.ExecContext(context.Background(),
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN %s %s`, table, column, definition))
	return err == nil, err
}

// backfillShards sets the shard of schedules stored before the
// column existed; it is computed in Go, not in SQL.
func (s *Storage) backfillShards() error {
	ctx := context.Background()
	rows, err := s.db.QueryContext(ctx, `SELECT id FROM ha_schedules`)
	if err != nil {
		return err
	}

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}

	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	defer tx.Rollback()
	for _, id := range ids {
		if _, err := tx.ExecContext(ctx, `UPDATE ha_schedules SET shard = ? WHERE id = ?`, domain.ScheduleShard(id), id); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (s *Storage) SaveJob(ctx context.Context, job *domain.Job) error {
	return saveJob(ctx, s.db, job)
}

// execer is satisfied by *sql.DB and *sql.Tx.
type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func saveJob(ctx context.Context, db execer, job *domain.Job) error {
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

	_, err := db.ExecContext(
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

const jobColumns = `id, name, queue, data, metadata, run_at, priority, status,
	attempts, max_attempts, backoff_type, backoff_base_ms, backoff_max_ms,
	schedule_id, idempotency_key, created_at, updated_at`

func scanJob(row scanner) (*domain.Job, error) {
	job := &domain.Job{}
	var meta, statusStr string
	var runAt, createdAt, updatedAt int64
	var schedID sql.NullString
	var backoffType int

	if err := row.Scan(
		&job.ID, &job.Name, &job.Queue, &job.Data, &meta, &runAt, &job.Priority, &statusStr,
		&job.Attempts, &job.MaxAttempts, &backoffType, &job.Backoff.BaseMs, &job.Backoff.MaxMs,
		&schedID, &job.IdempotencyKey, &createdAt, &updatedAt,
	); err != nil {
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
	return job, nil
}

func (s *Storage) ListJobs(ctx context.Context, q storage.JobQuery) ([]*domain.Job, error) {
	before := q.Before
	if before.IsZero() {
		before = time.Now().Add(time.Millisecond)
	}

	where := `WHERE schedule_id = ? AND updated_at < ?`
	args := []any{q.ScheduleID, before.UnixMilli()}
	if len(q.Statuses) > 0 {
		where += ` AND status IN (` + placeholders(len(q.Statuses)) + `)`
		for _, st := range q.Statuses {
			args = append(args, jobStatusText(st))
		}
	}
	args = append(args, limitOrAll(q.Limit))

	rows, err := s.reads.QueryContext(ctx, `SELECT `+jobColumns+` FROM ha_jobs `+where+` ORDER BY updated_at DESC LIMIT ?`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []*domain.Job
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, job)
	}

	return result, rows.Err()
}

func (s *Storage) ClaimJobs(ctx context.Context, queue domain.QueueName, workerID string, n int, ttl time.Duration) ([]storage.LeasedJob, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `SELECT `+jobColumns+` FROM ha_jobs
		WHERE queue = ? AND status = 'pending' ORDER BY run_at, priority LIMIT ?`, queue, n)
	if err != nil {
		return nil, err
	}

	var jobs []*domain.Job
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		jobs = append(jobs, job)
	}
	rows.Close()

	if len(jobs) == 0 {
		return nil, nil
	}

	// Mark as processing and insert leases, one statement each.
	now := time.Now()
	expires := now.Add(ttl).UnixMilli()
	ids := make([]any, len(jobs))
	leases := make([]any, 0, 3*len(jobs))
	result := make([]storage.LeasedJob, len(jobs))
	for i, job := range jobs {
		job.Status = domain.StatusProcessing
		job.UpdatedAt = now
		ids[i] = job.ID
		leases = append(leases, job.ID, workerID, expires)
		result[i] = storage.LeasedJob{Job: job}
	}

	if _, err := tx.ExecContext(ctx, `UPDATE ha_jobs SET status = 'processing', updated_at = ? WHERE id IN (`+placeholders(len(ids))+`)`,
		append([]any{now.UnixMilli()}, ids...)...); err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT OR REPLACE INTO ha_leases(job_id, worker_id, expires_at) VALUES `+
		strings.TrimSuffix(strings.Repeat("(?,?,?),", len(jobs)), ","), leases...); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return result, nil
}

func (s *Storage) AckJobs(ctx context.Context, jobIDs []string) error {
	if len(jobIDs) == 0 {
		return nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, chunk := range chunks(jobIDs, maxParams-1) {
		ids := anySlice(chunk)
		if _, err := tx.ExecContext(ctx, `DELETE FROM ha_leases WHERE job_id IN (`+placeholders(len(ids))+`)`, ids...); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE ha_jobs SET status = 'completed', updated_at = ? WHERE id IN (`+placeholders(len(ids))+`)`,
			append([]any{time.Now().UnixMilli()}, ids...)...); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (s *Storage) ExtendLease(ctx context.Context, jobID string, ttl time.Duration) error {
	res, err := s.db.ExecContext(ctx, `UPDATE ha_leases SET expires_at = ? WHERE job_id = ?`,
		time.Now().Add(ttl).UnixMilli(), jobID)
	if err != nil {
		return err
	}

	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("lease not found: %s", jobID)
	}

	return nil
}

func (s *Storage) FailJob(ctx context.Context, job *domain.Job, failErr error) (storage.FailResult, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return storage.FailResult{}, err
	}

	defer tx.Rollback()

	job.Attempts++
	job.UpdatedAt = time.Now()

	if job.Attempts < job.MaxAttempts {
		delay := job.Backoff.NextDelay(job.Attempts)
		nextRun := time.Now().Add(delay)
		job.RunAt = nextRun
		job.Status = domain.StatusPending

		_, err = tx.ExecContext(ctx, `DELETE FROM ha_leases WHERE job_id = ?`, job.ID)
		if err != nil {
			return storage.FailResult{}, err
		}

		query := `UPDATE ha_jobs SET status = ?, attempts = ?, run_at = ?,updated_at = ? WHERE id = ?`
		_, err = tx.ExecContext(ctx, query, "pending", job.Attempts, nextRun.UnixMilli(), job.UpdatedAt.UnixMilli(), job.ID)
		if err != nil {
			return storage.FailResult{}, err
		}

		return storage.FailResult{Action: storage.FailActionRetry, NextRunAt: nextRun}, tx.Commit()
	}

	job.Status = domain.StatusDeadLetter
	_, err = tx.ExecContext(ctx, `DELETE FROM ha_leases WHERE job_id = ?`, job.ID)
	if err != nil {
		return storage.FailResult{}, err
	}

	_, err = tx.ExecContext(ctx, `UPDATE ha_jobs SET status = ?, attempts = ?, updated_at = ? WHERE id = ?`,
		"dead_letter", job.Attempts, job.UpdatedAt.UnixMilli(), job.ID)
	if err != nil {
		return storage.FailResult{}, err
	}

	_, err = tx.ExecContext(ctx, `INSERT INTO ha_dead_letters(id, queue, payload, error, failed_at, attempts) VALUES(?,?,?,?,?,?)`,
		job.ID, job.Queue, job.Data, failErr.Error(), time.Now().UnixMilli(), job.Attempts)
	if err != nil {
		return storage.FailResult{}, err
	}

	return storage.FailResult{Action: storage.FailActionDeadLetter}, tx.Commit()
}

// ──────────────────────────────────────────────
//  Queue operations
// ──────────────────────────────────────────────

func (s *Storage) ReadyQueues(ctx context.Context) ([]domain.QueueName, error) {
	rows, err := s.reads.QueryContext(ctx, `SELECT DISTINCT queue FROM ha_jobs WHERE status = 'pending' AND run_at <= ?`, time.Now().UnixMilli())
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

const scheduleColumns = `id, name, queue, data, metadata,
	rule_type, rule_expression, rule_interval_ms, rule_interval_nanos,
	start_at, end_at, timezone, rule_offset_nanos,
	status, misfire_policy, last_run_at, next_run_at,
	priority, max_jobs, generated_jobs, version, created_at, updated_at`

// lightColumns are scheduleColumns without the payload (data,
// metadata), for the scheduler's window loads.
const lightColumns = `id, name, queue, NULL, NULL,
	rule_type, rule_expression, rule_interval_ms, rule_interval_nanos,
	start_at, end_at, timezone, rule_offset_nanos,
	status, misfire_policy, last_run_at, next_run_at,
	priority, max_jobs, generated_jobs, version, created_at, updated_at`

// scanner is satisfied by *sql.Row and *sql.Rows.
type scanner interface {
	Scan(dest ...any) error
}

func scanSchedule(row scanner) (*domain.JobSchedule, error) {
	sch := &domain.JobSchedule{}
	var meta sql.NullString
	var startAt, endAt, lastRun, nextRun sql.NullInt64
	var offsetNanos int64
	var statusInt, misfireInt int
	var createdAtMillis, updatedAtMillis int64

	if err := row.Scan(
		&sch.ID, &sch.Name, &sch.Queue, &sch.Data, &meta,
		&sch.Rule.Type, &sch.Rule.Expression, &sch.Rule.IntervalMs, &sch.Rule.IntervalNanos,
		&startAt, &endAt, &sch.Rule.Timezone, &offsetNanos,
		&statusInt, &misfireInt, &lastRun, &nextRun,
		&sch.Priority, &sch.MaxJobs, &sch.GeneratedJobs, &sch.Version,
		&createdAtMillis, &updatedAtMillis,
	); err != nil {
		return nil, err
	}

	if meta.Valid {
		sch.Metadata = unmarshalMap(meta.String)
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

func (s *Storage) querySchedules(ctx context.Context, where string, args ...any) ([]*domain.JobSchedule, error) {
	return s.selectSchedules(ctx, scheduleColumns, where, args...)
}

func (s *Storage) selectSchedules(ctx context.Context, columns, where string, args ...any) ([]*domain.JobSchedule, error) {
	rows, err := s.reads.QueryContext(ctx, `SELECT `+columns+` FROM ha_schedules `+where, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []*domain.JobSchedule
	for rows.Next() {
		sch, err := scanSchedule(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, sch)
	}
	return result, rows.Err()
}

func (s *Storage) SaveSchedule(ctx context.Context, schedule *domain.JobSchedule) error {
	return storage.SaveOne(ctx, s, schedule)
}

// upsertSchedule writes a schedule unless the stored one has a
// version at least as new: the WHERE of the upsert makes that
// a no-op, detected through the affected row count.
const upsertSchedule = `INSERT INTO ha_schedules(` + scheduleColumns + `, shard)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
			rule_offset_nanos = excluded.rule_offset_nanos,
			status = excluded.status,
			misfire_policy = excluded.misfire_policy,
			last_run_at = excluded.last_run_at,
			next_run_at = excluded.next_run_at,
			priority = excluded.priority,
			max_jobs = excluded.max_jobs,
			generated_jobs = excluded.generated_jobs,
			version = excluded.version,
			updated_at = excluded.updated_at
		WHERE excluded.version = 0 OR excluded.version > ha_schedules.version`

// SaveSchedules writes the batch in one transaction: one
// commit, one sync to disk.
func (s *Storage) SaveSchedules(ctx context.Context, schedules []*domain.JobSchedule) ([]error, error) {
	errs := make([]error, len(schedules))
	if len(schedules) == 0 {
		return errs, nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, upsertSchedule)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	now := time.Now().UnixMilli()
	for i, schedule := range schedules {
		if schedule.CreatedAt.IsZero() {
			schedule.CreatedAt = time.UnixMilli(now)
		}
		schedule.UpdatedAt = time.UnixMilli(now)

		res, err := stmt.ExecContext(ctx,
			schedule.ID,
			schedule.Name,
			schedule.Queue,
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
			int(schedule.Status),
			int(schedule.MisfirePolicy),
			optMillis(schedule.LastRunAt),
			optMillis(schedule.NextRunAt),
			schedule.Priority,
			schedule.MaxJobs,
			schedule.GeneratedJobs,
			schedule.Version,
			schedule.CreatedAt.UnixMilli(),
			schedule.UpdatedAt.UnixMilli(),
			domain.ScheduleShard(schedule.ID),
		)
		if err != nil {
			return nil, err
		}
		if n, err := res.RowsAffected(); err != nil {
			return nil, err
		} else if n == 0 {
			errs[i] = storage.ErrStaleVersion
		}
	}

	return errs, tx.Commit()
}

// ClaimOccurrences applies the claims in one transaction, so
// a batch costs one commit (one fsync) instead of one each.
func (s *Storage) ClaimOccurrences(ctx context.Context, claims []storage.Claim) ([]bool, error) {
	result := make([]bool, len(claims))
	if len(claims) == 0 {
		return result, nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `
		UPDATE ha_schedules SET
			next_run_at = ?1,
			last_run_at = ?2,
			generated_jobs = generated_jobs + ?3,
			status = CASE
				WHEN ?1 IS NULL OR (max_jobs > 0 AND generated_jobs + ?3 >= max_jobs) THEN ?4
				ELSE status
			END,
			updated_at = CASE
				WHEN ?1 IS NULL OR (max_jobs > 0 AND generated_jobs + ?3 >= max_jobs) THEN ?2
				ELSE updated_at
			END
		WHERE id = ?5 AND status = ?6 AND (next_run_at = ?7 OR next_run_at IS NULL)`)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	now := time.Now().UnixMilli()
	for i, c := range claims {
		var nextMillis any
		if !c.Next.IsZero() {
			nextMillis = c.Next.UnixMilli()
		}

		res, err := stmt.ExecContext(ctx, nextMillis, now, c.Generated, int(domain.ScheduleFinished),
			c.ScheduleID, int(domain.ScheduleActive), c.Expected.UnixMilli())
		if err != nil {
			return nil, err
		}
		if n, err := res.RowsAffected(); err != nil {
			return nil, err
		} else if n == 0 {
			continue
		}

		for _, job := range c.Jobs {
			if err := saveJob(ctx, tx, job); err != nil {
				return nil, err
			}
		}
		result[i] = true
	}

	return result, tx.Commit()
}

func (s *Storage) DeleteSchedule(ctx context.Context, scheduleID string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM ha_schedules WHERE id = ?`, scheduleID)
	return err
}

func (s *Storage) LoadDueSchedules(ctx context.Context, q storage.DueQuery) ([]*domain.JobSchedule, error) {
	where := `WHERE status = ? AND (next_run_at IS NULL OR next_run_at <= ?)`
	args := []any{int(domain.ScheduleActive), q.Before.UnixMilli()}
	if !q.After.IsZero() {
		where += ` AND next_run_at > ?`
		args = append(args, q.After.UnixMilli())
	}
	if !q.ChangedSince.IsZero() {
		where += ` AND updated_at > ?`
		args = append(args, q.ChangedSince.UnixMilli())
	}
	if q.Shards != nil {
		where += ` AND shard IN (` + placeholders(len(q.Shards)) + `)`
		for _, sh := range q.Shards {
			args = append(args, sh)
		}
	}
	// NULLs sort first: an unset NextRunAt is due now.
	where += ` ORDER BY next_run_at, id LIMIT ?`
	args = append(args, limitOrAll(q.Limit))
	return s.selectSchedules(ctx, lightColumns, where, args...)
}

func (s *Storage) LoadSchedules(ctx context.Context, ids []string) ([]*domain.JobSchedule, error) {
	var result []*domain.JobSchedule
	for _, chunk := range chunks(ids, maxParams) {
		part, err := s.querySchedules(ctx, `WHERE id IN (`+placeholders(len(chunk))+`)`, anySlice(chunk)...)
		if err != nil {
			return nil, err
		}
		result = append(result, part...)
	}
	return result, nil
}

func (s *Storage) PurgeHistory(ctx context.Context, before time.Time, limit int) (int, error) {
	res, err := s.db.ExecContext(ctx, `DELETE FROM ha_jobs WHERE rowid IN (
		SELECT rowid FROM ha_jobs WHERE status = 'completed' AND updated_at < ? LIMIT ?)`,
		before.UnixMilli(), limit)
	if err != nil {
		return 0, err
	}
	jobs, _ := res.RowsAffected()

	res, err = s.db.ExecContext(ctx, `DELETE FROM ha_schedules WHERE rowid IN (
		SELECT rowid FROM ha_schedules WHERE status IN (?, ?, ?) AND updated_at < ? LIMIT ?)`,
		int(domain.ScheduleFinished), int(domain.ScheduleCancelled), int(domain.ScheduleArchived),
		before.UnixMilli(), limit)
	if err != nil {
		return 0, err
	}
	schedules, _ := res.RowsAffected()

	return int(jobs + schedules), nil
}

func (s *Storage) Heartbeat(ctx context.Context, instanceID string, ttl time.Duration) ([]string, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	now := time.Now()
	if ttl > 0 {
		_, err = tx.ExecContext(ctx, `INSERT INTO ha_instances(id, expires_at) VALUES(?, ?)
			ON CONFLICT(id) DO UPDATE SET expires_at = excluded.expires_at`, instanceID, now.Add(ttl).UnixMilli())
	} else {
		_, err = tx.ExecContext(ctx, `DELETE FROM ha_instances WHERE id = ?`, instanceID)
	}
	if err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM ha_instances WHERE expires_at <= ?`, now.UnixMilli()); err != nil {
		return nil, err
	}

	rows, err := tx.QueryContext(ctx, `SELECT id FROM ha_instances ORDER BY id`)
	if err != nil {
		return nil, err
	}
	var live []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		live = append(live, id)
	}

	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return live, tx.Commit()
}

func (s *Storage) LoadSchedule(ctx context.Context, scheduleID string) (*domain.JobSchedule, error) {
	row := s.reads.QueryRowContext(ctx, `SELECT `+scheduleColumns+` FROM ha_schedules WHERE id = ?`, scheduleID)
	sch, err := scanSchedule(row)
	if err != nil {
		return nil, fmt.Errorf("load schedule: %w", err)
	}
	return sch, nil
}

func (s *Storage) ListSchedules(ctx context.Context, q storage.ScheduleQuery) ([]*domain.JobSchedule, error) {
	where := `WHERE 1 = 1`
	var args []any

	if len(q.Statuses) > 0 {
		where += ` AND status IN (` + placeholders(len(q.Statuses)) + `)`
		for _, st := range q.Statuses {
			args = append(args, int(st))
		}
	}
	if q.Queue != "" {
		where += ` AND queue = ?`
		args = append(args, q.Queue)
	}

	where += ` ORDER BY updated_at DESC, id LIMIT ? OFFSET ?`
	args = append(args, limitOrAll(q.Limit), q.Offset)

	return s.querySchedules(ctx, where, args...)
}

func (s *Storage) UpdateScheduleStatus(ctx context.Context, scheduleID string, status domain.ScheduleStatus) error {
	_, err := s.db.ExecContext(ctx, `UPDATE ha_schedules SET status = ?,updated_at = ? WHERE id = ?`, int(status), time.Now().UnixMilli(), scheduleID)
	return err
}

// ──────────────────────────────────────────────
//  Queue pause
// ──────────────────────────────────────────────

func (s *Storage) SetQueuePaused(ctx context.Context, queue domain.QueueName, paused bool) error {
	var err error
	if paused {
		_, err = s.db.ExecContext(ctx, `INSERT OR IGNORE INTO ha_paused_queues(queue) VALUES(?)`, queue)
	} else {
		_, err = s.db.ExecContext(ctx, `DELETE FROM ha_paused_queues WHERE queue = ?`, queue)
	}
	return err
}

func (s *Storage) PausedQueues(ctx context.Context) ([]domain.QueueName, error) {
	rows, err := s.reads.QueryContext(ctx, `SELECT queue FROM ha_paused_queues`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []domain.QueueName
	for rows.Next() {
		var q domain.QueueName
		if err := rows.Scan(&q); err != nil {
			return nil, err
		}
		result = append(result, q)
	}

	return result, rows.Err()
}

// ──────────────────────────────────────────────
//  DLQ operations
// ──────────────────────────────────────────────

func (s *Storage) ListDLQ(ctx context.Context, queue domain.QueueName) ([]storage.DeadLetter, error) {
	var rows *sql.Rows
	var err error
	if queue == "" {
		rows, err = s.reads.QueryContext(ctx, `SELECT id, queue, payload, error, failed_at, attempts FROM ha_dead_letters ORDER BY failed_at DESC`)
	} else {
		rows, err = s.reads.QueryContext(ctx, `SELECT id, queue, payload, error, failed_at, attempts FROM ha_dead_letters WHERE queue = ? ORDER BY failed_at DESC`, queue)
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
	err := s.reads.QueryRowContext(ctx, `SELECT run_at FROM ha_jobs WHERE status = 'scheduled' ORDER BY run_at LIMIT 1`).Scan(&runAt)
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

	rows, err := s.reads.QueryContext(ctx, `SELECT queue, COUNT(*) FROM ha_jobs WHERE status = 'pending' AND run_at <= ? GROUP BY queue`, time.Now().UnixMilli())
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

	_ = s.reads.QueryRowContext(ctx, `SELECT COUNT(*) FROM ha_jobs WHERE status='scheduled'`).Scan(&st.ScheduledCount)
	_ = s.reads.QueryRowContext(ctx, `SELECT COUNT(*) FROM ha_dead_letters`).Scan(&st.DLQCount)
	_ = s.reads.QueryRowContext(ctx, `SELECT COUNT(*) FROM ha_leases`).Scan(&st.LeaseCount)

	return st, nil
}

func (s *Storage) Close() error {
	if s.reads != s.db {
		s.reads.Close()
	}
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

func optTime(v sql.NullInt64) *time.Time {
	if !v.Valid {
		return nil
	}
	t := time.UnixMilli(v.Int64)
	return &t
}

// maxParams keeps IN lists under SQLite's bound parameter
// limit.
const maxParams = 500

func chunks[T any](items []T, size int) [][]T {
	var out [][]T
	for len(items) > size {
		out = append(out, items[:size])
		items = items[size:]
	}
	if len(items) > 0 {
		out = append(out, items)
	}
	return out
}

func anySlice(ids []string) []any {
	out := make([]any, len(ids))
	for i, id := range ids {
		out[i] = id
	}
	return out
}

func placeholders(n int) string {
	return strings.TrimSuffix(strings.Repeat("?,", n), ",")
}

// limitOrAll maps a non-positive limit to SQLite's "no limit".
func limitOrAll(limit int) int {
	if limit <= 0 {
		return -1
	}
	return limit
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
