package host

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lemenendez/deltaflow/pkg/connectors"
	pgstore "github.com/lemenendez/deltaflow/pkg/connectors/postgres"
	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
)

type OpenStoresOptions struct {
	MaxAttempts int
	LeaseLogger *slog.Logger
}

type FileLogger struct {
	Logger *slog.Logger
	Path   string
	file   *os.File
}

type WorkerLoopStats struct {
	RunOnceCalls int64
	Errors       int64
}

type RunTimings struct {
	Setup   time.Duration
	Enqueue time.Duration
	Drain   time.Duration
	Total   time.Duration
}

type JobCounts struct {
	Pending    int
	Processing int
	Synced     int
	Retrying   int
	Dead       int
	Ghosts     int
}

func DefaultDSN() string {
	return "postgres://deltaflow:deltaflow@postgres:5432/deltaflow?sslmode=disable"
}

func EnvInt(name string, defaultValue int) (int, error) {
	value := os.Getenv(name)
	if value == "" {
		return defaultValue, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", name, err)
	}
	if parsed <= 0 {
		return 0, fmt.Errorf("%s must be greater than zero", name)
	}
	return parsed, nil
}

func EnvUint64(name string, defaultValue uint64) (uint64, error) {
	value := os.Getenv(name)
	if value == "" {
		return defaultValue, nil
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be an unsigned integer: %w", name, err)
	}
	if parsed == 0 {
		return 0, fmt.Errorf("%s must be greater than zero", name)
	}
	return parsed, nil
}

func OpenStores(ctx context.Context, dsn string, maxAttempts int) (*sql.DB, *pgstore.DeltaStore, *pgstore.JobStore, *pgstore.DispatchStore, error) {
	return OpenStoresWithOptions(ctx, dsn, OpenStoresOptions{MaxAttempts: maxAttempts})
}

// OpenStoresWithOptions wires official DeltaFlow Postgres connectors for playground usage.
// It does not re-implement store behavior; it only opens DB and composes DeltaStore, JobStore, and DispatchStore.
func OpenStoresWithOptions(ctx context.Context, dsn string, opts OpenStoresOptions) (*sql.DB, *pgstore.DeltaStore, *pgstore.JobStore, *pgstore.DispatchStore, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, nil, nil, nil, err
	}

	deltaStore := pgstore.NewDeltaStore(db, connectors.DeltaStoreConfig{})
	jobStore := pgstore.NewJobStore(db, pgstore.JobStoreConfig{
		MaxAttempts: opts.MaxAttempts,
		LeaseLogger: opts.LeaseLogger,
	})
	dispatchStore := pgstore.NewDispatchStore(deltaStore, jobStore, pgstore.DispatchStoreConfig{})

	return db, deltaStore, jobStore, dispatchStore, nil
}

func OpenFileLogger(path string) (*FileLogger, error) {
	if path == "" {
		path = "logs/deltaflow-worker.log"
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}

	logger := slog.New(slog.NewJSONHandler(file, &slog.HandlerOptions{Level: slog.LevelDebug}))
	return &FileLogger{Logger: logger, Path: path, file: file}, nil
}

func (l *FileLogger) Close() error {
	if l == nil || l.file == nil {
		return nil
	}
	return l.file.Close()
}

func ResetSync(ctx context.Context, db *sql.DB, syncID deltaflow.SyncID) error {
	if _, err := db.ExecContext(ctx, `DELETE FROM deltaflow.deltaflow_sync_jobs WHERE sync_id = $1`, syncID); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM deltaflow.deltaflow_deltas WHERE sync_id = $1`, syncID); err != nil {
		return err
	}
	return nil
}

func RunWorkers(
	ctx context.Context,
	workerCount int,
	makeWorker func(workerID string) *deltaflow.SyncWorker,
	shouldStop func(context.Context) (bool, error),
	afterRunOnce func(context.Context) error,
) (WorkerLoopStats, error) {
	var stats WorkerLoopStats
	errCh := make(chan error, workerCount)
	done := make(chan struct{})
	var once sync.Once
	stop := func() { once.Do(func() { close(done) }) }

	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		workerID := fmt.Sprintf("deltaflow-worker-%d", i+1)
		worker := makeWorker(workerID)

		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					errCh <- ctx.Err()
					stop()
					return
				case <-done:
					return
				default:
				}

				if err := worker.RunOnce(ctx); err != nil {
					atomic.AddInt64(&stats.Errors, 1)
					errCh <- err
					stop()
					return
				}
				atomic.AddInt64(&stats.RunOnceCalls, 1)

				if afterRunOnce != nil {
					if err := afterRunOnce(ctx); err != nil {
						errCh <- err
						stop()
						return
					}
				}

				ok, err := shouldStop(ctx)
				if err != nil {
					errCh <- err
					stop()
					return
				}
				if ok {
					stop()
					return
				}

				timer := time.NewTimer(5 * time.Millisecond)
				select {
				case <-ctx.Done():
					timer.Stop()
					errCh <- ctx.Err()
					stop()
					return
				case <-done:
					timer.Stop()
					return
				case <-timer.C:
				}
			}
		}()
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return stats, err
		}
	}
	return stats, nil
}

func MakeRetryingAvailable(ctx context.Context, db *sql.DB, syncID deltaflow.SyncID) error {
	_, err := db.ExecContext(ctx, `
UPDATE deltaflow.deltaflow_sync_jobs
SET available_at = NOW(), updated_at = NOW()
WHERE sync_id = $1 AND state = 'retrying' AND available_at > NOW()`, syncID)
	return err
}

func CountJobs(ctx context.Context, db *sql.DB, syncID deltaflow.SyncID) (JobCounts, error) {
	rows, err := db.QueryContext(ctx, `
SELECT state, COUNT(*), COUNT(*) FILTER (WHERE ghost_detected)
FROM deltaflow.deltaflow_sync_jobs
WHERE sync_id = $1
GROUP BY state
ORDER BY state`, syncID)
	if err != nil {
		return JobCounts{}, err
	}
	defer rows.Close()

	var counts JobCounts
	for rows.Next() {
		var state string
		var n int
		var ghosts int
		if err := rows.Scan(&state, &n, &ghosts); err != nil {
			return JobCounts{}, err
		}
		counts.Ghosts += ghosts
		switch deltaflow.SyncJobState(state) {
		case deltaflow.StatePending:
			counts.Pending = n
		case deltaflow.StateProcessing:
			counts.Processing = n
		case deltaflow.StateSynced:
			counts.Synced = n
		case deltaflow.StateRetrying:
			counts.Retrying = n
		case deltaflow.StateDead:
			counts.Dead = n
		}
	}
	if err := rows.Err(); err != nil {
		return JobCounts{}, err
	}
	return counts, nil
}

func CountPendingDeltas(ctx context.Context, db *sql.DB, syncID deltaflow.SyncID) (int, error) {
	row := db.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM deltaflow.deltaflow_deltas
WHERE sync_id = $1 AND state = 'pending'`, syncID)
	var count int
	if err := row.Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func WorkComplete(ctx context.Context, db *sql.DB, syncID deltaflow.SyncID) (bool, error) {
	pendingDeltas, err := CountPendingDeltas(ctx, db, syncID)
	if err != nil {
		return false, err
	}
	if pendingDeltas > 0 {
		return false, nil
	}
	counts, err := CountJobs(ctx, db, syncID)
	if err != nil {
		return false, err
	}
	return counts.Pending == 0 && counts.Processing == 0 && counts.Retrying == 0, nil
}

func PerSecond(count int, d time.Duration) float64 {
	if count <= 0 || d <= 0 {
		return 0
	}
	return float64(count) / d.Seconds()
}

func FormatDuration(d time.Duration) string {
	if d < time.Millisecond {
		return d.String()
	}
	return d.Truncate(time.Millisecond).String()
}

func JSONProjection(identity deltaflow.ProjectionIdentity, payload any) (deltaflow.Projection, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return deltaflow.Projection{}, err
	}
	sum := sha256.Sum256(data)
	return deltaflow.Projection{
		Identity:  identity,
		Payload:   data,
		MediaType: "application/json",
		Checksum:  fmt.Sprintf("%x", sum[:]),
	}, nil
}

func StringKey(field string, value string) deltaflow.ProjectionKey {
	raw, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return deltaflow.ProjectionKey{field: raw}
}

func StringFromKey(key deltaflow.ProjectionKey, field string) (string, error) {
	raw, ok := key[field]
	if !ok {
		return "", fmt.Errorf("projection key is missing %s", field)
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", fmt.Errorf("invalid %s in projection key: %w", field, err)
	}
	if value == "" {
		return "", fmt.Errorf("%s cannot be empty", field)
	}
	return value, nil
}

func StableDigest(docs map[string][]byte) string {
	keys := make([]string, 0, len(docs))
	for key := range docs {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	h := sha256.New()
	for _, key := range keys {
		h.Write([]byte(key))
		h.Write([]byte{0})
		h.Write(docs[key])
		h.Write([]byte{0})
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

func FirstSample(docs map[string][]byte) (string, []byte, bool) {
	if len(docs) == 0 {
		return "", nil, false
	}
	keys := make([]string, 0, len(docs))
	for key := range docs {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	key := keys[0]
	return key, append([]byte(nil), docs[key]...), true
}

func NewDelta(syncID deltaflow.SyncID, projectionType deltaflow.ProjectionType, key deltaflow.ProjectionKey, origin deltaflow.OriginOperationType, occurredAt time.Time, metadata map[string]any) deltaflow.Delta {
	return deltaflow.Delta{
		SyncID:         syncID,
		Origin:         origin,
		ProjectionType: projectionType,
		ProjectionKey:  key,
		State:          deltaflow.DeltaPending,
		OccurredAt:     occurredAt,
		Metadata:       metadata,
	}
}
