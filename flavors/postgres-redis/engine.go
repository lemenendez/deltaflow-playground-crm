package main

import (
	"context"
	"os"
	"sync/atomic"
	"time"

	hostpkg "github.com/lemenendez/deltaflow-playground-crm/internal/scenario/host"
	pgstore "github.com/lemenendez/deltaflow/pkg/connectors/postgres"
	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
)

type demoResult struct {
	Scenario        *scenario
	Enqueued        int
	WorkerStats     hostpkg.WorkerLoopStats
	JobCounts       hostpkg.JobCounts
	ProjectorGhosts int64
	SearchQueue     []string
	OrderQueue      []string
	SearchDocs      map[string][]byte
	TargetUpserts   int
	TargetDeletes   int
	TargetFailures  int
	Timings         hostpkg.RunTimings
	WorkerLogPath   string
}

type workerFactory func(workerID string, jobStore *pgstore.JobStore, dispatchStore *pgstore.DispatchStore, projector deltaflow.Projector, target deltaflow.ProjectionApplier) *deltaflow.SyncWorker

func runDemo(ctx context.Context, dsn string, makeWorker workerFactory) (demoResult, error) {
	totalStart := time.Now()
	setupStart := totalStart

	fileLogger, err := hostpkg.OpenFileLogger(os.Getenv("DELTAFLOW_WORKER_LOG"))
	if err != nil {
		return demoResult{}, err
	}
	defer fileLogger.Close()
	fileLogger.Logger.Info("playground_run_started", "sync_id", syncID, "scenario", "postgres-redis")

	db, deltaStore, jobStore, dispatchStore, err := hostpkg.OpenStoresWithOptions(ctx, dsn, hostpkg.OpenStoresOptions{
		MaxAttempts: workerMaxAttempts,
		LeaseLogger: fileLogger.Logger,
	})
	if err != nil {
		return demoResult{}, err
	}
	defer db.Close()

	if err := hostpkg.ResetSync(ctx, db, syncID); err != nil {
		return demoResult{}, err
	}

	scenario, err := buildScenario(ctx, db)
	if err != nil {
		return demoResult{}, err
	}
	projector := &countingProjector{projectFn: scenario.source.project}
	setupElapsed := time.Since(setupStart)

	var writersDone atomic.Bool
	workerCtx, workerCancel := context.WithCancel(ctx)
	defer workerCancel()
	workerStatsCh := make(chan struct {
		stats hostpkg.WorkerLoopStats
		err   error
	}, 1)
	go func() {
		stats, err := hostpkg.RunWorkers(
			workerCtx,
			1,
			func(workerID string) *deltaflow.SyncWorker {
				worker := makeWorker(workerID, jobStore, dispatchStore, projector, scenario.target)
				worker.Logger = fileLogger.Logger
				return worker
			},
			func(ctx context.Context) (bool, error) {
				if !writersDone.Load() {
					return false, nil
				}
				return hostpkg.WorkComplete(ctx, db, syncID)
			},
			func(ctx context.Context) error {
				return hostpkg.MakeRetryingAvailable(ctx, db, syncID)
			},
		)
		workerStatsCh <- struct {
			stats hostpkg.WorkerLoopStats
			err   error
		}{stats: stats, err: err}
	}()

	enqueueStart := time.Now()
	writerResult, err := runWriters(ctx, db, deltaStore, scenario.source, scenario.events)
	enqueueElapsed := time.Since(enqueueStart)
	writersDone.Store(true)
	if err != nil {
		workerCancel()
		select {
		case <-workerStatsCh:
		case <-time.After(2 * time.Second):
		case <-ctx.Done():
		}
		return demoResult{
			Scenario:      scenario,
			Enqueued:      writerResult.Enqueued,
			Timings:       hostpkg.RunTimings{Setup: setupElapsed, Enqueue: enqueueElapsed, Total: time.Since(totalStart)},
			WorkerLogPath: fileLogger.Path,
		}, err
	}

	drainStart := time.Now()
	workerResult := <-workerStatsCh
	drainElapsed := time.Since(drainStart)
	if workerResult.err != nil {
		return demoResult{}, workerResult.err
	}

	counts, err := hostpkg.CountJobs(ctx, db, syncID)
	if err != nil {
		return demoResult{}, err
	}
	searchQueue, orderQueue, searchDocs, upserts, deletes, failures, err := scenario.target.snapshot(ctx)
	if err != nil {
		return demoResult{}, err
	}
	timings := hostpkg.RunTimings{
		Setup:   setupElapsed,
		Enqueue: enqueueElapsed,
		Drain:   drainElapsed,
		Total:   time.Since(totalStart),
	}
	fileLogger.Logger.Info("playground_run_completed",
		"sync_id", syncID,
		"enqueued", writerResult.Enqueued,
		"jobs_synced", counts.Synced,
		"jobs_dead", counts.Dead,
		"ghost_jobs", counts.Ghosts,
		"run_once_calls", workerResult.stats.RunOnceCalls,
		"total_ms", timings.Total.Milliseconds(),
	)

	return demoResult{
		Scenario:        scenario,
		Enqueued:        writerResult.Enqueued,
		WorkerStats:     workerResult.stats,
		JobCounts:       counts,
		ProjectorGhosts: projector.ghostCount(),
		SearchQueue:     searchQueue,
		OrderQueue:      orderQueue,
		SearchDocs:      searchDocs,
		TargetUpserts:   upserts,
		TargetDeletes:   deletes,
		TargetFailures:  failures,
		Timings:         timings,
		WorkerLogPath:   fileLogger.Path,
	}, nil
}
