package main

import (
	"context"
	"os"
	"sync/atomic"
	"time"

	hostpkg "github.com/lemenendez/deltaflow-playground-crm/internal/scenario/host"
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

func runDemo(ctx context.Context, dsn string) (demoResult, error) {
	totalStart := time.Now()
	setupStart := totalStart

	fileLogger, err := hostpkg.OpenFileLogger(os.Getenv("DELTAFLOW_WORKER_LOG"))
	if err != nil {
		return demoResult{}, err
	}
	defer fileLogger.Close()
	fileLogger.Logger.Info("playground_run_started", "sync_id", syncID, "scenario", "postgres-redis")

	stores, err := hostpkg.OpenStoresWithOptions(ctx, dsn, hostpkg.OpenStoresOptions{
		MaxAttempts: workerMaxAttempts,
		LeaseLogger: fileLogger.Logger,
	})
	if err != nil {
		return demoResult{}, err
	}
	defer stores.DB.Close()

	if err := hostpkg.ResetSync(ctx, stores.DB, syncID); err != nil {
		return demoResult{}, err
	}

	scenario, err := buildScenario(ctx, stores)
	if err != nil {
		return demoResult{}, err
	}
	projector := &countingProjector{projectFn: scenario.source.project}
	setupElapsed := time.Since(setupStart)

	var writersDone atomic.Bool
	workerStatsCh := make(chan struct {
		stats hostpkg.WorkerLoopStats
		err   error
	}, 1)
	go func() {
		stats, err := hostpkg.RunWorkers(
			ctx,
			1,
			func(workerID string) *deltaflow.SyncWorker {
				worker := hostpkg.MakeWorker(stores, syncID, workerID, projector, scenario.target, 0, workerBatchSize)
				worker.Concurrency = workerConcurrency
				worker.Logger = fileLogger.Logger
				return worker
			},
			func(ctx context.Context) (bool, error) {
				if !writersDone.Load() {
					return false, nil
				}
				return hostpkg.WorkComplete(ctx, stores.DB, syncID)
			},
			func(ctx context.Context) error {
				return hostpkg.MakeRetryingAvailable(ctx, stores.DB, syncID)
			},
		)
		workerStatsCh <- struct {
			stats hostpkg.WorkerLoopStats
			err   error
		}{stats: stats, err: err}
	}()

	enqueueStart := time.Now()
	writerResult, err := runWriters(ctx, stores, scenario.source, scenario.events)
	enqueueElapsed := time.Since(enqueueStart)
	writersDone.Store(true)
	if err != nil {
		select {
		case <-workerStatsCh:
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

	counts, err := hostpkg.CountJobs(ctx, stores.DB, syncID)
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
