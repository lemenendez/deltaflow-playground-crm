package main

import (
	"fmt"
	"sort"
	"strings"

	hostpkg "github.com/lemenendez/deltaflow-playground-crm/internal/scenario/host"
)

func printReport(result demoResult) {
	breakdown := mutationBreakdown(result.Scenario.events)
	retryOrderID := retryOrderID(result.Scenario.events)

	fmt.Println("DeltaFlow playground postgres-redis")
	fmt.Println()
	fmt.Println("What happened")
	fmt.Printf("- Source universe: seed %d generated %d users, %d customers, and %d normal orders plus 1 poison order.\n", seed, userCount, customerCount, orderCount)
	fmt.Printf("- Workload size: %d random CRM mutations plus 3 special deltas.\n", mutationCount)
	fmt.Printf("- %d application-side actors updated durable Postgres CRM rows and wrote outbox deltas with DeltaStore.EnqueueInTx.\n", writerCount)
	fmt.Printf("- DeltaFlow workers ran with workers.concurrency=%d and workers.batch_size=%d.\n", workerConcurrency, workerBatchSize)
	fmt.Println("- Projector reads the latest CRM row and builds user/customer/order projections.")
	if redisAddress == "" {
		fmt.Println("- Applier simulates Redis metric refresh because DELTAFLOW_REDIS_ADDR is not set.")
	} else {
		fmt.Printf("- Applier refreshes Redis metric keys at %s on order upsert/delete.\n", redisAddress)
	}
	fmt.Println()

	fmt.Println("Workload")
	fmt.Printf("- CRM mutations: %s.\n", formatBreakdown(breakdown))
	if workerMaxAttempts >= 2 {
		fmt.Printf("- %s: simulated one temporary target timeout, then retry succeeded.\n", retryOrderID)
	} else {
		fmt.Printf("- %s: simulated one temporary target timeout, but workers.max_attempts=%d marks the job dead immediately.\n", retryOrderID, workerMaxAttempts)
	}
	fmt.Println("- ord-dead-001: simulated permanent target rejection, so the job reached dead-letter after max attempts.")
	fmt.Println("- ord-ghost-001: stale order view with no source order, so DeltaFlow issued a delete.")
	fmt.Println()

	fmt.Println("DeltaFlow result")
	fmt.Printf("- Deltas enqueued: %d of %d planned mutations.\n", result.Enqueued, len(result.Scenario.events))
	fmt.Printf("- Jobs synced: %d, dead-lettered: %d, ghosts deleted: %d.\n", result.JobCounts.Synced, result.JobCounts.Dead, result.JobCounts.Ghosts)
	fmt.Printf("- Queue drained: pending=%d retrying=%d processing=%d.\n", result.JobCounts.Pending, result.JobCounts.Retrying, result.JobCounts.Processing)
	fmt.Printf("- Worker RunOnce calls: %d.\n", result.WorkerStats.RunOnceCalls)
	fmt.Println()

	fmt.Println("Timing")
	fmt.Printf("- Setup: %s. Enqueue: %s. Worker drain: %s. Total: %s.\n",
		hostpkg.FormatDuration(result.Timings.Setup),
		hostpkg.FormatDuration(result.Timings.Enqueue),
		hostpkg.FormatDuration(result.Timings.Drain),
		hostpkg.FormatDuration(result.Timings.Total),
	)
	fmt.Printf("- Enqueue throughput: %.1f deltas/sec. Drain throughput: %.1f terminal jobs/sec.\n",
		hostpkg.PerSecond(result.Enqueued, result.Timings.Enqueue),
		hostpkg.PerSecond(result.JobCounts.Synced+result.JobCounts.Dead, result.Timings.Drain),
	)
	fmt.Printf("- Worker and lease log: %s.\n", result.WorkerLogPath)
	fmt.Println()

	if redisAddress == "" {
		fmt.Println("Simulated Redis result")
	} else {
		fmt.Println("Redis result")
	}
	fmt.Printf("- Upserts: %d, deletes: %d, target failures observed: %d.\n", result.TargetUpserts, result.TargetDeletes, result.TargetFailures)
	fmt.Printf("- Order operation events: %d. Metrics refresh events: %d.\n", len(result.SearchQueue), len(result.OrderQueue))
	if len(result.SearchDocs) > 0 {
		fmt.Printf("- Redis metric keys: %d. Snapshot digest: %s.\n", len(result.SearchDocs), hostpkg.StableDigest(result.SearchDocs))
	}
	fmt.Printf("- Last order operation events: %s.\n", queueTail(result.SearchQueue, 5))
}

func mutationBreakdown(events []mutation) map[string]int {
	counts := make(map[string]int)
	for _, event := range events {
		key := event.Entity + "." + event.Kind
		counts[key]++
	}
	return counts
}

func formatBreakdown(counts map[string]int) string {
	keys := make([]string, 0, len(counts))
	for key := range counts {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", key, counts[key]))
	}
	return strings.Join(parts, ", ")
}

func retryOrderID(events []mutation) string {
	for _, event := range events {
		if event.Seq == mutationCount+1 {
			return event.EntityID
		}
	}
	return "retry-order"
}
