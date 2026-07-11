package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	hostpkg "github.com/lemenendez/deltaflow-playground-crm/internal/scenario/host"
)

const (
	syncID          = "playground-crm-to-elasticsearch"
	userProjection  = "CRMUserView"
	custProjection  = "CRMCustomerView"
	orderProjection = "CRMOrderFanout"
)

var (
	seed                  = uint64(4004)
	userCount             = 8
	customerCount         = 18
	orderCount            = 22
	mutationCount         = 64
	writerCount           = 4
	workerConcurrency     = 2
	workerBatchSize       = 8
	workerMaxAttempts     = 3
	elasticsearchEndpoint = ""
)

var baseActorNames = []string{
	"api-server-1",
	"api-server-2",
	"crm-worker-1",
	"crm-worker-2",
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	if err := loadConfig(); err != nil {
		log.Fatalf("load config: %v", err)
	}

	dsn := os.Getenv("DELTAFLOW_PG_DSN")
	if dsn == "" {
		dsn = hostpkg.DefaultDSN()
	}

	result, err := runDemo(ctx, dsn)
	if err != nil {
		log.Fatalf("scenario failed: %v", err)
	}

	printReport(result)
}

func loadConfig() error {
	var err error
	if seed, err = hostpkg.EnvUint64("SIM_SEED", seed); err != nil {
		return err
	}
	if userCount, err = hostpkg.EnvInt("USER_COUNT", userCount); err != nil {
		return err
	}
	if customerCount, err = hostpkg.EnvInt("CUSTOMER_COUNT", customerCount); err != nil {
		return err
	}
	if orderCount, err = hostpkg.EnvInt("ORDER_COUNT", orderCount); err != nil {
		return err
	}
	if mutationCount, err = hostpkg.EnvInt("MUTATION_COUNT", mutationCount); err != nil {
		return err
	}
	if writerCount, err = hostpkg.EnvInt("WRITER_COUNT", writerCount); err != nil {
		return err
	}
	if writerCount < 4 {
		return fmt.Errorf("WRITER_COUNT must be >= 4 for this scenario")
	}
	if workerConcurrency, err = hostpkg.EnvInt("WORKERS_CONCURRENCY", workerConcurrency); err != nil {
		return err
	}
	if workerConcurrency <= 0 {
		return fmt.Errorf("WORKERS_CONCURRENCY must be > 0")
	}
	if workerBatchSize, err = hostpkg.EnvInt("WORKERS_BATCH_SIZE", workerBatchSize); err != nil {
		return err
	}
	if workerBatchSize <= 0 {
		return fmt.Errorf("WORKERS_BATCH_SIZE must be > 0")
	}
	if workerMaxAttempts, err = hostpkg.EnvInt("WORKERS_MAX_ATTEMPTS", workerMaxAttempts); err != nil {
		return err
	}
	if workerMaxAttempts <= 0 {
		return fmt.Errorf("WORKERS_MAX_ATTEMPTS must be > 0")
	}
	elasticsearchEndpoint = os.Getenv("DELTAFLOW_ES_ENDPOINT")
	return nil
}

func actorName(actorID int) string {
	if actorID >= 0 && actorID < len(baseActorNames) {
		return baseActorNames[actorID]
	}
	return fmt.Sprintf("crm-actor-%d", actorID+1)
}
