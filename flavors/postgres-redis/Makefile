DC ?= docker compose
PG_SERVICE ?= postgres
SYNC_ID ?= playground-crm-to-redis-metrics
LOG_LINES ?= 100
SIM_SEED ?= 4004
USER_COUNT ?= 8
CUSTOMER_COUNT ?= 18
ORDER_COUNT ?= 22
MUTATION_COUNT ?= 64
WRITER_COUNT ?= 4
WORKERS_CONCURRENCY ?= 1
WORKERS_BATCH_SIZE ?= 16
WORKERS_MAX_ATTEMPTS ?= 3
PSQL = $(DC) exec -T $(PG_SERVICE) psql -U deltaflow -d deltaflow -v ON_ERROR_STOP=1
REDIS = $(DC) exec -T redis redis-cli
DELTAFLOW_DIR := $(shell cd ../.. && go list -m -f '{{.Dir}}' github.com/lemenendez/deltaflow 2>/dev/null)
MIGRATIONS_DIR := $(DELTAFLOW_DIR)/pkg/connectors/postgres/migrations

export SIM_SEED USER_COUNT CUSTOMER_COUNT ORDER_COUNT MUTATION_COUNT WRITER_COUNT WORKERS_CONCURRENCY WORKERS_BATCH_SIZE WORKERS_MAX_ATTEMPTS DELTAFLOW_WORKER_LOG

.PHONY: up down pgcompat migrate run reset logs counts deltas pending-deltas jobs jobs-by-state dead-jobs worker-log worker-log-beautify beautify redis-keys

up:
	$(DC) up -d --wait $(PG_SERVICE) redis

down:
	$(DC) down --volumes --remove-orphans

pgcompat: up
	$(PSQL) -c "CREATE EXTENSION IF NOT EXISTS pgcrypto;"
	$(PSQL) -c 'CREATE OR REPLACE FUNCTION uuidv7() RETURNS uuid LANGUAGE SQL AS $$$$ SELECT gen_random_uuid(); $$$$;'

migrate: pgcompat
	@test -n "$(DELTAFLOW_DIR)" || (echo "Could not resolve github.com/lemenendez/deltaflow module path. Run 'go mod download' from repo root." && exit 1)
	@test -d "$(MIGRATIONS_DIR)" || (echo "Migrations directory not found: $(MIGRATIONS_DIR)" && exit 1)
	$(PSQL) -f - < "$(MIGRATIONS_DIR)/000001_create_deltaflow_deltas.sql"
	$(PSQL) -f - < "$(MIGRATIONS_DIR)/000002_create_deltaflow_sync_jobs.sql"
	$(PSQL) -f - < "$(MIGRATIONS_DIR)/000003_align_outbox_unique_index.sql"
	$(PSQL) -f - < "$(MIGRATIONS_DIR)/000004_scope_pending_index_by_sync_id.sql"
	$(PSQL) -f - < "$(MIGRATIONS_DIR)/000005_scope_claim_index_by_sync_id.sql"
	$(PSQL) -f - < "$(MIGRATIONS_DIR)/000006_add_processing_lease_ops_index.sql"
	$(PSQL) -f - < "$(MIGRATIONS_DIR)/000007_add_delta_dedup.sql"

run: migrate
	$(DC) run --rm playground

reset: up
	$(PSQL) -c "DROP SCHEMA IF EXISTS deltaflow CASCADE;"
	$(MAKE) migrate

logs:
	$(DC) logs -f $(PG_SERVICE) redis

counts: up
	$(PSQL) -c "SELECT 'deltas' AS table_name, state, COUNT(*) AS count FROM deltaflow.deltaflow_deltas WHERE sync_id = '$(SYNC_ID)' GROUP BY state UNION ALL SELECT 'jobs' AS table_name, state, COUNT(*) AS count FROM deltaflow.deltaflow_sync_jobs WHERE sync_id = '$(SYNC_ID)' GROUP BY state ORDER BY table_name, state;"

deltas: up
	$(PSQL) -c "SELECT id::text, state, origin, projection_type, projection_key, metadata, occurred_at, created_at, dispatched_at FROM deltaflow.deltaflow_deltas WHERE sync_id = '$(SYNC_ID)' ORDER BY occurred_at ASC, created_at ASC, id ASC LIMIT 50;"

pending-deltas: up
	$(PSQL) -c "SELECT id::text, origin, projection_type, projection_key, metadata, occurred_at, created_at FROM deltaflow.deltaflow_deltas WHERE sync_id = '$(SYNC_ID)' AND state = 'pending' ORDER BY occurred_at ASC, created_at ASC, id ASC LIMIT 50;"

jobs-by-state: up
	$(PSQL) -c "SELECT state, COUNT(*) AS count, COUNT(*) FILTER (WHERE ghost_detected) AS ghosts, MIN(created_at) AS first_created_at, MAX(updated_at) AS last_updated_at FROM deltaflow.deltaflow_sync_jobs WHERE sync_id = '$(SYNC_ID)' GROUP BY state ORDER BY state;"

jobs: up
	$(PSQL) -c "SELECT id::text, state, attempt_count, max_attempts, projection_type, projection_key, locked_by, locked_until, ghost_detected, last_error, created_at, updated_at FROM deltaflow.deltaflow_sync_jobs WHERE sync_id = '$(SYNC_ID)' ORDER BY created_at ASC, id ASC LIMIT 50;"

dead-jobs: up
	$(PSQL) -c "SELECT id::text, attempt_count, max_attempts, projection_type, projection_key, last_error, dead_at FROM deltaflow.deltaflow_sync_jobs WHERE sync_id = '$(SYNC_ID)' AND state = 'dead' ORDER BY dead_at ASC, id ASC;"

redis-keys: up
	$(REDIS) KEYS 'metrics:*' | cat

worker-log:
	@if [ -f logs/deltaflow-worker.log ]; then tail -n $(LOG_LINES) logs/deltaflow-worker.log; else echo "logs/deltaflow-worker.log does not exist yet. Run make run first."; fi

worker-log-beautify:
	@if [ ! -f logs/deltaflow-worker.log ]; then echo "logs/deltaflow-worker.log does not exist yet. Run make run first."; elif command -v jq >/dev/null 2>&1; then tail -n $(LOG_LINES) logs/deltaflow-worker.log | jq .; else echo "jq is required to beautify JSON logs. Install jq or use make worker-log."; fi

beautify: worker-log-beautify
