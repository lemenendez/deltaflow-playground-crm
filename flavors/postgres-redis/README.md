# postgres-redis (order metrics fanout)

Project flavor status:

- [x] `flavors/postgres-es` (implemented)
- [x] `flavors/postgres-redis` (implemented)
- [ ] `flavors/sqlite-es` (pending)
- [ ] `flavors/sqlite-redis` (pending)

Concurrent workload playground for CRM read models and Redis aggregate fanout.

Flavor identity:

- Source and sync stores: Postgres
- Metrics fanout target: Redis (KV)
- Aggregation policy: refresh counters/averages on every order upsert/delete
- Connector usage: DeltaFlow Redis applier (`pkg/connectors/redis`) wrapped by custom aggregate simulation logic

The scenario simulates:

- backend servers changing users, roles, customers, and orders
- CRM workers updating customer contact data and order statuses
- 1 DeltaFlow worker claiming jobs from the same Postgres-backed sync by default
- deterministic fake users/customers/orders from `gofakeit` with a fixed seed
- API/worker mutations update durable Postgres CRM tables and enqueue every change through `DeltaStore.EnqueueInTx`
- Redis metric key refreshes when order projections are upserted/deleted
- transient retry and dead-letter behavior without crashing a process

The REST/API consistency model is represented by the writer stage: every application-side CRM mutation and its Delta are committed in the same Postgres transaction through `DeltaStore.EnqueueInTx`. Redis is updated asynchronously by DeltaFlow workers after the transaction commits.

## Metric Keys

This flavor keeps Redis writes intentionally lightweight and practical:

- `metrics:global:orders`
- `metrics:customer:{customer_id}:orders`

Each key stores JSON with:

- `order_count`
- `total_cents`
- `avg_total_cents`

## Run

From this folder:

- `make run`

Default simulation size:

- `USER_COUNT=8`
- `CUSTOMER_COUNT=18`
- `ORDER_COUNT=22`
- `MUTATION_COUNT=64`
- `WRITER_COUNT=4`
- `WORKERS_CONCURRENCY=1`
- `WORKERS_BATCH_SIZE=16`
- `WORKERS_MAX_ATTEMPTS=3`
- `SIM_SEED=4004`

Useful commands:

- `make up` to start Postgres and Redis
- `make migrate` to apply schema
- `make reset` to drop/recreate the schema
- `make down` to stop and remove containers/volumes
- `make counts` to show delta/job counts by state
- `make jobs-by-state` to show job state counts with ghost totals
- `make deltas` to show the first 50 deltas for this sync
- `make pending-deltas` to inspect un-dispatched deltas
- `make jobs` to show the first 50 sync jobs
- `make dead-jobs` to inspect dead-lettered jobs
- `make redis-keys` to list metric keys
- `make worker-log` to tail `logs/deltaflow-worker.log`
- `make beautify` to pretty-print the latest JSON worker log lines

The worker and Postgres lease logs are written to `logs/deltaflow-worker.log` by default. Override the file path with `DELTAFLOW_WORKER_LOG`.

## Isolation

This flavor uses a separate DeltaFlow `sync_id` and an isolated source schema (`playground_redis`) so it can be run alongside other flavors without interfering with their source tables or job streams.
