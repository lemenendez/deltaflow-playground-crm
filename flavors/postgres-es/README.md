# postgres-es (with order fanout simulation)

Project flavor status:

- [x] `flavors/postgres-es` (implemented)
- [ ] `flavors/postgres-redis` (pending)
- [ ] `flavors/sqlite-es` (pending)
- [ ] `flavors/sqlite-redis` (pending)

Concurrent workload playground for CRM read models and search/queue fanout.

Flavor identity:

- Source and sync stores: Postgres
- Search fanout target: Elasticsearch
- Order queue fanout events: simulated in-process (no Redis container)

The scenario simulates:

- backend servers changing users, roles, customers, and orders
- CRM workers updating customer contact data and order statuses
- 2 DeltaFlow workers claiming jobs from the same Postgres-backed sync
- deterministic fake users/customers/orders from `gofakeit` with a fixed seed
- API/worker mutations update durable Postgres CRM tables and enqueue every change through `DeltaStore.EnqueueInTx`
- Elasticsearch-backed search fanout through the concrete Elasticsearch applier
- ghost deletion for a stale customer view
- transient retry and dead-letter behavior without crashing a process

The CRM source and DeltaFlow stores are durable Postgres tables. The docker compose run starts Elasticsearch and uses the concrete applier for the search fanout side while keeping order publication as a local simulation. A direct local `go run .` without `DELTAFLOW_ES_ENDPOINT` still falls back to the all-simulated target.

The REST/API consistency model is represented by the writer stage: every application-side CRM mutation and its Delta are committed in the same Postgres transaction through `DeltaStore.EnqueueInTx`. Elasticsearch is updated asynchronously by DeltaFlow workers after the transaction commits.

## Public API vs Playground Glue

This playground uses the public Elasticsearch applier from `pkg/connectors/elasticsearch` for the real search fanout writes. `elasticsearch_target.go` wraps that applier only for demo concerns:

- create/reset the demo Elasticsearch index
- seed a stale ghost customer document
- map projection keys to CRM document IDs
- keep order publication as a local simulation
- inject retry/dead-letter failures
- count applied operations and snapshot indexed documents for the report
- fall back to the all-simulated target when `DELTAFLOW_ES_ENDPOINT` is unset

The application-owned work is still explicit: `domain.go` defines the source model and projector, `engine.go` wires `SyncWorker`, and the writer path enqueues Deltas transactionally with source writes.

## Run

From this folder:

- `make run`

Default simulation size:

- `USER_COUNT=8`
- `CUSTOMER_COUNT=18`
- `ORDER_COUNT=22`
- `MUTATION_COUNT=64`
- `WRITER_COUNT=4`
- `WORKERS_CONCURRENCY=2`
- `WORKERS_BATCH_SIZE=8`
- `WORKERS_MAX_ATTEMPTS=3`
- `SIM_SEED=4004`

Custom size example:

- `make run USER_COUNT=50 CUSTOMER_COUNT=500 ORDER_COUNT=1000 MUTATION_COUNT=5000 WRITER_COUNT=8 WORKERS_CONCURRENCY=4 WORKERS_BATCH_SIZE=128`

`USER_COUNT`, `CUSTOMER_COUNT`, and `ORDER_COUNT` change the source universe. `MUTATION_COUNT` controls how many live writes/deltas/jobs are created. If you only increase `CUSTOMER_COUNT` or `ORDER_COUNT`, the run has a larger pool to choose from but the same number of mutations.

The workload always adds three special deltas on top of `MUTATION_COUNT`: one retry-once customer, one dead-letter order, and one ghost delete.

Useful commands:

- `make up` to start Postgres and Elasticsearch
- `make migrate` to apply schema
- `make reset` to drop/recreate the schema
- `make down` to stop and remove containers/volumes
- `make counts` to show delta/job counts by state
- `make jobs-by-state` to show job state counts with ghost totals
- `make deltas` to show the first 50 deltas for this sync
- `make pending-deltas` to inspect un-dispatched deltas
- `make jobs` to show the first 50 sync jobs
- `make dead-jobs` to inspect dead-lettered jobs
- `make worker-log` to tail `logs/deltaflow-worker.log`
- `make beautify` to pretty-print the latest JSON worker log lines
- `make beautify LOG_LINES=20` to control how many log lines are formatted

The worker and Postgres lease logs are written to `logs/deltaflow-worker.log` by default. Override the file path with `DELTAFLOW_WORKER_LOG`.

## Troubleshooting

### Error: `job lease not owned`

This usually means a worker claimed a batch and one of the later jobs in that batch lost lease ownership before it was finalized.

Practical fixes:

- Keep `WORKERS_BATCH_SIZE` small for this flavor (default is `8`; use `4` or `1` for maximum stability).
- If you need larger batches, increase worker lease duration in [internal/scenario/host/host.go](internal/scenario/host/host.go).
- Reduce `WORKERS_CONCURRENCY` if Elasticsearch/applier paths are slow.

Example stable run:

- `make run WORKERS_BATCH_SIZE=4 WORKERS_CONCURRENCY=2`

### Error: `column dedup_window ... does not exist`

The schema is missing Deltaflow dedup migrations.

Fix:

- Run `make migrate` again (the Makefile applies migration `000007_add_delta_dedup.sql`).
