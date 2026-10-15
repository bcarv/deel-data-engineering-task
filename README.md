# Solution — Streaming Analytics Platform
 
> Deel instructions are preserved at the bottom of this README, under
> ["Original task README"](#original-task-readme).
 
From `docker-compose up` to live business metrics in **~2 minutes**, with a single command and zero manual setup.
 
## Quick start
 
Requirements: Docker + docker-compose, Unix shell (macOS/Linux — the compose file uses `${PWD}`).
 
```bash
# from the repository root
docker-compose up -d --build
```
 
Then open:
 
| What | Where | Notes |
|---|---|---|
| **Live metrics dashboard** | http://localhost:3000/d/acme-logistics | No login required (anonymous viewer) |
| Spark UI (streaming metrics) | http://localhost:4040 | Structured Streaming tab: one named query per topic |
| Analytics database | `localhost:5432`, db `finance_db`, user `finance_db_user` / `1234` | Analytical model in schema `analytics` |
 
On a cold start the dashboard panels populate within ~2–3 minutes: Postgres init → Kafka Connect boots → the Debezium connector is registered → snapshot fills the topics → Spark starts streaming. The Spark app actively waits for the CDC topics before starting its queries, so the whole stack converges without intervention.
 
## Architecture
 
![Architecture](diagrams/architecture.md)
 
```
pg_cron ──> Postgres (operations schema)
                 │  WAL (wal_level=logical)
                 ▼
         Debezium / Kafka Connect ──> Kafka (4 CDC topics)
                                            │
                                            ▼
                     Spark Structured Streaming (4 queries, one per topic)
                       foreachBatch: dedup → staging → ON CONFLICT merge
                       orders also append into orders_status_history
                                            │
                                            ▼
                  Postgres (analytics schema: star schema + history + views)
                                            │
                                            ▼
                         Grafana (provisioned dashboard, 10s refresh)
```
 
Cluster sizing strategy and the Spark metrics backing it: see [docs/cluster-sizing.md](docs/cluster-sizing.md). <!-- TODO: create -->
 
## Dimensional model (schema `analytics`)
 
| Table | Grain | Role |
|---|---|---|
| `fact_orders` | one row per order (current state) | Order facts: dates, customer, **status** |
| `fact_order_items` | one row per order item (current state) | Quantities per product per order |
| `dim_customers` | one row per customer | Customer dimension |
| `dim_products` | one row per product | Product dimension |
| `orders_status_history` | **one row per order event** (append-only) | Full audit trail of status transitions — covers the *historical information* requirement alongside the current state |
 
Every state table carries two control columns written by the pipeline: `is_deleted` (logical delete flag) and `event_ts_ms` (source event timestamp, used as a merge guard).
 
The four business questions are answered by dedicated views in `db/03-views/` (also exposed in the Grafana dashboard):
 
1. `vw_open_orders_by_delivery_date_status`
2. `vw_top3_delivery_dates_open_orders`
3. `vw_pending_items_by_product`
4. `vw_top3_customers_pending_orders`
**Definition of "open" order:** `status NOT IN ('COMPLETED', 'CANCELLED')` and not logically deleted. <!-- TODO: confirm against SELECT DISTINCT status and adjust if needed -->
 
## How it works — the Spark application
 
`docker/spark-app/app/streaming_job.py` runs four Structured Streaming queries on one SparkSession, one per Debezium topic, driven by a single configuration list (topic, row schema, primary key, staging and target tables). Each micro-batch:
 
1. **Parses** the Debezium envelope (`before`/`after`/`op`/`ts_ms` — published as plain JSON at the message top level, `schemas.enable=false`) into typed columns. On deletes, `after` is null and the primary key is taken from `before`.
2. **Deduplicates** within the batch, keeping the latest event per primary key (`ts_ms`).
3. Writes the batch to a **staging table** (recreated by Spark on every batch — intentionally not part of the versioned DDL) and merges into the final table with `INSERT ... ON CONFLICT ... DO UPDATE`, guarded by `WHERE target.event_ts_ms <= EXCLUDED.event_ts_ms` so a late or replayed event can never overwrite newer state.
4. For orders, additionally **appends every raw event** (no dedup) into `orders_status_history`.
Checkpoints live in a named Docker volume (`spark-checkpoints`), one directory per query. Combined with the idempotent merge, a crash or restart replays the last micro-batch with an identical end result — effectively-once semantics for the state tables.
 
## Key design decisions
 
- **Logical FKs only.** CDC events across topics are not mutually ordered, so enforcing physical foreign keys would break batches on out-of-order arrival; referential integrity is already guaranteed by the transactional source. Relationships are documented in the model, and the metric views use `LEFT JOIN` to tolerate a momentarily missing dimension row. PKs are enforced — `ON CONFLICT` requires them.
- **Deletes as a flag (`is_deleted`)**, never removing rows, so history-style queries keep working; metric views filter it out.
- **Staging tables without DDL.** They are transient per-batch artifacts owned by Spark (`mode="overwrite"`); only final tables live in versioned DDL. Each query has its own staging table — they must never be shared.
- **Startup resilience.** On a cold start the Spark app boots before Debezium has created the topics; the job polls for topic availability (clean `[startup] waiting for topic ...` logs) instead of crashing, with `restart: unless-stopped` as a safety net.
- **`orders_status_history` appends are at-least-once**: a crash between the append and the offset commit can duplicate history rows (state tables are unaffected). Accepted for the MVP and fixable with a unique index + `ON CONFLICT DO NOTHING` (see improvements).
- **Grafana provisioned as code** (datasource + dashboard in `docker/grafana/`), so the reviewer gets a working dashboard with zero setup; the 10s refresh matches the pipeline trigger interval. Human-friendly column names are applied at the presentation edge (SQL aliases in the dashboard), keeping snake_case in the data layer.
- **Demo credentials are intentionally plain** (consistent with the provided stack); production would use secrets management.
- **Validate the data contract against real messages.** Three real issues were found and fixed during development by inspecting the running system rather than trusting assumptions: the startup race with Debezium topic creation, an invalid Kafka batch-probe option (`endingOffsets` must be `latest`), and the Debezium envelope being published without the `payload` wrapper. The commit history tells this story.
## How to verify
 
With the stack running (all commands from the repository root):
 
```bash
# Data freshness — age of the latest processed event (expected: a few seconds)
docker-compose exec transactions-db psql -U finance_db_user -d finance_db -c "
SELECT MAX(to_timestamp(event_ts_ms / 1000.0))         AS latest_event,
       NOW() - MAX(to_timestamp(event_ts_ms / 1000.0)) AS data_age
FROM analytics.fact_orders;"
 
# Growing counts — run twice ~20s apart
docker-compose exec transactions-db psql -U finance_db_user -d finance_db -c "
SELECT NOW()::time AS queried_at,
       (SELECT COUNT(*) FROM analytics.fact_orders)           AS orders,
       (SELECT COUNT(*) FROM analytics.fact_order_items)      AS order_items,
       (SELECT COUNT(*) FROM analytics.dim_customers)         AS customers,
       (SELECT COUNT(*) FROM analytics.dim_products)          AS products,
       (SELECT COUNT(*) FROM analytics.orders_status_history) AS history;"
 
# Full journey of one order — current state + audit trail
docker-compose exec transactions-db psql -U finance_db_user -d finance_db -c "
SELECT order_id, status, op, changed_at
FROM analytics.orders_status_history
ORDER BY ingested_at DESC LIMIT 10;"
 
# Recovery test: kill the Spark app mid-batch and watch it resume from the
# checkpoint with no duplicates (idempotent merge)
docker-compose kill spark-app && docker-compose up -d spark-app
```
 
Evidence of the running system (Spark UI metrics, dashboard, latency tests): [docs/evidence/](docs/evidence/).
 
Notes for reviewers:
- A panel may briefly show *No data* when the business condition is legitimately empty (e.g. no open orders with items at that instant) — it repopulates as the generator creates activity.
- If your browser holds a stale Grafana session cookie from a previous local Grafana, open the dashboard in a private window or visit `/logout` once.
- During development, `docker-compose up -d --build spark-app` rebuilds only the Spark app and its dependency chain (Grafana is not part of it); the full stack is always `docker-compose up -d --build`.
## Future improvements
 
- **SCD Type 2** on dimensions for full attribute history (current model keeps latest state + order status history).
- **Schema Registry** (Avro/Protobuf) instead of raw JSON: schemas resolved at runtime with compatibility rules, removing the hand-written `StructType`s.
- **Dead-letter queue** for malformed messages (today, rows failing the primary-key filter are silently dropped; a counter/log plus a DLQ topic would make that observable).
- **Exactly-once history**: unique index on `(order_id, source_ts_ms, op)` + `ON CONFLICT DO NOTHING`, and populate `previous_status`.
- **Automated tests** (parse unit tests with sample envelopes; an end-to-end smoke test in CI).
- **Modularization** of the Spark job (`config.py`, `schemas.py`, `merge.py`) as it grows; trigger interval and batch size via environment for tuning without rebuilds.
---
 
# Original task README



## Data Engineering Take-Home Task

### Welcome

Welcome to Deel's Data Engineering Take-Home task, as mentioned in the Task specification document, this is the pre-built stack that will help you on your solution development. This repository contains a pre-configured database containing the database represented by the following DER:


![Database Diagram](./diagrams/database-diagram.png)


### Database Configuration

Once you have [Docker](https://www.docker.com/products/docker-desktop/) and [docker-compose](https://docs.docker.com/compose/install/) configured in your computer, with your Docker engine running, you must execute the following command provision the source database:


> docker-compose up


:warning:**Important**: Before running this command make sure you're in the root folder of the project.

Once you have the Database up and running feel free to connect to this using any tool you want, for this you can use the following credentials:

- **Username**: `finance_db_user`
- **Password**: `1234`
- **Database**: `finance_db`

### Debezium CDC

The stack includes a Debezium CDC pipeline that streams database changes to Kafka in real-time. Kafka is available at `localhost:9092`.

#### Topics

| Kafka Topic | Source Table |
|---|---|
| `finance_db.operations.customers` | `operations.customers` |
| `finance_db.operations.products` | `operations.products` |
| `finance_db.operations.orders` | `operations.orders` |
| `finance_db.operations.order_items` | `operations.order_items` |

#### Kafka Connection Example

```properties
bootstrap.servers=localhost:9092
```

Extra informations and tips about the task execution can be found in the task description document shared by our recruiting team.

For any questions, feel free to reach us out through data-platform@deel.com
