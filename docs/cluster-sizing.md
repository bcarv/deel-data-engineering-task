# Spark Cluster Sizing Strategy

This document covers the deliverable "the Spark cluster sizing strategy, and a
description of which variables were taken into account", plus the key Spark
metrics observed on the running demo.

## 1. Variables taken into account

Sizing a Structured Streaming cluster for this workload is driven by six
variables. For each one: what it controls, and its value in this demo.

| Variable | What it drives | In this demo |
|---|---|---|
| **Event throughput (events/s)** | Total processing capacity needed | Low-to-moderate: ~65 ev/s aggregate average over the first ~5 min (snapshot included), dominated by `order_items` (~49 ev/s) and `orders` (~15 ev/s) |
| **Message size** | Network + deserialization cost per event; executor memory per batch | Small JSON (~1–2 KB per Debezium envelope) |
| **Kafka partitions per topic** | **Upper bound on read parallelism**: each partition maps to at most one Spark task per micro-batch | 1 partition per topic (Debezium default on this stack) → parallelism 1 per query, 4 concurrent queries |
| **Latency SLA (trigger interval)** | How often micro-batches fire; smaller trigger = lower latency but more scheduling overhead and more frequent JDBC merges | 10 s — matches the "near-real-time" requirement and the dashboard refresh |
| **Micro-batch size bound** (`maxOffsetsPerTrigger`) | Backpressure: caps how much is read per trigger so a backlog (e.g. the initial snapshot or downtime recovery) is drained in bounded, memory-safe batches instead of one giant batch | 10 000 offsets per trigger per query |
| **Sink write cost** | The JDBC staging write + `ON CONFLICT` merge runs per batch; dedup-before-write keeps it proportional to *distinct keys* per batch, not raw events | Single Postgres instance, 4 independent staging tables |

Two derived rules that follow from these variables:

- **Executors scale with partitions, not with data volume alone.** Adding
  executors beyond the total partition count buys nothing for the Kafka read
  stage; to scale reads, partitions and executors grow together.
- **The system is healthy when `batch duration < trigger interval`** for every
  query. That single inequality, read off the Spark UI, is the sizing
  acceptance test.

## 2. Demo sizing: `local[2]`, single container

The demo runs driver and executors in one JVM with 2 cores
(`spark-submit --master local[2]`, default 1 GB executor memory), deliberately:

- With 1 partition per topic, per-query parallelism is 1; the 4 queries share
  the 2 cores through Spark's fair scheduling of their (small) tasks.
- A standalone cluster (separate master/workers) would add operational surface
  to the docker-compose stack with **zero** throughput benefit at this volume.
- The constrained sizing is also honest: the observed headroom (below) shows
  how far even a minimal footprint goes, which is the baseline the scaling
  strategy extrapolates from.

## 3. Observed metrics (Spark UI, Structured Streaming tab)

Measured on the demo stack ~3–6 minutes after a cold start (averages include
the initial snapshot burst). Screenshots in `docs/evidence/`.

**Per-query rates (Structured Streaming overview):**

| Query | Avg input (ev/s) | Avg process (ev/s) | Rate headroom |
|---|---:|---:|---:|
| `order_items` | 48.87 | 123.38 | 2.5× |
| `orders` | 15.13 | 36.02 | 2.4× |
| `products` | 0.60 | 1.49 | 2.5× |
| `customers` | 0.35 | 1.34 | 3.8× |
| **Aggregate** | **~65** | **~162 demonstrated** | — |

**Batch-level detail (query `orders`, 16 completed batches over 5m40s):**

| Metric | Observed | Interpretation |
|---|---|---|
| Batch duration | ~5 000–8 500 ms typical; first (snapshot) batch ~13 000 ms | Steady-state batches complete within the 10 s trigger, but headroom on batch time is **~1.2–2×, not large** — the honest constraint of this sizing |
| Operation duration breakdown | Dominated by `addBatch` (the sink path: JDBC staging write + `ON CONFLICT` merge + history append) | **The bottleneck is the Postgres sink, not stream processing** — empirically confirming scaling lever #4 below |
| Input rows per batch | Steady ~200 rows/batch for orders | Regular CDC flow; `maxOffsetsPerTrigger` never binding at this volume |
| Batch cadence | 16 batches in 5m40s (~21 s/cycle vs 10 s trigger) | The 4 concurrent queries partially serialize on the 2 shared cores; no backlog accumulates (process ≥ input throughout), but the demo runs closer to capacity than the rate table alone suggests — more cores/executors is the first lever at higher volume |

End-to-end latency measured at the row level
(`orders_status_history.ingested_at - changed_at`) remains a few seconds.

Evidence: `docs/evidence/` (Spark UI screenshots, per-row `pipeline_lag`
measurements from `orders_status_history`, typically a few seconds end to end).

## 4. Scaling strategy for production volumes

The task states "millions of records". The path from this demo to that scale,
in order of leverage:

1. **Partition the CDC topics** (e.g. 12–24 partitions per high-volume topic,
   keyed by primary key so per-key ordering is preserved — required for the
   dedup/merge correctness).
2. **Move to a real cluster** (Kubernetes/EMR/Databricks): driver 2 cores/4 GB;
   executors sized 4 cores/8 GB each; executor count ≈ total partitions /
   cores-per-executor. Enable dynamic allocation to track daily volume curves.
3. **Tune the trigger to the SLA, not lower.** If 10 s meets the business SLA,
   keep it: fewer, larger batches amortize the per-batch JDBC merge cost.
   Raise `maxOffsetsPerTrigger` proportionally to keep recovery drain time
   bounded (e.g. target: backlog after a 10-minute outage drained in under
   2 minutes).
4. **Watch the sink before the cluster.** Already visible at demo scale: the
   `addBatch` phase (JDBC staging write + merge + history append) dominates
   batch duration. At high volume the Postgres merge becomes the bottleneck
   before Spark does; mitigations in order: batch `executemany`/COPY into
   staging, partitioned target tables, or promoting the analytical store to a
   warehouse/lakehouse (see README improvements).
5. **Capacity rule of thumb** from the observed baseline: the 4 queries
   demonstrated an aggregate processing rate of ~162 ev/s on 2 shared cores
   (~80 ev/s per core with this sink-bound pipeline shape). Size cores for the
   expected peak input rate with ≥ 2× headroom — and pair every core increase
   with sink-side capacity (lever #4), since batch time is sink-dominated —
   then let the `batch duration < trigger interval` check on the Spark UI
   validate the sizing empirically.

## 5. Monitoring in production

The same signals used here, automated: `batch duration` and `scheduling delay`
trending toward the trigger interval are the early-warning alerts;
`orders_status_history.ingested_at - changed_at` (per-row pipeline lag) is the
end-to-end SLA metric exposed directly in SQL.