# Architecture

End-to-end streaming analytics pipeline. Every component below maps 1:1 to a
service in `docker-compose.yaml` or a code path in
`docker/spark-app/app/streaming_job.py`.

```mermaid
flowchart TB
    subgraph source["Source — transactions-db (Postgres 15)"]
        pgcron["pg_cron<br/><i>auto-generates inserts/updates/deletes</i>"]
        operations[("operations schema<br/>customers · products<br/>orders · order_items")]
        pgcron --> operations
    end

    subgraph cdc["Change Data Capture"]
        connect["kafka-connect<br/>(Debezium 2.4)"]
        init["debezium-init<br/><i>registers finance-db-connector,<br/>waits for Connect readiness</i>"]
        init -. "POST /connectors" .-> connect
    end

    operations == "WAL<br/>(wal_level=logical)" ==> connect

    subgraph kafka_broker["Kafka (+ Zookeeper)"]
        topics[["4 CDC topics<br/>finance_db.operations.*<br/><i>snapshot (op=r) + incremental (c/u/d)</i>"]]
    end

    connect == "Debezium envelope JSON<br/>(before/after/op/ts_ms)" ==> topics

    subgraph spark["spark-app — Spark Structured Streaming 3.5 (local[2])"]
        wait["startup guard:<br/>wait_for_kafka_topics()"]
        queries["4 streaming queries<br/><i>one per topic · trigger 10s<br/>maxOffsetsPerTrigger=10000</i>"]
        feb["foreachBatch<br/>1· parse envelope → typed rows<br/>2· dedup: latest event per PK<br/>3· staging (overwrite) → ON CONFLICT merge<br/><i>guard: event_ts_ms</i>"]
        ckpt[("spark-checkpoints<br/>volume<br/><i>one dir per query</i>")]
        wait --> queries --> feb
        queries -. "offsets" .-> ckpt
    end

    topics ==> wait

    subgraph analytics["Analytics — same Postgres, analytics schema"]
        staging[("stg_* tables<br/><i>transient, per batch</i>")]
        facts[("Star schema<br/>fact_orders · fact_order_items<br/>dim_customers · dim_products<br/><i>+ is_deleted · event_ts_ms</i>")]
        history[("orders_status_history<br/><i>append-only audit trail</i>")]
        views["4 metric views<br/>open orders by date/status · top-3 dates<br/>pending items by product · top-3 customers"]
        staging -- "INSERT … ON CONFLICT<br/>DO UPDATE (idempotent)" --> facts
        facts --> views
        history --> views
    end

    feb == "JDBC write" ==> staging
    feb == "orders only:<br/>append every event" ==> history

    subgraph serving["Serving"]
        grafana["Grafana OSS<br/><i>provisioned as code · anonymous viewer<br/>10s refresh = trigger interval</i>"]
    end

    views ==> grafana
```

## Reading the diagram

- **Thick arrows** are the data path; **dotted arrows** are control/metadata
  (connector registration, checkpoint offsets).
- **Historical + current state (MUST HAVE):** the initial Debezium snapshot
  (`op=r`) flows through the same topics and code path as incremental CDC, so
  one streaming job performs both the historical load and the near-real-time
  updates. Current state lives in the star schema (upserted); the full event
  trail lives in `orders_status_history` (append-only).
- **Consistency:** checkpoints + the idempotent `ON CONFLICT` merge guarded by
  `event_ts_ms` give effectively-once semantics for the state tables — a
  replayed micro-batch produces an identical end state.
- **Startup ordering:** on a cold start the Spark app boots before Debezium has
  created the topics; the startup guard polls for topic availability instead of
  failing, and `restart: unless-stopped` covers any other crash.

Cluster sizing strategy and the Spark metrics behind these choices:
[`../docs/cluster-sizing.md`](../docs/cluster-sizing.md).