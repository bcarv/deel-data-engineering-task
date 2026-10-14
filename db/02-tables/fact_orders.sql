-- analytics.fact_orders (idempotent). PK supports ON CONFLICT upserts. No physical FKs.

CREATE TABLE IF NOT EXISTS analytics.fact_orders (
    order_id       BIGINT   NOT NULL,
    order_date     DATE,
    delivery_date  DATE,
    customer_id    BIGINT,
    status         VARCHAR,
    is_deleted     BOOLEAN  NOT NULL DEFAULT FALSE,
    event_ts_ms    BIGINT,
    PRIMARY KEY (order_id)
);
