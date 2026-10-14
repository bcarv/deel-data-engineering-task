-- analytics.fact_order_items (idempotent). PK supports ON CONFLICT upserts. No physical FKs.

CREATE TABLE IF NOT EXISTS analytics.fact_order_items (
    order_item_id  BIGINT   NOT NULL,
    order_id       BIGINT,
    product_id     BIGINT,
    quantity       INTEGER,
    is_deleted     BOOLEAN  NOT NULL DEFAULT FALSE,
    event_ts_ms    BIGINT,
    PRIMARY KEY (order_item_id)
);
