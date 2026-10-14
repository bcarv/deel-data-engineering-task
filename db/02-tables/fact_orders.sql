-- analytics.fact_orders (idempotent). PK supports ON CONFLICT upserts. No physical FKs.

CREATE TABLE IF NOT EXISTS analytics.fact_orders (
    order_id        BIGINT        NOT NULL,
    customer_id     BIGINT,
    order_date      DATE,
    delivery_date   DATE,
    status          VARCHAR,
    updated_at      TIMESTAMP(3),
    PRIMARY KEY (order_id)
);
