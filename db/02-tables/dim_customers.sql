-- analytics.dim_customers (idempotent). PK supports ON CONFLICT upserts.

CREATE TABLE IF NOT EXISTS analytics.dim_customers (
    customer_id       BIGINT   NOT NULL,
    customer_name     VARCHAR(500),
    is_active         BOOLEAN,
    customer_address  VARCHAR(500),
    is_deleted        BOOLEAN  NOT NULL DEFAULT FALSE,
    event_ts_ms       BIGINT,
    PRIMARY KEY (customer_id)
);
