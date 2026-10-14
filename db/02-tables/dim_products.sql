-- analytics.dim_products (idempotent). PK supports ON CONFLICT upserts.

CREATE TABLE IF NOT EXISTS analytics.dim_products (
    product_id    BIGINT   NOT NULL,
    product_name  VARCHAR(500),
    barcode       VARCHAR(100),
    unity_price   DECIMAL(10,2),
    is_active     BOOLEAN,
    is_deleted    BOOLEAN  NOT NULL DEFAULT FALSE,
    event_ts_ms   BIGINT,
    PRIMARY KEY (product_id)
);
