-- analytics.dim_customers (idempotent). PK supports ON CONFLICT upserts.

CREATE TABLE IF NOT EXISTS analytics.dim_customers (
    customer_id  BIGINT        NOT NULL,
    nome         VARCHAR(500)  NOT NULL,
    is_active    BOOLEAN       NOT NULL DEFAULT TRUE,
    endereco     VARCHAR(500),
    updated_at   TIMESTAMP(3),
    PRIMARY KEY (customer_id)
);
