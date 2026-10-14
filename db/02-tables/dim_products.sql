-- analytics.dim_products (idempotent). PK supports ON CONFLICT upserts.

CREATE TABLE IF NOT EXISTS analytics.dim_products (
    product_id  BIGINT        NOT NULL,
    nome        VARCHAR(500)  NOT NULL,
    barcode     VARCHAR(26)   NOT NULL,
    preco       DECIMAL       NOT NULL,
    is_active   BOOLEAN,
    updated_at  TIMESTAMP(3),
    PRIMARY KEY (product_id)
);
