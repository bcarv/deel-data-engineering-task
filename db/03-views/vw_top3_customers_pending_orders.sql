-- Objective: Top 3 Customers with more pending orders

CREATE OR REPLACE VIEW analytics.vw_top3_customers_pending_orders AS
SELECT
    orders.customer_id,
    customers.customer_name,
    COUNT(*) AS pending_orders
FROM analytics.fact_orders AS orders
LEFT JOIN analytics.dim_customers AS customers
  ON customers.customer_id = orders.customer_id
WHERE NOT orders.is_deleted
  AND orders.status NOT IN ('COMPLETED', 'CANCELLED')
GROUP BY orders.customer_id, customers.customer_name
ORDER BY pending_orders DESC
LIMIT 3;