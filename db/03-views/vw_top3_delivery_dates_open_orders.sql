-- Objective: Top 3 delivery dates with more open orders

CREATE OR REPLACE VIEW analytics.vw_top3_delivery_dates_open_orders AS
SELECT
    delivery_date,
    COUNT(*) AS open_orders
FROM analytics.fact_orders
WHERE NOT is_deleted
  AND status NOT IN ('COMPLETED', 'CANCELLED')
GROUP BY delivery_date
ORDER BY open_orders DESC, delivery_date
LIMIT 3;