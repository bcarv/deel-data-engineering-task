-- Objective: Number of open orders by DELIVERY DATE and STATUS

CREATE OR REPLACE VIEW analytics.vw_open_orders_by_delivery_date_status AS
SELECT delivery_date,
       status,
       COUNT(*) AS open_orders
  FROM analytics.fact_orders
 WHERE NOT is_deleted
   AND status NOT IN ('COMPLETED', 'CANCELLED')
GROUP BY delivery_date, status
ORDER BY delivery_date, status;