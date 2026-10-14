-- Objective: Number of open pending items by PRODUCT_ID. This information can be queried using the order status and the order items

CREATE OR REPLACE VIEW analytics.vw_pending_items_by_product AS
SELECT order_items.product_id,
       products.product_name,
       SUM(order_items.quantity) AS pending_items
  FROM analytics.fact_order_items AS order_items
       JOIN analytics.fact_orders AS orders ON orders.order_id = order_items.order_id
       LEFT JOIN analytics.dim_products AS products ON products.product_id = order_items.product_id
 WHERE NOT orders.is_deleted
   AND NOT order_items.is_deleted
   AND orders.status NOT IN ('COMPLETED', 'CANCELLED')
GROUP BY order_items.product_id, products.product_name
ORDER BY pending_items DESC;