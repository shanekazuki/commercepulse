-- Run these queries in the catalog/schema containing the CommercePulse Gold tables.

-- Revenue and AOV by day
SELECT order_date, net_revenue, order_count, average_order_value, units_sold
FROM gold_daily_sales
ORDER BY order_date;

-- Best-selling products by net revenue
SELECT product_id, product_name, category, net_revenue, units_sold, order_count
FROM gold_product_performance
ORDER BY net_revenue DESC
LIMIT 20;

-- Repeat purchase rate and customer counts
SELECT COUNT(*) AS customers,
       SUM(CASE WHEN is_repeat_customer THEN 1 ELSE 0 END) AS repeat_customers,
       ROUND(100.0 * SUM(CASE WHEN is_repeat_customer THEN 1 ELSE 0 END) / COUNT(*), 2)
           AS repeat_customer_rate_pct
FROM gold_customer_repeat_purchase;

-- Late delivery volume and on-time rate
SELECT order_date, delivered_order_items, on_time_order_items, late_order_items,
       on_time_rate_pct, average_delay_days
FROM gold_delivery_performance
ORDER BY order_date;
