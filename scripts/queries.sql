-- DuckDB queries for the WarpStream Tableflow ecommerce lab.
-- Update table paths if your session UUIDs differ:
--   ls /tmp/warpstream-tableflow-iceberg/warpstream/_tableflow/
--
-- In the DuckDB CLI: INSTALL iceberg; LOAD iceberg;

-- Session table paths (replace if needed)
-- clickstream: ecommerce_kafka__clickstream-cd3e99a7-da9e-40ab-b9b0-75c785d8655b
-- orders:      ecommerce_kafka__orders-a0a2a76f-74a7-4e21-a233-ba72f9038039
-- inventory:   ecommerce_kafka__inventory_updates-3df8adbf-6147-4833-aed8-1619d9cd3496

-- Query 1: row counts
SELECT 'clickstream' AS tbl, COUNT(*) AS rows
FROM iceberg_scan('/tmp/warpstream-tableflow-iceberg/warpstream/_tableflow/ecommerce_kafka__clickstream-cd3e99a7-da9e-40ab-b9b0-75c785d8655b')
UNION ALL
SELECT 'orders', COUNT(*)
FROM iceberg_scan('/tmp/warpstream-tableflow-iceberg/warpstream/_tableflow/ecommerce_kafka__orders-a0a2a76f-74a7-4e21-a233-ba72f9038039')
UNION ALL
SELECT 'inventory', COUNT(*)
FROM iceberg_scan('/tmp/warpstream-tableflow-iceberg/warpstream/_tableflow/ecommerce_kafka__inventory_updates-3df8adbf-6147-4833-aed8-1619d9cd3496');

-- Query 2: revenue by payment method
SELECT payment_method, COUNT(*) AS order_count,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    ROUND(AVG(total_amount), 2) AS avg_order_value
FROM iceberg_scan('/tmp/warpstream-tableflow-iceberg/warpstream/_tableflow/ecommerce_kafka__orders-a0a2a76f-74a7-4e21-a233-ba72f9038039')
GROUP BY payment_method
ORDER BY total_revenue DESC;

-- Query 3: clickstream → orders attribution
WITH clicks AS (
    SELECT * FROM iceberg_scan('/tmp/warpstream-tableflow-iceberg/warpstream/_tableflow/ecommerce_kafka__clickstream-cd3e99a7-da9e-40ab-b9b0-75c785d8655b')
),
purchases AS (
    SELECT * FROM iceberg_scan('/tmp/warpstream-tableflow-iceberg/warpstream/_tableflow/ecommerce_kafka__orders-a0a2a76f-74a7-4e21-a233-ba72f9038039')
    WHERE status = 'confirmed'
)
SELECT c.page_url,
    COUNT(DISTINCT p.order_id) AS attributed_orders,
    ROUND(SUM(p.total_amount), 2) AS attributed_revenue
FROM clicks c
JOIN purchases p ON c.user_id = p.customer_id AND c.timestamp_ms <= p.created_at_ms
GROUP BY c.page_url
ORDER BY attributed_revenue DESC
LIMIT 10;

-- Query 4: inventory snapshot
WITH latest AS (
    SELECT sku, warehouse_id, quantity_after, updated_at_ms,
        ROW_NUMBER() OVER (PARTITION BY sku, warehouse_id ORDER BY updated_at_ms DESC) AS rn
    FROM iceberg_scan('/tmp/warpstream-tableflow-iceberg/warpstream/_tableflow/ecommerce_kafka__inventory_updates-3df8adbf-6147-4833-aed8-1619d9cd3496')
)
SELECT sku, warehouse_id, quantity_after AS current_stock
FROM latest WHERE rn = 1
ORDER BY current_stock ASC
LIMIT 15;

-- Query 5: unified customer view
WITH clicks AS (
    SELECT user_id, COUNT(*) AS page_views
    FROM iceberg_scan('/tmp/warpstream-tableflow-iceberg/warpstream/_tableflow/ecommerce_kafka__clickstream-cd3e99a7-da9e-40ab-b9b0-75c785d8655b')
    GROUP BY user_id
),
purchases AS (
    SELECT customer_id AS user_id, COUNT(*) AS orders, ROUND(SUM(total_amount), 2) AS total_spent
    FROM iceberg_scan('/tmp/warpstream-tableflow-iceberg/warpstream/_tableflow/ecommerce_kafka__orders-a0a2a76f-74a7-4e21-a233-ba72f9038039')
    GROUP BY customer_id
)
SELECT COALESCE(c.user_id, p.user_id) AS user_id,
    COALESCE(c.page_views, 0) AS page_views,
    COALESCE(p.orders, 0) AS orders,
    COALESCE(p.total_spent, 0) AS total_spent,
    CASE WHEN p.orders > 0 THEN ROUND(p.total_spent / p.orders, 2) ELSE 0 END AS avg_order_value
FROM clicks c
FULL OUTER JOIN purchases p ON c.user_id = p.user_id
ORDER BY total_spent DESC
LIMIT 15;
