#!/usr/bin/env bash
# Retail DuckDB stats against local Tableflow Iceberg tables.
# Run after ./scripts/run-demo.sh (tables + metadata must already exist).
#
# Usage:
#   ./scripts/query-retail.sh
#
# Env knobs:
#   ICEBERG_DIR  Local Iceberg root (default /tmp/warpstream-tableflow-iceberg)
set -euo pipefail

export PATH="${HOME}/.duckdb/cli/latest:${PATH}"

ICEBERG_DIR="${ICEBERG_DIR:-/tmp/warpstream-tableflow-iceberg}"
TABLEFLOW_DIR="${ICEBERG_DIR}/warpstream/_tableflow"

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Missing required command: $1" >&2
    exit 1
  }
}

need duckdb

CLICKS_TABLE=$(ls -d "${TABLEFLOW_DIR}"/ecommerce_kafka__clickstream-* 2>/dev/null | head -n1 || true)
ORDERS_TABLE=$(ls -d "${TABLEFLOW_DIR}"/ecommerce_kafka__orders-* 2>/dev/null | head -n1 || true)
INVENTORY_TABLE=$(ls -d "${TABLEFLOW_DIR}"/ecommerce_kafka__inventory_updates-* 2>/dev/null | head -n1 || true)

if [[ -z "${CLICKS_TABLE}" || -z "${ORDERS_TABLE}" || -z "${INVENTORY_TABLE}" ]]; then
  echo "Could not resolve all three table dirs under ${TABLEFLOW_DIR}" >&2
  echo "Run ./scripts/run-demo.sh first and wait for Iceberg metadata." >&2
  ls -la "${TABLEFLOW_DIR}" 2>/dev/null || true
  exit 1
fi

echo "CLICKS_TABLE=${CLICKS_TABLE}"
echo "ORDERS_TABLE=${ORDERS_TABLE}"
echo "INVENTORY_TABLE=${INVENTORY_TABLE}"

run_query() {
  local title="$1"
  local sql="$2"
  echo
  echo "--- ${title} ---"
  duckdb -c "LOAD iceberg; ${sql}"
}

run_query "Order summary (GMV / AOV)" "
SELECT
  COUNT(*) AS orders,
  ROUND(SUM(total_amount), 2) AS gmv,
  ROUND(AVG(total_amount), 2) AS aov,
  ROUND(MIN(total_amount), 2) AS min_order,
  ROUND(MAX(total_amount), 2) AS max_order,
  ROUND(AVG(item_count), 2) AS avg_items_per_order
FROM iceberg_scan('${ORDERS_TABLE}');
"

run_query "Revenue by payment method" "
SELECT payment_method,
  COUNT(*) AS order_count,
  ROUND(SUM(total_amount), 2) AS total_revenue,
  ROUND(AVG(total_amount), 2) AS avg_order_value
FROM iceberg_scan('${ORDERS_TABLE}')
GROUP BY payment_method
ORDER BY total_revenue DESC;
"

run_query "Fulfillment mix (orders by status)" "
SELECT status,
  COUNT(*) AS order_count,
  ROUND(SUM(total_amount), 2) AS revenue,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_of_orders
FROM iceberg_scan('${ORDERS_TABLE}')
GROUP BY status
ORDER BY order_count DESC;
"

run_query "Top pages / event types (clickstream)" "
SELECT page_url, event_type, COUNT(*) AS events
FROM iceberg_scan('${CLICKS_TABLE}')
GROUP BY page_url, event_type
ORDER BY events DESC
LIMIT 12;
"

run_query "Traffic sources (referrer)" "
SELECT
  CASE WHEN referrer = '' OR referrer IS NULL THEN '(direct)' ELSE referrer END AS referrer,
  COUNT(*) AS events,
  COUNT(DISTINCT user_id) AS unique_users
FROM iceberg_scan('${CLICKS_TABLE}')
GROUP BY 1
ORDER BY events DESC;
"

run_query "Low-stock SKUs (latest inventory per sku/warehouse)" "
WITH latest AS (
  SELECT sku, warehouse_id, quantity_after, updated_at_ms,
    ROW_NUMBER() OVER (PARTITION BY sku, warehouse_id ORDER BY updated_at_ms DESC) AS rn
  FROM iceberg_scan('${INVENTORY_TABLE}')
)
SELECT sku, warehouse_id, quantity_after AS current_stock
FROM latest
WHERE rn = 1
ORDER BY current_stock ASC
LIMIT 15;
"

run_query "Click → purchase attribution (confirmed orders)" "
WITH clicks AS (
  SELECT * FROM iceberg_scan('${CLICKS_TABLE}')
),
purchases AS (
  SELECT * FROM iceberg_scan('${ORDERS_TABLE}')
  WHERE status = 'confirmed'
)
SELECT c.page_url,
  COUNT(DISTINCT p.order_id) AS attributed_orders,
  ROUND(SUM(p.total_amount), 2) AS attributed_revenue
FROM clicks c
JOIN purchases p
  ON c.user_id = p.customer_id
 AND c.timestamp_ms <= p.created_at_ms
GROUP BY c.page_url
ORDER BY attributed_revenue DESC
LIMIT 10;
"

run_query "Top customers (spend + engagement)" "
WITH clicks AS (
  SELECT user_id, COUNT(*) AS page_views
  FROM iceberg_scan('${CLICKS_TABLE}')
  GROUP BY user_id
),
purchases AS (
  SELECT customer_id AS user_id,
    COUNT(*) AS orders,
    ROUND(SUM(total_amount), 2) AS total_spent
  FROM iceberg_scan('${ORDERS_TABLE}')
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
"
