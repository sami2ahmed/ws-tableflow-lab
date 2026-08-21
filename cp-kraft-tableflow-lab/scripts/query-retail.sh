#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/iceberg-query-common.sh"

wait_for_table_root
clicks=$(find_table 'confluent_platform__clickstream' || true)
orders=$(find_table 'confluent_platform__orders' || true)
attempts="$ICEBERG_QUERY_ATTEMPTS"
interval="$ICEBERG_QUERY_INTERVAL_SECS"
last_output=""

for i in $(seq 1 "$attempts"); do
  if [[ -n "$clicks" && -n "$orders" ]]; then
    clicks_meta=$(find_complete_metadata "$clicks" || true)
    orders_meta=$(find_complete_metadata "$orders" || true)
    if [[ -n "$clicks_meta" && -n "$orders_meta" ]]; then
      clicks_i=$(iceberg_path "$clicks_meta")
      orders_i=$(iceberg_path "$orders_meta")
      sql="INSTALL iceberg; LOAD iceberg; SELECT COUNT(*) AS orders, ROUND(SUM(total_amount),2) AS gmv, ROUND(AVG(total_amount),2) AS aov FROM iceberg_scan('$orders_i'); SELECT payment_method, COUNT(*) AS order_count, ROUND(SUM(total_amount),2) AS revenue FROM iceberg_scan('$orders_i') GROUP BY payment_method ORDER BY revenue DESC; SELECT page_url, event_type, COUNT(*) AS events FROM iceberg_scan('$clicks_i') GROUP BY page_url, event_type ORDER BY events DESC LIMIT 12;"
      if output=$(duckdb_query "$sql" 2>&1); then
        printf '%s\n' "$output"
        exit 0
      fi
      last_output="$output"
    fi
  fi
  echo "DuckDB found no complete readable Iceberg snapshot yet (${i}/${attempts}); waiting..." >&2
  sleep "$interval"
done
[[ -z "$last_output" ]] || printf '%s\n' "$last_output" >&2
print_incomplete_snapshot_help
exit 1
