#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/iceberg-query-common.sh"
[[ -f "$ROOT/.env" ]] && { set -a; source "$ROOT/.env"; set +a; }
BROKER="${BROKER:-localhost:19090}"
PYTHON="$ROOT/.venv/bin/python"
[[ -x "$PYTHON" ]] || PYTHON=python3
orders=$(find_table 'confluent_platform__orders' || true)
[[ -n "$orders" ]] || { echo "Run run-demo.sh first." >&2; exit 1; }

for i in $(seq 1 "${MAX_ATTEMPTS:-150}"); do
  orders_meta=$(find_complete_metadata "$orders" || true)
  if [[ -n "$orders_meta" ]]; then
    orders_i=$(iceberg_path "$orders_meta")
    set +e
    row=$(docker run --rm -i -v "$ROOT/iceberg:/iceberg:ro" -v "$ROOT/iceberg:/tmp/tableflow-iceberg:ro" -w /iceberg duckdb/duckdb:latest -csv -noheader -c "INSTALL iceberg; LOAD iceberg; SELECT COUNT(*), COUNT(discount_code) FROM iceberg_scan('$orders_i');" 2>/dev/null)
    status=$?
    set -e
    if [[ $status -eq 0 && "${row##*,}" =~ ^[0-9]+$ ]] && (( ${row##*,} > 0 )); then
      echo "discount_code is visible after ${i} polls"
      exit 0
    fi
  fi
  if (( i == 3 )); then
    "$PYTHON" "$ROOT/scripts/produce-protobuf.py" --broker "$BROKER" --count "${EXTRA_PRODUCE_COUNT:-30}" --with-discount
  fi
  sleep "${INTERVAL_SECS:-5}"
done

echo "Timed out waiting for discount_code." >&2
print_incomplete_snapshot_help
exit 1
