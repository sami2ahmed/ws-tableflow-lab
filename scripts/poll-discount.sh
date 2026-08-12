#!/usr/bin/env bash
# Poll the orders Iceberg table until discount_code has non-null values.
#
# Usage (after evolve-schema.sh + produce --with-discount):
#   export PATH="$HOME/.duckdb/cli/latest:$PATH"
#   ./scripts/poll-discount.sh
#
# Options via env:
#   INTERVAL_SECS     poll interval (default 5)
#   MAX_ATTEMPTS      give up after N polls (default 150 ≈ 12.5 minutes at 5s)
#   EXTRA_PRODUCE_AT  attempt to kick an optional --count 30 batch (default 3)
#   EXTRA_PRODUCE_COUNT  size of that optional batch (default 30)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export PATH="${HOME}/.duckdb/cli/latest:${PATH}"

INTERVAL_SECS="${INTERVAL_SECS:-5}"
MAX_ATTEMPTS="${MAX_ATTEMPTS:-150}"
EXTRA_PRODUCE_AT="${EXTRA_PRODUCE_AT:-3}"
EXTRA_PRODUCE_COUNT="${EXTRA_PRODUCE_COUNT:-30}"

ORDERS_TABLE=$(ls -d /tmp/warpstream-tableflow-iceberg/warpstream/_tableflow/ecommerce_kafka__orders-* 2>/dev/null | head -n1 || true)
if [[ -z "${ORDERS_TABLE}" ]]; then
  echo "No orders table under /tmp/warpstream-tableflow-iceberg/warpstream/_tableflow/" >&2
  exit 1
fi

echo "Polling discount_code on: ${ORDERS_TABLE}"
echo "Interval=${INTERVAL_SECS}s max_attempts=${MAX_ATTEMPTS} (extra produce at attempt ${EXTRA_PRODUCE_AT})"
echo "Note: iceberg_scan waits on Tableflow snapshot commit; Parquet may show discount_code earlier."

parquet_hint() {
  # Best-effort: confirm discounted rows landed in Parquet while Iceberg catches up.
  set +e
  PQ=$(duckdb -csv -c "
SELECT
  COUNT(*) AS parquet_rows,
  COUNT(discount_code) AS parquet_with_discount
FROM read_parquet('${ORDERS_TABLE}/data/*.parquet', union_by_name=true);
" 2>&1)
  local st=$?
  set -e
  if [[ ${st} -ne 0 ]]; then
    echo "       parquet: not readable yet"
    return
  fi
  local data
  data=$(echo "${PQ}" | tail -n 1)
  echo "       parquet: rows=$(echo "${data}" | cut -d, -f1) with_discount=$(echo "${data}" | cut -d, -f2)"
}

for ((i = 1; i <= MAX_ATTEMPTS; i++)); do
  HINT="?"
  if [[ -f "${ORDERS_TABLE}/metadata/version-hint.text" ]]; then
    HINT=$(tr -d '[:space:]' <"${ORDERS_TABLE}/metadata/version-hint.text")
  fi

  # CSV is easy to parse; column may be missing until schema metadata lands.
  set +e
  CSV=$(duckdb -csv -c "
LOAD iceberg;
SELECT
  COUNT(*) AS total_orders,
  COUNT(discount_code) AS with_discount,
  COUNT(*) - COUNT(discount_code) AS null_discount
FROM iceberg_scan('${ORDERS_TABLE}');
" 2>&1)
  STATUS=$?
  set -e

  if [[ ${STATUS} -ne 0 ]]; then
    echo "[${i}] version-hint=${HINT}  discount_code not queryable yet (schema metadata still catching up)"
    echo "       ${CSV}" | head -n 3
    parquet_hint
  else
    # header + one data row
    DATA=$(echo "${CSV}" | tail -n 1)
    TOTAL=$(echo "${DATA}" | cut -d, -f1)
    WITH_D=$(echo "${DATA}" | cut -d, -f2)
    NULL_D=$(echo "${DATA}" | cut -d, -f3)
    echo "[${i}] version-hint=${HINT}  iceberg: total=${TOTAL} with_discount=${WITH_D} null_discount=${NULL_D}"

    if [[ "${WITH_D}" =~ ^[0-9]+$ ]] && (( WITH_D > 0 )); then
      echo "Non-null discount_code visible in iceberg_scan."
      duckdb -c "
LOAD iceberg;
SELECT order_id, customer_id, total_amount, discount_code
FROM iceberg_scan('${ORDERS_TABLE}')
WHERE discount_code IS NOT NULL
ORDER BY created_at_ms DESC
LIMIT 8;
"
      exit 0
    fi
    parquet_hint
  fi

  if (( i == EXTRA_PRODUCE_AT )); then
    if [[ -x "${ROOT}/.venv/bin/python" ]]; then
      PYTHON="${ROOT}/.venv/bin/python"
    else
      PYTHON="python3"
    fi
    echo "Still waiting — producing optional extra batch (--count ${EXTRA_PRODUCE_COUNT} --with-discount)"
    (
      cd "${ROOT}"
      # shellcheck disable=SC1091
      [[ -f .venv/bin/activate ]] && source .venv/bin/activate
      "${PYTHON}" scripts/produce-protobuf.py --broker localhost:9092 --count "${EXTRA_PRODUCE_COUNT}" --with-discount
    )
  fi

  if (( i < MAX_ATTEMPTS )); then
    sleep "${INTERVAL_SECS}"
  fi
done

echo "Timed out after ${MAX_ATTEMPTS} attempts (~$((MAX_ATTEMPTS * INTERVAL_SECS / 60)) minutes)." >&2
echo "Parquet may already have discount_code; Iceberg metadata on playground can lag several minutes." >&2
exit 1
