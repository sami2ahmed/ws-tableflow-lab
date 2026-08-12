#!/usr/bin/env bash
# Run the base Tableflow demo through DuckDB row counts (steps 0–4).
# Extra retail queries: ./scripts/query-retail.sh
# Schema evolution: ./scripts/evolve-schema.sh then ./scripts/poll-discount.sh
#
# Prerequisites: warpstream playground running, .env filled, .venv + duckdb installed.
#
# Usage:
#   ./scripts/run-demo.sh
#
# Env knobs:
#   BROKER              Kafka broker (default localhost:9092)
#   ICEBERG_DIR         Local Iceberg root (default /tmp/warpstream-tableflow-iceberg)
#   SKIP_CLEAN          If 1, do not wipe ICEBERG_DIR before starting
#   WAIT_INTERVAL_SECS  Poll interval for Parquet/metadata (default 5)
#   WAIT_MAX_ATTEMPTS   Give up after N polls (default 40 ≈ 10 minutes)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${ROOT}"

if [[ -f "${ROOT}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${ROOT}/.env"
  set +a
fi

# shellcheck disable=SC1091
[[ -f "${ROOT}/.venv/bin/activate" ]] && source "${ROOT}/.venv/bin/activate"

export PATH="${HOME}/.duckdb/cli/latest:${PATH}"

BROKER="${BROKER:-localhost:9092}"
ICEBERG_DIR="${ICEBERG_DIR:-/tmp/warpstream-tableflow-iceberg}"
TABLEFLOW_DIR="${ICEBERG_DIR}/warpstream/_tableflow"
SKIP_CLEAN="${SKIP_CLEAN:-0}"
WAIT_INTERVAL_SECS="${WAIT_INTERVAL_SECS:-5}"
WAIT_MAX_ATTEMPTS="${WAIT_MAX_ATTEMPTS:-40}"

if [[ -x "${ROOT}/.venv/bin/python" ]]; then
  PYTHON="${ROOT}/.venv/bin/python"
else
  PYTHON="python3"
fi

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Missing required command: $1" >&2
    exit 1
  }
}

need curl
need jq
need duckdb
need "${PYTHON}"

: "${BASE_URL:?BASE_URL is required (set in .env)}"
: "${API_KEY:?API_KEY is required (set in .env)}"
: "${VIRTUAL_CLUSTER_ID:?VIRTUAL_CLUSTER_ID is required (set in .env)}"

step() {
  echo
  echo "=== $* ==="
}

wait_for() {
  local label="$1"
  local check_cmd="$2"
  local i
  for ((i = 1; i <= WAIT_MAX_ATTEMPTS; i++)); do
    if eval "${check_cmd}"; then
      echo "Ready: ${label} (attempt ${i})"
      return 0
    fi
    echo "[${i}/${WAIT_MAX_ATTEMPTS}] waiting for ${label}..."
    sleep "${WAIT_INTERVAL_SECS}"
  done
  echo "Timed out waiting for ${label} after ~$((WAIT_MAX_ATTEMPTS * WAIT_INTERVAL_SECS / 60)) minutes." >&2
  exit 1
}

resolve_tables() {
  CLICKS_TABLE=$(ls -d "${TABLEFLOW_DIR}"/ecommerce_kafka__clickstream-* 2>/dev/null | head -n1 || true)
  ORDERS_TABLE=$(ls -d "${TABLEFLOW_DIR}"/ecommerce_kafka__orders-* 2>/dev/null | head -n1 || true)
  INVENTORY_TABLE=$(ls -d "${TABLEFLOW_DIR}"/ecommerce_kafka__inventory_updates-* 2>/dev/null | head -n1 || true)
}

tables_ready() {
  resolve_tables
  [[ -n "${CLICKS_TABLE}" && -n "${ORDERS_TABLE}" && -n "${INVENTORY_TABLE}" ]] || return 1
  [[ -n "$(find "${CLICKS_TABLE}" -name "v*.metadata.json" 2>/dev/null | head -n1)" ]] || return 1
  [[ -n "$(find "${ORDERS_TABLE}" -name "v*.metadata.json" 2>/dev/null | head -n1)" ]] || return 1
  [[ -n "$(find "${INVENTORY_TABLE}" -name "v*.metadata.json" 2>/dev/null | head -n1)" ]] || return 1
  return 0
}

run_query() {
  local title="$1"
  local sql="$2"
  echo
  echo "--- ${title} ---"
  duckdb -c "LOAD iceberg; ${sql}"
}

# --- 0) Clean local Iceberg dir ---
if [[ "${SKIP_CLEAN}" != "1" ]]; then
  step "0) Clean ${ICEBERG_DIR}"
  rm -rf "${ICEBERG_DIR}"
  mkdir -p "${ICEBERG_DIR}"
else
  step "0) SKIP_CLEAN=1 — keeping ${ICEBERG_DIR}"
  mkdir -p "${ICEBERG_DIR}"
fi

# --- 1) Seed Kafka ---
step "1) Produce 200 msgs × 3 topics (no discount)"
"${PYTHON}" scripts/produce-protobuf.py --broker "${BROKER}" --count 200

# --- 2) Configure Tableflow ---
step "2) Configure / start Tableflow pipeline"
./scripts/configure-tableflow.sh

# --- 3) Wait for Parquet, then Iceberg metadata ---
step "3) Wait for Parquet files"
wait_for "Parquet under ${ICEBERG_DIR}" \
  '[[ "$(find "${ICEBERG_DIR}" -name "*.parquet" 2>/dev/null | wc -l | tr -d " ")" -ge 1 ]]'

echo "Parquet count: $(find "${ICEBERG_DIR}" -name "*.parquet" | wc -l | tr -d ' ')"
ls "${TABLEFLOW_DIR}" 2>/dev/null || true

step "3b) Wait for Iceberg metadata (v*.metadata.json) on all three tables"
wait_for "Iceberg metadata for clickstream/orders/inventory" 'tables_ready'

resolve_tables
if [[ -z "${CLICKS_TABLE}" || -z "${ORDERS_TABLE}" || -z "${INVENTORY_TABLE}" ]]; then
  echo "Could not resolve all three table dirs under ${TABLEFLOW_DIR}" >&2
  ls -la "${TABLEFLOW_DIR}" 2>/dev/null || true
  exit 1
fi
echo "CLICKS_TABLE=${CLICKS_TABLE}"
echo "ORDERS_TABLE=${ORDERS_TABLE}"
echo "INVENTORY_TABLE=${INVENTORY_TABLE}"

# --- 4) Query with DuckDB ---
step "4) Query Iceberg row counts with DuckDB"

run_query "Table row counts" "
SELECT 'clickstream' AS tbl, COUNT(*) AS rows FROM iceberg_scan('${CLICKS_TABLE}')
UNION ALL SELECT 'orders', COUNT(*) FROM iceberg_scan('${ORDERS_TABLE}')
UNION ALL SELECT 'inventory', COUNT(*) FROM iceberg_scan('${INVENTORY_TABLE}');
"

step "Done (steps 0–4)"
