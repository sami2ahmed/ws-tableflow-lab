#!/usr/bin/env bash
# Evolve orders schema: add optional discount_code (proto field 9).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
if [[ -f "$ROOT/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$ROOT/.env"
  set +a
fi

: "${BASE_URL:?BASE_URL is required}"
: "${API_KEY:?API_KEY is required}"
: "${VIRTUAL_CLUSTER_ID:?VIRTUAL_CLUSTER_ID is required}"
: "${PIPELINE_ID:?PIPELINE_ID is required}"

echo "Pausing pipeline $PIPELINE_ID..."
curl -s -X POST "${BASE_URL}/api/v1/change_pipeline_state" \
  -H "Content-Type: application/json" \
  -H "warpstream-api-key: ${API_KEY}" \
  -d "{
    \"virtual_cluster_id\":\"${VIRTUAL_CLUSTER_ID}\",
    \"pipeline_id\":\"${PIPELINE_ID}\",
    \"desired_state\":\"paused\"
  }" | jq .

CONFIG_YAML=$(cat <<'ENDOFYAML'
source_clusters:
  - name: ecommerce_kafka
    bootstrap_brokers:
      - hostname: localhost
        port: 9092
destination_bucket_url: "file:///tmp/warpstream-tableflow-iceberg"
tables:
  - source_cluster_name: ecommerce_kafka
    source_topic: clickstream
    source_format: protobuf
    wire_format: raw
    schema_mode: inline
    partitioning_scheme: hour
    compression: zstd
    dlq_mode: skip
    input_schema: |
      syntax = "proto3";
      package ecommerce;
      message ClickEvent {
        string event_id = 1;
        string user_id = 2;
        string session_id = 3;
        string page_url = 4;
        string event_type = 5;
        string referrer = 6;
        int64 timestamp_ms = 7;
      }
  - source_cluster_name: ecommerce_kafka
    source_topic: orders
    source_format: protobuf
    wire_format: raw
    schema_mode: inline
    partitioning_scheme: hour
    compression: zstd
    dlq_mode: skip
    input_schema: |
      syntax = "proto3";
      package ecommerce;
      message OrderEvent {
        string order_id = 1;
        string customer_id = 2;
        string status = 3;
        double total_amount = 4;
        string currency = 5;
        string payment_method = 6;
        int32 item_count = 7;
        int64 created_at_ms = 8;
        string discount_code = 9;
      }
  - source_cluster_name: ecommerce_kafka
    source_topic: inventory_updates
    source_format: protobuf
    wire_format: raw
    schema_mode: inline
    partitioning_scheme: unpartitioned
    compression: zstd
    dlq_mode: skip
    input_schema: |
      syntax = "proto3";
      package ecommerce;
      message InventoryUpdate {
        string sku = 1;
        string warehouse_id = 2;
        int32 quantity_change = 3;
        int32 quantity_after = 4;
        string update_type = 5;
        int64 updated_at_ms = 6;
      }
ENDOFYAML
)

CONFIG_JSON=$(printf '%s' "$CONFIG_YAML" | jq -Rs .)

echo "Creating evolved configuration..."
CONFIG_RESP=$(curl -s -X POST "${BASE_URL}/api/v1/create_pipeline_configuration" \
  -H "Content-Type: application/json" \
  -H "warpstream-api-key: ${API_KEY}" \
  -d "{
    \"virtual_cluster_id\":\"${VIRTUAL_CLUSTER_ID}\",
    \"pipeline_id\":\"${PIPELINE_ID}\",
    \"configuration_yaml\": $CONFIG_JSON
  }")

echo "$CONFIG_RESP" | jq .
CONFIG_ID=$(echo "$CONFIG_RESP" | jq -r '.configuration_id')
if [[ -z "${CONFIG_ID}" || "${CONFIG_ID}" == "null" ]]; then
  echo "Failed to create evolved configuration" >&2
  exit 1
fi
echo "New Configuration ID: $CONFIG_ID"

echo "Resuming pipeline with evolved config..."
curl -s -X POST "${BASE_URL}/api/v1/change_pipeline_state" \
  -H "Content-Type: application/json" \
  -H "warpstream-api-key: ${API_KEY}" \
  -d "{
    \"virtual_cluster_id\":\"${VIRTUAL_CLUSTER_ID}\",
    \"pipeline_id\":\"${PIPELINE_ID}\",
    \"desired_state\":\"running\",
    \"deployed_configuration_id\":\"${CONFIG_ID}\"
  }" | jq .

curl -s -X POST "${BASE_URL}/api/v1/describe_pipeline" \
  -H "Content-Type: application/json" \
  -H "warpstream-api-key: ${API_KEY}" \
  -d "{\"virtual_cluster_id\":\"${VIRTUAL_CLUSTER_ID}\",\"pipeline_id\":\"${PIPELINE_ID}\"}" \
  | jq .pipeline_overview

ENV_FILE="$ROOT/.env" PIPELINE_ID="$PIPELINE_ID" CONFIG_ID="$CONFIG_ID" python3 - <<'PY'
import os
from pathlib import Path

path = Path(os.environ["ENV_FILE"])
vals = {
    "PIPELINE_ID": os.environ["PIPELINE_ID"],
    "CONFIG_ID": os.environ["CONFIG_ID"],
}
text = path.read_text() if path.exists() else ""
lines = []
seen = set()
for line in text.splitlines():
    key = line.split("=", 1)[0] if "=" in line else None
    if key in vals:
        lines.append(f"{key}={vals[key]}")
        seen.add(key)
    else:
        lines.append(line)
for key, value in vals.items():
    if key not in seen:
        lines.append(f"{key}={value}")
path.write_text("\n".join(lines) + "\n")
print("Updated CONFIG_ID in .env")
PY
