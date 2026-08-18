#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
[[ -f "$ROOT/.env" ]] && { set -a; source "$ROOT/.env"; set +a; }
: "${BASE_URL:?BASE_URL is required in .env}"
: "${API_KEY:?API_KEY is required in .env}"
: "${VIRTUAL_CLUSTER_ID:?VIRTUAL_CLUSTER_ID is required in .env}"
TABLEFLOW_BROKER_HOST="${TABLEFLOW_BROKER_HOST:-cp-server}"
TABLEFLOW_BROKER_PORT="${TABLEFLOW_BROKER_PORT:-9090}"
TABLEFLOW_BUCKET_URL="${TABLEFLOW_BUCKET_URL:-file:///tmp/tableflow-iceberg}"
WITH_DISCOUNT="${WITH_DISCOUNT:-0}"
post() { curl -sS -X POST "$1" -H "Content-Type: application/json" -H "warpstream-api-key: ${API_KEY}" -d "$2"; }
EXISTING=$(post "${BASE_URL}/api/v1/list_pipelines" "{\"virtual_cluster_id\":\"${VIRTUAL_CLUSTER_ID}\"}")
PIPELINE_ID=$(echo "$EXISTING" | jq -r '.pipelines[]? | select(.type=="data_lake") | .id' | head -n1)
if [[ -z "$PIPELINE_ID" || "$PIPELINE_ID" == "null" ]]; then
  RESP=$(post "${BASE_URL}/api/v1/create_pipeline" "{\"virtual_cluster_id\":\"${VIRTUAL_CLUSTER_ID}\",\"pipeline_name\":\"ecommerce-lakehouse\",\"pipeline_type\":\"data_lake\"}")
  PIPELINE_ID=$(echo "$RESP" | jq -r '.pipeline_id')
fi
DISCOUNT_FIELD=""
[[ "$WITH_DISCOUNT" == "1" ]] && DISCOUNT_FIELD="        string discount_code = 9;"
CONFIG_YAML=$(cat <<ENDOFYAML
source_clusters:
- name: confluent_platform
  bootstrap_brokers:
  - hostname: ${TABLEFLOW_BROKER_HOST}
    port: ${TABLEFLOW_BROKER_PORT}
destination_bucket_url: "${TABLEFLOW_BUCKET_URL}"
tables:
- source_cluster_name: confluent_platform
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
- source_cluster_name: confluent_platform
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
${DISCOUNT_FIELD}
    }
- source_cluster_name: confluent_platform
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
MESSAGE_OPENINGS=$(printf '%s\n' "$CONFIG_YAML" | grep -cE '^[[:space:]]*message[[:space:]]+[^[:space:]]+[[:space:]]*\{[[:space:]]*$' || true)
MESSAGE_CLOSINGS=$(printf '%s\n' "$CONFIG_YAML" | grep -cE '^[[:space:]]*\}[[:space:]]*$' || true)
if [[ "$MESSAGE_OPENINGS" -ne "$MESSAGE_CLOSINGS" ]]; then
  echo "Invalid inline protobuf schema: found ${MESSAGE_OPENINGS} message block(s) and ${MESSAGE_CLOSINGS} closing brace(s)." >&2
  exit 1
fi
CONFIG_JSON=$(printf '%s' "$CONFIG_YAML" | jq -Rs .)
RESP=$(post "${BASE_URL}/api/v1/create_pipeline_configuration" "{\"virtual_cluster_id\":\"${VIRTUAL_CLUSTER_ID}\",\"pipeline_id\":\"${PIPELINE_ID}\",\"configuration_yaml\":${CONFIG_JSON}}")
echo "$RESP" | jq .
CONFIG_ID=$(echo "$RESP" | jq -r '.configuration_id')
[[ -n "$CONFIG_ID" && "$CONFIG_ID" != "null" ]] || { echo "Failed to create Tableflow configuration" >&2; exit 1; }
post "${BASE_URL}/api/v1/change_pipeline_state" "{\"virtual_cluster_id\":\"${VIRTUAL_CLUSTER_ID}\",\"pipeline_id\":\"${PIPELINE_ID}\",\"desired_state\":\"running\",\"deployed_configuration_id\":\"${CONFIG_ID}\"}" | jq .
ENV_FILE="$ROOT/.env" PIPELINE_ID="$PIPELINE_ID" CONFIG_ID="$CONFIG_ID" python3 - <<'PY'
import os
from pathlib import Path
path = Path(os.environ["ENV_FILE"])
values = {"PIPELINE_ID": os.environ["PIPELINE_ID"], "CONFIG_ID": os.environ["CONFIG_ID"]}
lines, seen = [], set()
for line in path.read_text().splitlines() if path.exists() else []:
    key = line.split("=", 1)[0] if "=" in line else ""
    if key in values:
        lines.append(f"{key}={values[key]}"); seen.add(key)
    else:
        lines.append(line)
for key, value in values.items():
    if key not in seen: lines.append(f"{key}={value}")
path.write_text("\n".join(lines) + "\n")
PY
echo "Tableflow pipeline ${PIPELINE_ID} is running against ${TABLEFLOW_BROKER_HOST}:${TABLEFLOW_BROKER_PORT}."
