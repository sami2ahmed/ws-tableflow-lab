#!/usr/bin/env bash
# Pause pipeline, delete Tableflow tables, delete pipeline, remove local Iceberg output.
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

if [[ -z "${PIPELINE_ID:-}" ]]; then
  PIPELINE_ID=$(curl -s -X POST "${BASE_URL}/api/v1/list_pipelines" \
    -H "Content-Type: application/json" \
    -H "warpstream-api-key: ${API_KEY}" \
    -d "{\"virtual_cluster_id\":\"${VIRTUAL_CLUSTER_ID}\"}" \
    | jq -r '.pipelines[]? | select(.type=="data_lake") | .id' | head -n1)
fi

if [[ -z "${PIPELINE_ID}" || "${PIPELINE_ID}" == "null" ]]; then
  echo "No data_lake pipeline found; nothing to delete."
else
  echo "Pausing pipeline $PIPELINE_ID..."
  curl -s -X POST "${BASE_URL}/api/v1/change_pipeline_state" \
    -H "Content-Type: application/json" \
    -H "warpstream-api-key: ${API_KEY}" \
    -d "{
      \"virtual_cluster_id\":\"${VIRTUAL_CLUSTER_ID}\",
      \"pipeline_id\":\"${PIPELINE_ID}\",
      \"desired_state\":\"paused\"
    }" | jq .

  echo "Listing tables..."
  TABLES=$(curl -s -X POST "${BASE_URL}/api/v1/dl/list_tables" \
    -H "Content-Type: application/json" \
    -H "warpstream-api-key: ${API_KEY}" \
    -d "{\"virtual_cluster_id\":\"${VIRTUAL_CLUSTER_ID}\"}")
  echo "$TABLES" | jq .

  echo "$TABLES" | jq -r '.tables[]?.table_uuid // empty' | while read -r TABLE_UUID; do
    [[ -z "$TABLE_UUID" ]] && continue
    echo "Deleting table $TABLE_UUID..."
    curl -s -X POST "${BASE_URL}/api/v1/dl/delete_table" \
      -H "Content-Type: application/json" \
      -H "warpstream-api-key: ${API_KEY}" \
      -d "{
        \"virtual_cluster_id\":\"${VIRTUAL_CLUSTER_ID}\",
        \"table_uuid\":\"${TABLE_UUID}\"
      }" | jq .
  done

  echo "Deleting pipeline $PIPELINE_ID..."
  curl -s -X POST "${BASE_URL}/api/v1/delete_pipeline" \
    -H "Content-Type: application/json" \
    -H "warpstream-api-key: ${API_KEY}" \
    -d "{
      \"virtual_cluster_id\":\"${VIRTUAL_CLUSTER_ID}\",
      \"pipeline_id\":\"${PIPELINE_ID}\"
    }" | jq .
fi

echo "Removing local Iceberg output..."
rm -rf /tmp/warpstream-tableflow-iceberg

ENV_FILE="$ROOT/.env" python3 - <<'PY'
import os
from pathlib import Path

path = Path(os.environ["ENV_FILE"])
if not path.exists():
    raise SystemExit(0)
vals = {"PIPELINE_ID", "CONFIG_ID"}
lines = []
seen = set()
for line in path.read_text().splitlines():
    key = line.split("=", 1)[0] if "=" in line else None
    if key in vals:
        lines.append(f"{key}=")
        seen.add(key)
    else:
        lines.append(line)
for key in vals:
    if key not in seen:
        lines.append(f"{key}=")
path.write_text("\n".join(lines) + "\n")
print("Cleared PIPELINE_ID and CONFIG_ID in .env")
PY

echo "Cleanup complete."
