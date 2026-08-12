#!/usr/bin/env bash
# Lab cleanup for WarpStream Tableflow playground.
#
# Playground / demo accounts cannot delete tables (demo_not_allowed), which also
# blocks pipeline delete (cannot_delete_tableflow_pipeline_with_tables). Do not
# attempt those APIs here — they only add noise.
#
# This script:
#   1) pauses the pipeline
#   2) removes local Iceberg output
#   3) clears PIPELINE_ID / CONFIG_ID / VIRTUAL_CLUSTER_ID / API_KEY in .env
#
# Enough for this lab: pause + local rm + Ctrl+C playground.
# Cloud-side tables/pipeline go away when the playground cluster expires.
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
  echo "No data_lake pipeline found; skipping pause."
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

  echo "Skipping table/pipeline delete: not allowed on WarpStream playground"
  echo "(demo_not_allowed / cannot_delete_tableflow_pipeline_with_tables)."
  echo "Playground clusters expire on their own; pause + local wipe is enough."
fi

echo "Removing local Iceberg output..."
rm -rf /tmp/warpstream-tableflow-iceberg

ENV_FILE="$ROOT/.env" python3 - <<'PY'
import os
from pathlib import Path

path = Path(os.environ["ENV_FILE"])
if not path.exists():
    raise SystemExit(0)
vals = {"PIPELINE_ID", "CONFIG_ID", "VIRTUAL_CLUSTER_ID", "API_KEY"}
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
print("Cleared PIPELINE_ID, CONFIG_ID, VIRTUAL_CLUSTER_ID, and API_KEY in .env")
PY

echo "Cleanup complete (paused + local wipe). Ctrl+C warpstream playground when done."
