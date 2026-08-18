#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
[[ -f .env ]] && { set -a; source .env; set +a; }
if [[ -n "${BASE_URL:-}" && -n "${API_KEY:-}" && -n "${VIRTUAL_CLUSTER_ID:-}" && -n "${PIPELINE_ID:-}" ]]; then
  curl -sS -X POST "${BASE_URL}/api/v1/change_pipeline_state" -H "Content-Type: application/json" -H "warpstream-api-key: ${API_KEY}" -d "{\"virtual_cluster_id\":\"${VIRTUAL_CLUSTER_ID}\",\"pipeline_id\":\"${PIPELINE_ID}\",\"desired_state\":\"paused\"}" | jq .
fi
if [[ "${WIPE_KAFKA_DATA:-0}" == "1" ]]; then
  docker compose down -v
else
  docker compose down
fi
# Preserve the local object-store prefix by default. The active Tableflow VCI
# may still reference files under warpstream/; deleting them can corrupt a run.
if [[ "${RESET_LOCAL_BUCKET:-0}" == "1" ]]; then
  [[ "${CONFIRM_LOCAL_BUCKET_RESET:-}" == "YES" ]] || {
    echo "Refusing to clear iceberg: set CONFIRM_LOCAL_BUCKET_RESET=YES." >&2
    exit 1
  }
  rm -rf iceberg
  mkdir -p iceberg/.tmp
  echo "Local containers stopped and Iceberg output reset."
else
  echo "Local containers stopped; Iceberg output preserved."
fi
