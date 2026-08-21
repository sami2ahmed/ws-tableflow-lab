#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
[[ -f .env ]] && { set -a; source .env; set +a; }
: "${BASE_URL:?BASE_URL is required}"
: "${API_KEY:?API_KEY is required}"
: "${VIRTUAL_CLUSTER_ID:?VIRTUAL_CLUSTER_ID is required}"
: "${PIPELINE_ID:?Run run-demo.sh first}"
curl -sS -X POST "${BASE_URL}/api/v1/change_pipeline_state" -H "Content-Type: application/json" -H "warpstream-api-key: ${API_KEY}" -d "{\"virtual_cluster_id\":\"${VIRTUAL_CLUSTER_ID}\",\"pipeline_id\":\"${PIPELINE_ID}\",\"desired_state\":\"paused\"}" | jq .
WITH_DISCOUNT=1 "$ROOT/scripts/configure-tableflow.sh"
