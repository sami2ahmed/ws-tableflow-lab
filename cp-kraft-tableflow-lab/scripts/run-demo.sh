#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
[[ -f .env ]] && { set -a; source .env; set +a; }
BROKER="${BROKER:-localhost:19090}"
PYTHON="$ROOT/.venv/bin/python"; [[ -x "$PYTHON" ]] || PYTHON=python3
# Preserve the local object-store prefix by default. Tableflow metadata is
# associated with the VCI; deleting iceberg while reusing that VCI can leave
# metadata pointing at files that no longer exist. Reset only with an explicitly
# confirmed fresh VCI.
if [[ "${RESET_LOCAL_BUCKET:-0}" == "1" ]]; then
  [[ "${CONFIRM_LOCAL_BUCKET_RESET:-}" == "YES" ]] || {
    echo "Refusing to clear iceberg: set CONFIRM_LOCAL_BUCKET_RESET=YES." >&2
    exit 1
  }
  docker compose stop warpstream-agent >/dev/null 2>&1 || true
  docker compose rm -sf warpstream-agent >/dev/null 2>&1 || true
  rm -rf iceberg
fi
mkdir -p iceberg/.tmp queries
"$ROOT/scripts/start.sh"
"$PYTHON" -m pip install -r requirements.txt >/dev/null
"$PYTHON" scripts/produce-protobuf.py --broker "$BROKER" --count 200
"$ROOT/scripts/configure-tableflow.sh"
for i in $(seq 1 "${WAIT_MAX_ATTEMPTS:-60}"); do
  if find iceberg/warpstream/_tableflow -type f \( -name '*.json' -o -name '*.avro' \) -print -quit 2>/dev/null | grep -q .; then break; fi
  [[ "$i" -eq "${WAIT_MAX_ATTEMPTS:-60}" ]] && { echo "Timed out waiting for Tableflow metadata or data files." >&2; exit 1; }
  sleep "${WAIT_INTERVAL_SECS:-5}"
done
"$ROOT/scripts/query.sh"
