#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
[[ -f .env ]] && { set -a; source .env; set +a; }
mkdir -p iceberg/.tmp queries
BROKER="${BROKER:-localhost:19090}"
command -v docker >/dev/null 2>&1 || { echo "Missing required command: docker" >&2; exit 1; }
docker compose up -d cp-server
for i in $(seq 1 40); do
  if docker compose exec -T cp-server kafka-topics --bootstrap-server cp-server:9090 --list >/dev/null 2>&1; then break; fi
  [[ "$i" -eq 40 ]] && { docker compose logs cp-server; exit 1; }
  sleep 2
done
docker compose up -d warpstream-agent
echo "CP internal listener: cp-server:9090"
echo "CP host listener: localhost:${BROKER##*:}"
echo "Tableflow data path: ${ROOT}/iceberg"
