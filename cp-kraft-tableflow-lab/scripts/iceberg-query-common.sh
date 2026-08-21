#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TABLE_ROOT="$ROOT/iceberg/warpstream/_tableflow"
ICEBERG_QUERY_ATTEMPTS="${ICEBERG_QUERY_ATTEMPTS:-24}"
ICEBERG_QUERY_INTERVAL_SECS="${ICEBERG_QUERY_INTERVAL_SECS:-5}"

iceberg_path() {
  printf '/iceberg/%s' "${1#"$ROOT/iceberg/"}"
}

duckdb_query() {
  docker run --rm -i \
    -v "$ROOT/iceberg:/iceberg:ro" \
    -v "$ROOT/iceberg:/tmp/tableflow-iceberg:ro" \
    -w /iceberg \
    duckdb/duckdb:latest -c "$1"
}

find_table() {
  find "$TABLE_ROOT" -mindepth 1 -maxdepth 1 -type d -name "$1-*" -print -quit
}

metadata_candidates() {
  local table="$1" candidate mtime
  [[ -d "$table/metadata" ]] || return 0
  while IFS= read -r -d '' candidate; do
    jq -e 'type == "object"' "$candidate" >/dev/null 2>&1 || continue
    mtime=$(stat -f %m "$candidate" 2>/dev/null || stat -c %Y "$candidate" 2>/dev/null || echo 0)
    printf '%s\t%s\n' "$mtime" "$candidate"
  done < <(find "$table/metadata" -maxdepth 1 -type f -name '*.json' -print0) \
    | sort -nr -k1,1 \
    | cut -f2-
}

# Return the newest metadata JSON that DuckDB can actually open. A newer
# metadata file can be visible before its referenced data/Parquet file.
find_complete_metadata() {
  local table="$1" candidate candidate_i
  while IFS= read -r candidate; do
    [[ -n "$candidate" ]] || continue
    candidate_i=$(iceberg_path "$candidate")
    if duckdb_query "INSTALL iceberg; LOAD iceberg; SELECT COUNT(*) FROM iceberg_scan('$candidate_i');" >/dev/null 2>&1; then
      printf '%s' "$candidate"
      return 0
    fi
  done < <(metadata_candidates "$table")
  return 1
}

wait_for_table_root() {
  local i
  for i in $(seq 1 "$ICEBERG_QUERY_ATTEMPTS"); do
    if [[ -d "$TABLE_ROOT" ]]; then
      return 0
    fi
    echo "Tableflow output directory is not present yet (${i}/${ICEBERG_QUERY_ATTEMPTS}); waiting..." >&2
    sleep "$ICEBERG_QUERY_INTERVAL_SECS"
  done
  echo "No Tableflow output found at host path: $TABLE_ROOT" >&2
  docker compose logs --tail=50 warpstream-agent >&2 || true
  return 1
}

print_incomplete_snapshot_help() {
  cat >&2 <<'MSG'
DuckDB could not open a complete Iceberg snapshot. The newest metadata JSON can be
published before its referenced data/Parquet file. The scripts tried older metadata
files as well. If no snapshot becomes complete, stop and remove the Tableflow agent
before clearing the local bucket, then rerun the demo.
MSG
}
