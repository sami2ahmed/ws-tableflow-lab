# CP KRaft + Tableflow Docker Demo Runbook

## 1. Purpose

This bundle runs the Tableflow demo with:

- A single-node combined-mode Confluent Platform KRaft broker.
- Single-node KRaft controller quorum through `KAFKA_CONTROLLER_QUORUM_VOTERS`.
- No Kafka authentication or TLS.
- A separate WarpStream agent running as the Tableflow worker.
- Local `file://` Iceberg output shared with DuckDB.
- A host-side raw-Protobuf producer.

The original demo flow is preserved: produce clickstream, orders, and inventory events; materialize them with Tableflow; query them with DuckDB; evolve the orders schema; and produce records containing `discount_code`.

## 2. Network layout


| Component            | Address           | Purpose                                    |
| -------------------- | ----------------- | ------------------------------------------ |
| CP internal listener | `cp-server:9090`  | Tableflow source bootstrap inside Compose  |
| CP host listener     | `localhost:19090` | Producer and other host-side Kafka clients |
| CP controller        | `cp-server:9093`  | Combined KRaft controller quorum           |
| Tableflow agent HTTP | `localhost:8080`  | Metrics and HTTP endpoint                  |
| Iceberg files        | `./iceberg`       | Shared host/agent/DuckDB data path         |


The two broker listeners are intentional. If the host producer used the Docker-advertised hostname, Kafka metadata would return `cp-server`, which is not normally resolvable from the host. Tableflow uses `cp-server:9090`; the producer uses `localhost:19090`.

## 3. Prerequisites

Install:

- Docker Engine or Docker Desktop with Compose v2.
- Python 3.10 or later.
- `curl` and `jq`.
- A WarpStream Tableflow cluster, agent key, and control-plane API key.

The Tableflow agent is not an offline-only process. It needs a registered Tableflow virtual-cluster ID, agent key, region, object-store URL, and access to the WarpStream control plane. The broker and Iceberg data path are local in this demo.

## 4. Prepare the bundle

From this directory:

```bash
cp .env.example .env
mkdir -p iceberg/.tmp queries
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt

# Optional but recommended after an interrupted or failed Compose run.
# This removes stale project containers so Compose recreates its network cleanly.
docker compose down --remove-orphans 2>/dev/null || true
docker compose rm -sf 2>/dev/null || true
```

Edit `.env`:

```dotenv
BASE_URL=https://api.warpstream.com
API_KEY=<WarpStream API key>
VIRTUAL_CLUSTER_ID=<Tableflow virtual cluster ID>
WARPSTREAM_DEFAULT_VIRTUAL_CLUSTER_ID=<same Tableflow virtual cluster ID>
WARPSTREAM_AGENT_KEY=<Tableflow agent key>
WARPSTREAM_REGION=ap-southeast-1
```

Keep these local-demo values unless you intentionally change the layout:

```dotenv
BROKER=localhost:19090
TABLEFLOW_BROKER_HOST=cp-server
TABLEFLOW_BROKER_PORT=9090
TABLEFLOW_BUCKET_URL=file:///tmp/tableflow-iceberg
```

`VIRTUAL_CLUSTER_ID` is used by the Pipeline API. `WARPSTREAM_DEFAULT_VIRTUAL_CLUSTER_ID` is used by the agent and should identify the same Tableflow cluster.

## 5. Start the stack

```bash
./scripts/start.sh
```

The script starts the CP broker, waits for `cp-server:9090` to become ready, then starts the Tableflow agent and DuckDB.

Verify container state:

```bash
docker compose ps
docker compose logs --tail=100 cp-server
docker compose logs --tail=100 warpstream-agent
```

Verify the broker from the host listener:

```bash
docker run --rm --network host edenhill/kcat:1.7.1 \
  -b localhost:19090 -L
```

If the host-side `kcat` image is unavailable, verify from the broker container instead:

```bash
docker compose exec cp-server \
  kafka-topics --bootstrap-server cp-server:9090 --list
```

## 6. Run the base demo

```bash
./scripts/run-demo.sh
```

The script:

- Preserves the existing local object-store prefix by default.
- Recreates `iceberg/.tmp` and starts the Compose services.
- Produces 200 raw-Protobuf records to each topic.
- Creates or reuses the `ecommerce-lakehouse` Tableflow pipeline.
- Configures Tableflow with `cp-server:9090` as the source broker.
- Uses inline raw-Protobuf schemas.
- Waits for Parquet output.
- Runs a DuckDB row-count query.

Do not wipe `iceberg/warpstream` while reusing the same Tableflow VCI. Tableflow metadata is persisted in the control plane and synchronized to the bucket; deleting the local `warpstream` prefix can leave the active VCI referencing data files that no longer exist. For a genuinely clean run, create a new `byoc_data_lake` VCI and matching agent key first, update `.env`, then reset the local directory with the explicit command in the recovery section.

The producer creates these topics through Kafka auto-topic creation:

- `clickstream`
- `orders`
- `inventory_updates`

The Tableflow configuration must keep the source-cluster connection separate from the destination and table definitions. `destination_bucket_url` and `tables` are top-level fields; do not indent them beneath the `source_clusters` item:

```yaml
source_clusters:
  - name: confluent_platform
    bootstrap_brokers:
      - hostname: cp-server
        port: 9090

destination_bucket_url: "file:///tmp/tableflow-iceberg"

tables:
  - source_cluster_name: confluent_platform
    source_topic: clickstream
    source_format: protobuf
    wire_format: raw
    schema_mode: inline
    input_schema: |
      syntax = "proto3";
      package ecommerce;
      message ClickEvent {
        string event_id = 1;
        string user_id = 2;
        int64 timestamp_ms = 3;
      }
```

If the API reports `field destination_bucket_url not found in type dlconfig.SourceCluster`, the fields are still nested under `source_clusters`; fix the indentation and rerun `./scripts/configure-tableflow.sh`.

The inline protobuf schemas must have a closing `}` for every `message {` declaration. The demo script validates this before submitting the configuration to the Tableflow API. If you see `syntax error: unexpected $end`, inspect the generated block and ensure the final schema ends like this:

```protobuf
message InventoryUpdate {
  string sku = 1;
  string warehouse_id = 2;
  int32 quantity_change = 3;
  int32 quantity_after = 4;
  string update_type = 5;
  int64 updated_at_ms = 6;
}
```

Rerun:

```bash
./scripts/configure-tableflow.sh
```

## 7. Run analytical queries

Run the compact row-count query:

```bash
./scripts/query.sh
```

Run the retail queries:

```bash
./scripts/query-retail.sh
```

Both query scripts run DuckDB as a one-shot container with the host `./iceberg` directory mounted read-only at both `/iceberg` and `/tmp/tableflow-iceberg`. The second mount is required because local Tableflow metadata can contain absolute `file://` paths rooted at `/tmp/tableflow-iceberg`. For each table, the scripts try metadata JSON files newest-first and select the newest snapshot DuckDB can actually open, because Tableflow can publish metadata before all referenced `data/*.parquet` files are visible. They then pass that metadata file explicitly to DuckDB's `iceberg_scan`, avoiding Iceberg version-hint guessing. The scripts continue retrying while a complete snapshot is being published. If every metadata candidate references a file that is absent from the host bucket, this is not a DuckDB query problem; the local Tableflow object-store state is inconsistent and must be recovered with a fresh Tableflow VCI. Configure the retry window when needed:

```bash
ICEBERG_QUERY_ATTEMPTS=60 ICEBERG_QUERY_INTERVAL_SECS=5 ./scripts/query-retail.sh
```

If the retry window expires with a missing Avro or Parquet data file, the local snapshot is incomplete; follow the recovery procedure in the troubleshooting section rather than repeatedly querying the same snapshot.

Open an interactive DuckDB shell using the host `./iceberg` directory:

```bash
docker run --rm -it \
  -v "$(pwd)/iceberg:/iceberg:ro" \
  -v "$(pwd)/iceberg:/tmp/tableflow-iceberg:ro" \
  -w /iceberg \
  duckdb/duckdb:latest
```

Inside DuckDB:

```sql
INSTALL iceberg;
LOAD iceberg;

-- First discover a table's metadata JSON from the shell:
-- find /iceberg/warpstream/_tableflow -path '*/metadata/*.json' -type f
-- Then pass the selected metadata JSON explicitly:
SELECT *
FROM iceberg_scan('/iceberg/warpstream/_tableflow/<table-id>/metadata/<metadata-file>.json')
LIMIT 10;
```

The exact table directory names include a Tableflow-generated suffix. The supplied scripts resolve those directories and try their `metadata/*.json` files newest-first, falling back to an older complete snapshot when a newer metadata file references a data file that is not present yet. They invoke `duckdb/duckdb:latest` with the local bucket mounted at both `/iceberg` and `/tmp/tableflow-iceberg`.

## 8. Evolve the orders schema

Pause the existing pipeline, deploy a new inline schema containing optional `discount_code`, and resume ingestion:

```bash
./scripts/evolve-schema.sh
```

Produce orders containing the new field and wait for Tableflow metadata to catch up:

```bash
./scripts/poll-discount.sh
```

The polling script produces additional discount-bearing orders after a few attempts and checks the Iceberg table through DuckDB until `discount_code` becomes queryable.

## 9. Inspect the Tableflow configuration

The configuration script can be rerun safely for the same Tableflow virtual cluster:

```bash
./scripts/configure-tableflow.sh
```

For the evolved schema:

```bash
WITH_DISCOUNT=1 ./scripts/configure-tableflow.sh
```

The scripts use these Pipeline API operations:

- `list_pipelines`
- `create_pipeline`
- `create_pipeline_configuration`
- `change_pipeline_state`

The latest pipeline and configuration IDs are written back to `.env`.

## 10. Troubleshooting

### Docker reports `network ... not found`

This usually means a Compose container still references a project network that Docker removed, often after an interrupted `docker compose down` or manual network cleanup. Remove the stale project containers and let Compose recreate the network:

```bash
docker compose down --remove-orphans 2>/dev/null || true
docker compose rm -sf 2>/dev/null || true
docker compose up -d --force-recreate --remove-orphans cp-server warpstream-agent
```

Then rerun the demo startup script if required:

```bash
./scripts/start.sh
```

Do not remove all Docker networks globally; the commands above are scoped to this Compose project.

### Agent exits during startup

Check the required values:

```bash
grep -E '^(API_KEY|VIRTUAL_CLUSTER_ID|WARPSTREAM_DEFAULT_VIRTUAL_CLUSTER_ID|WARPSTREAM_AGENT_KEY|WARPSTREAM_REGION)=' .env
```

Then inspect logs:

```bash
docker compose logs warpstream-agent
```

The agent requires a valid Tableflow cluster ID, agent key, region, and bucket URL.

### Tableflow cannot connect to Kafka

The Tableflow source broker must be:

```text
cp-server:9090
```

Do not use `localhost:19090` in the Tableflow Pipeline API configuration. `localhost` inside the Tableflow container refers to the Tableflow container itself.

Test connectivity from the agent container:

```bash
docker compose exec warpstream-agent sh -c \
  'wget -qO- http://cp-server:8080/ || true'
```

If the image does not contain `wget`, use the broker readiness check:

```bash
docker compose exec cp-server \
  kafka-topics --bootstrap-server cp-server:9090 --list
```

### Host producer cannot connect

Use:

```bash
BROKER=localhost:19090 .venv/bin/python scripts/produce-protobuf.py --count 5
```

Do not use `localhost:9090` for the host producer. Port `9090` is the internal Docker listener; port `19090` is the host-advertised listener.

### No Iceberg files appear

Check:

```bash
find iceberg -maxdepth 6 -type f | head -50
docker compose logs --tail=200 warpstream-agent
```

Confirm that the Tableflow destination is:

```text
file:///tmp/tableflow-iceberg
```

The agent temporary directory must remain inside that same mounted path:

```text
/tmp/tableflow-iceberg/.tmp
```

This avoids cross-device rename errors when the local file backend moves temporary permission files into the bucket directory. Confirm that the agent mount is:

```text
./iceberg:/tmp/tableflow-iceberg
```

### Agent reports `mkdir /tmp/tableflow-iceberg/warpstream: no such file or directory`

This usually happens when `iceberg/` was deleted while an existing Tableflow agent container still held the bind mount. Do not remove the directory while the agent is running. Recover the mount and recreate the agent:

```bash
docker compose stop warpstream-agent 2>/dev/null || true
docker compose rm -sf warpstream-agent 2>/dev/null || true
rm -rf iceberg
mkdir -p iceberg/.tmp queries
./scripts/start.sh
```

Then wait for new output:

```bash
find iceberg/warpstream/_tableflow -type f \( -name '*.json' -o -name '*.avro' \) -print -quit
docker compose logs --tail=100 warpstream-agent
```

The Compose mapping must remain:

```yaml
- ./iceberg:/tmp/tableflow-iceberg
```

and the agent temporary directory must remain inside the mounted path:

```yaml
TMPDIR: /tmp/tableflow-iceberg/.tmp
```

### DuckDB cannot find tables

The query scripts discover Tableflow output on the host at `./iceberg` and run DuckDB with two read-only mounts: `docker run --rm -v "$(pwd)/iceberg:/iceberg:ro" -v "$(pwd)/iceberg:/tmp/tableflow-iceberg:ro"`. `/iceberg` is the query path and `/tmp/tableflow-iceberg` preserves the absolute local paths embedded in Tableflow Iceberg metadata. Do not run `find /iceberg/...` from the host shell.

Check the host-side bind mount:

```bash
find ./iceberg/warpstream/_tableflow -maxdepth 2 -type f | head -50
./scripts/query.sh
```

If `./iceberg/warpstream/_tableflow` does not exist yet, wait for the agent to publish its first Tableflow snapshot:

```bash
ICEBERG_QUERY_ATTEMPTS=60 ICEBERG_QUERY_INTERVAL_SECS=5 ./scripts/query.sh
```

The scripts now wait for the host directory before invoking DuckDB, then translate host paths to `/iceberg/...` inside the DuckDB container. If the directory never appears, inspect the agent:

```bash
docker compose ps
docker compose logs --tail=100 warpstream-agent
```

### Important: local `file://` storage and reruns

The local file backend is suitable for development only and is not robust. Tableflow keeps metadata in the WarpStream control plane and periodically synchronizes it to the destination bucket. Do not delete the `warpstream` prefix and then reuse the same Tableflow VCI; that can produce metadata which references missing Parquet or manifest files.

Use the same local directory for an existing VCI and let the agent continue publishing. For a clean demo, create a new `byoc_data_lake` VCI and matching agent key, update `VIRTUAL_CLUSTER_ID`, `WARPSTREAM_DEFAULT_VIRTUAL_CLUSTER_ID`, and `WARPSTREAM_AGENT_KEY` in `.env`, then stop/remove the agent and clear `iceberg` before starting again.

### DuckDB cannot guess the Iceberg table version

The local Tableflow layout can contain metadata JSON whose paths use the agent container root `/tmp/tableflow-iceberg`. The generated scripts map the host bucket to both `/iceberg` and `/tmp/tableflow-iceberg`, then test metadata JSON files newest-first with DuckDB. They select the newest file whose referenced manifests and data files are readable; a newer metadata file can be visible before its `data/*.parquet` file. If no candidate is complete yet, the scripts wait and retry.

For an interactive session, run:

```sql
INSTALL iceberg;
LOAD iceberg;
-- Use a metadata file that has a complete referenced data snapshot.
-- If the newest file reports a missing data/*.parquet file, try an older metadata JSON
-- or run ./scripts/query-retail.sh, which tests candidates automatically.
SELECT * FROM iceberg_scan('/iceberg/warpstream/_tableflow/<table-id>/metadata/<complete-metadata-file>.json') LIMIT 10;
```

### DuckDB reports a missing Avro or Parquet file

An error such as:

```text
Cannot open file "/tmp/tableflow-iceberg/warpstream/.../data/....parquet": No such file or directory
```

or:

```text
Cannot open file "/tmp/tableflow-iceberg/warpstream/.../metadata/snap-....avro": No such file or directory
```

first indicates either a path-visibility problem or an incomplete snapshot. Local Tableflow metadata may reference `/tmp/tableflow-iceberg`, so DuckDB must mount the host `./iceberg` directory at both `/iceberg` and `/tmp/tableflow-iceberg`; the query scripts do this automatically. The scripts also try older metadata JSON files, so a newer incomplete snapshot does not block reading the last complete one.

Check whether the file named by the error exists on the host:

```bash
find ./iceberg/warpstream/_tableflow -type f \( -name '*.parquet' -o -name '*.avro' \) | head -50
docker compose logs --tail=200 warpstream-agent
```

If the referenced file is absent and every metadata candidate fails, no DuckDB setting or query can repair that snapshot. Do not repeatedly run `rm -rf iceberg` and reuse the same Tableflow VCI: Tableflow keeps control-plane metadata and synchronizes it to the object store, and the WarpStream documentation warns against manually deleting files under the `warpstream` prefix. The `file://` backend is intended only for local development and is not robust.

For the current VCI, preserve the bucket and inspect the agent first:

```bash
docker compose logs --since=15m warpstream-agent
docker compose ps
```

For a clean local demo, create a new `byoc_data_lake` Tableflow VCI and matching agent key, update `.env`, then reset the local directory only after stopping and removing the agent:

```bash
docker compose stop warpstream-agent 2>/dev/null || true
docker compose rm -sf warpstream-agent 2>/dev/null || true
rm -rf iceberg
mkdir -p iceberg/.tmp queries
./scripts/start.sh
./scripts/run-demo.sh
```

The supplied `run-demo.sh` no longer deletes `iceberg` automatically. If you intentionally reset a fresh VCI, use `RESET_LOCAL_BUCKET=1 CONFIRM_LOCAL_BUCKET_RESET=YES ./scripts/run-demo.sh`.

Do not run `rm -rf iceberg` while `warpstream-agent` is running. The agent can retain a stale or empty bind mount, and subsequent writes may fail with `mkdir /tmp/tableflow-iceberg/warpstream: no such file or directory`.

### KRaft storage-format error about missing voters

If startup reports `Because controller.quorum.voters is not set on this controller`, use the supplied Compose file with:

```yaml
KAFKA_CONTROLLER_QUORUM_VOTERS: "0@cp-server:9093"
```

The stock `cp-server:latest` entrypoint uses this value while formatting the single-node KRaft log. Do not set `KAFKA_CONTROLLER_QUORUM_BOOTSTRAP_SERVERS` at the same time for this demo. Dynamic controller quorum bootstrap requires a custom initialization flow that formats storage with `--standalone` or `--initial-controllers`; the supplied stack intentionally uses the compatible static single-node voter configuration.

## 11. Stop and clean up

Pause the Tableflow pipeline, remove local Iceberg files, and stop the containers:

```bash
./scripts/cleanup.sh
```

To also remove the named Kafka data volume and start with a fresh broker:

```bash
WIPE_KAFKA_DATA=1 ./scripts/cleanup.sh
```

The cleanup script does not delete the persistent Tableflow pipeline or virtual cluster from the WarpStream control plane. It preserves the local `iceberg/` directory by default; this is intentional. Do not manually delete its `warpstream` prefix while reusing the same Tableflow VCI.

## 12. Source references

- [ws-tableflow-lab repository](https://github.com/sami2ahmed/ws-tableflow-lab/tree/dev)
- [WarpStream Tableflow setup](https://docs.warpstream.com/warpstream/tableflow/tableflow)
- [WarpStream agent deployment](https://docs.warpstream.com/warpstream/agent-setup/deploy)
- [WarpStream object storage configuration](https://docs.warpstream.com/warpstream/agent-setup/different-object-stores)

&nbsp;