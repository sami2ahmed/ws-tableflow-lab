# Elevate 2026 — WarpStream Tableflow Demo

Local tooling and workshop notes for WarpStream Tableflow: Kafka topics materialized as Apache Iceberg tables, queried with DuckDB.

## What is WarpStream Tableflow

[WarpStream Tableflow](https://www.warpstream.com/tableflow) automatically materializes Kafka topics as Apache Iceberg tables in your object storage. It is the simplest path from streaming data to a queryable lakehouse.

- Reads from **any Kafka-compatible source** (OSS Kafka, MSK, Confluent Cloud, WarpStream)
- Supports **JSON, Avro, and Protobuf** formats with inline schema definitions (Schema Registry integration coming soon)
- Deploys as a **single stateless binary** that auto-scales via Kubernetes HPA
- **Zero-ops table maintenance**: compaction, snapshot cleanup, orphan file cleanup, and data expiration run automatically in the background
- **BYOC deployment**: agents run in your VPC; data stays in your object storage bucket
- Integrates with **BigQuery, Databricks, Snowflake, AWS Glue, DuckDB, ClickHouse, Trino, Athena**

Tableflow overview

Tableflow handles the entire lifecycle — ingestion, schema evolution, partitioning, compaction, and retention — in a single declarative YAML configuration.

## What Tableflow Supports



### Ingestion

- **Formats**: JSON, Avro, and Protobuf with nested types. Schemas are defined inline in the YAML configuration — no external Schema Registry required.
- **Transforms**: Stateless [Bento/Bloblang](https://warpstreamlabs.github.io/bento/docs/guides/bloblang/about) transforms at ingest time — rename fields, drop PII, restructure nested payloads, convert types, or filter records (not demonstrated in this session).
- **Exactly-once semantics**: Parquet file registration and Kafka offset commit happen in one atomic transaction. Failure recovery is not demonstrated today, but the atomic commit guarantees no duplicates and no data loss by design.



### Storage and Query Optimization

- **Partitioning**: Unpartitioned, or by hour/day/month/year, or any arbitrary field with bucket/truncate transforms. Kafka partitioning (for ordering) is independent from Iceberg partitioning (for query pruning) — Tableflow handles the impedance mismatch.
- **Compression**: Configurable per table — snappy (default), zstd, lz4, gzip, brotli, or none.
- **Column statistics**: Parquet files include min/max/null-count stats per column, enabling predicate pushdown in query engines.
- **Schema evolution**: Add fields, widen numeric types, make fields optional — update the YAML config and deploy. Tableflow migrates the Iceberg schema automatically; old rows return `null` for new columns.



### Automatic Table Maintenance

All of the following run in the background with zero tuning:

- **Compaction** — merges small Parquet files into larger ones for better query performance
- **Data expiration** — removes data older than a configurable `retention_ttl` per table, based on record timestamps
- **Snapshot cleanup** — prunes old Iceberg snapshots to reduce metadata overhead
- **Orphan file cleanup** — detects and deletes unreferenced Parquet files



### Catalog Integrations

WarpStream Tableflow exposes a built-in [Iceberg REST Catalog](https://docs.warpstream.com/warpstream/tableflow/iceberg-catalog) and pushes metadata to external catalogs: BigQuery, Databricks Unity Catalog, AWS Glue, HMS (WIP), and Snowflake.

## What This Demo Shows

This demo covers WarpStream Tableflow 101:

1. **Produce protobuf-encoded events** to three Kafka topics representing a subset of an e-commerce data platform
2. **Configure Tableflow** via the Pipeline API to ingest all three topics into Iceberg tables with one YAML file
3. **Query the Iceberg tables** directly with DuckDB — cross-topic joins, aggregations, and time-based filtering
4. **Evolve the schema** by adding a new field through the Pipeline API and observe backward-compatible results



### Demo Scenario

Three topics model distinct layers of an e-commerce data platform:


| Topic               | Data                                       | Volume Profile                   |
| ------------------- | ------------------------------------------ | -------------------------------- |
| `clickstream`       | Page views, button clicks, session context | High volume, append-only         |
| `orders`            | Purchase transactions with line items      | Medium volume, business-critical |
| `inventory_updates` | Stock level changes per SKU/warehouse      | Low volume, operational          |


All topics use **protobuf** with `wire_format: raw` (no schema registry required). Tableflow ingests them into Iceberg tables partitioned by hour.

## Prerequisites

You need:


| Tool                            | Purpose                                              |
| ------------------------------- | ---------------------------------------------------- |
| macOS or Linux                  | Local demo host                                      |
| Python 3.9+                     | Protobuf producer scripts                            |
| curl                            | DuckDB / WarpStream installers                       |
| Homebrew (macOS) or apt (Linux) | `kcat`, `jq`, optionally WarpStream                  |
| WarpStream CLI                  | Local Kafka + Schema Registry + Tableflow playground |
| DuckDB CLI + Iceberg extension  | Query materialized Iceberg tables                    |
| `kcat`                          | Kafka CLI                                            |
| `jq`                            | JSON formatting for API responses                    |


Follow the sections below in order.

## 1. Python setup

Create a project-local virtual environment and install dependencies from `requirements.txt` (`confluent-kafka`, `protobuf`):

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

Activate in later sessions with:

```bash
source .venv/bin/activate
```



## 2. DuckDB CLI + Iceberg

Install the DuckDB CLI:

```bash
curl https://install.duckdb.org | sh
```

Add DuckDB to your `PATH` (zsh example):

```bash
echo 'export PATH="$HOME/.duckdb/cli/latest:$PATH"' >> ~/.zshrc
source ~/.zshrc
```

Verify:

```bash
duckdb --version
```

Install the Iceberg extension (one-time). Load it again in each DuckDB session:

```bash
duckdb -c "INSTALL iceberg; LOAD iceberg;"
```

For remote Iceberg / S3 access, also install `httpfs`:

```bash
duckdb -c "INSTALL httpfs; LOAD httpfs;"
```



## 3. WarpStream CLI

Install the WarpStream Agent / CLI with Homebrew or the installation script:

[Install the WarpStream Agent / CLI](https://docs.warpstream.com/warpstream/getting-started/install-the-warpstream-agent?select=brew,installation-script#brew)

## 4. kcat (Kafka CLI)

```bash
# macOS
brew install kcat

# Linux
apt-get install kafkacat
```



## 5. jq (JSON formatter)

```bash
# macOS
brew install jq

# Linux
apt-get install jq
```



## 6. Environment variables

Copy the example env file and fill in secrets from the WarpStream console (after playground is running — see next section):

```bash
cp .env.example .env
```

`.env` fields:


| Variable                    | Value                                                                                           |
| --------------------------- | ----------------------------------------------------------------------------------------------- |
| `BASE_URL`                  | `https://api.warpstream.com`                                                                    |
| `API_KEY`                   | Console → API Keys (create one if needed)                                                       |
| `VIRTUAL_CLUSTER_ID`        | The **Tableflow** cluster id, looks like `vci_dl_...` (not the Kafka `vci_...` default cluster) |
| `BROKER`                    | `localhost:9092` (playground Kafka port)                                                        |
| `PIPELINE_ID` / `CONFIG_ID` | Leave blank; `configure-tableflow.sh` writes them                                               |


Load them into your shell:

```bash
set -a && source .env && set +a
```

`.env` is gitignored. Do not commit real keys.

## 7. Start WarpStream Playground

Keep this process running in a dedicated terminal:

```bash
warpstream playground
```

Expected output (cluster IDs and session keys will differ):

```
Creating Schema Registry cluster...
Creating Tableflow cluster...
Creating temporary data directory:
Starting local agents...
Enabling events on the default cluster...

open the developer console at: https://console.warpstream.com/login?warpstream_redirect_to=virtual_clusters%2Fvci_...%2Foverview&warpstream_session_key=sks_...

You now have WarpStream Kafka, Schema Registry, and Tableflow clusters running.

Ports:
    [Kafka Agent]
        Kafka protocol port (TCP): 9092 (override using -kafkaPort)
        internal HTTP port: 8080 (override using -httpPort)
    [Schema Registry Agent]
        Schema Registry protocol (HTTP) port: 9094 (override using -schemaRegistryPort)
        internal HTTP port: 8070 (override using -schemaRegistryHTTPPort)
    [Tableflow Agent]
        internal HTTP port: 8081 (override using -tableflowInternalHTTPPort)
```

Open the console URL from the output, then set `API_KEY` and the Tableflow `VIRTUAL_CLUSTER_ID` (`vci_dl_...`) in `.env` and reload env vars as in step 6.

If produce later fails with clock-sync / heartbeat errors in the playground logs, stop playground (Ctrl+C) and start it again.

## 8. Local Iceberg output directory

```bash
mkdir -p /tmp/warpstream-tableflow-iceberg
```



## Setup checklist

- [ ] `.venv` created and `pip install -r requirements.txt` succeeded
- [ ] `duckdb --version` works and `INSTALL iceberg` succeeded
- [ ] `warpstream` CLI installed
- [ ] `kcat` and `jq` installed
- [ ] `warpstream playground` running (ports 9092 / 9094 / 8081)
- [ ] `.env` filled with `API_KEY` and Tableflow `VIRTUAL_CLUSTER_ID` (`vci_dl_...`)
- [ ] `/tmp/warpstream-tableflow-iceberg` exists



## End-to-end path (exact commands from this session)

With playground already running in another terminal, tooling installed, and `.env` filled:

```bash
cd /path/to/elevate-2026
source .venv/bin/activate
set -a && source .env && set +a
export PATH="$HOME/.duckdb/cli/latest:$PATH"

mkdir -p /tmp/warpstream-tableflow-iceberg

# 1) Seed Kafka topics (200 msgs × 3 topics)
python scripts/produce-protobuf.py --broker localhost:9092 --count 200 --with-discount

# 2) Create/reuse Tableflow pipeline + deploy YAML + start it
./scripts/configure-tableflow.sh

# 3) Wait until Parquet exists, then until Iceberg metadata appears
ls /tmp/warpstream-tableflow-iceberg/warpstream/_tableflow/
find /tmp/warpstream-tableflow-iceberg -name "*.parquet" | wc -l
# poll until this returns files (often a few minutes on playground; workshop says up to ~10)
find /tmp/warpstream-tableflow-iceberg -name "v*.metadata.json"

CLICKS_TABLE=$(ls -d /tmp/warpstream-tableflow-iceberg/warpstream/_tableflow/ecommerce_kafka__clickstream-*)
ORDERS_TABLE=$(ls -d /tmp/warpstream-tableflow-iceberg/warpstream/_tableflow/ecommerce_kafka__orders-*)
INVENTORY_TABLE=$(ls -d /tmp/warpstream-tableflow-iceberg/warpstream/_tableflow/ecommerce_kafka__inventory_updates-*)

# 4) Query Iceberg with DuckDB
duckdb -c "
LOAD iceberg;
SELECT 'clickstream' AS tbl, COUNT(*) AS rows FROM iceberg_scan('$CLICKS_TABLE')
UNION ALL SELECT 'orders', COUNT(*) FROM iceberg_scan('$ORDERS_TABLE')
UNION ALL SELECT 'inventory', COUNT(*) FROM iceberg_scan('$INVENTORY_TABLE');
"

# 5) Evolve orders schema (adds discount_code = 9), then produce more
./scripts/evolve-schema.sh
python scripts/produce-protobuf.py --broker localhost:9092 --count 50 --with-discount
# wait for new Parquet, then for Iceberg metadata bump (we needed ~1-2+ minutes)
# optional extra batch if discount_code is still all NULL in iceberg_scan:
# python scripts/produce-protobuf.py --broker localhost:9092 --count 30 --with-discount

duckdb -c "
LOAD iceberg;
SELECT
  COUNT(*) AS total_orders,
  COUNT(discount_code) AS with_discount,
  COUNT(*) - COUNT(discount_code) AS null_discount
FROM iceberg_scan('$ORDERS_TABLE');
"
```

This session ended schema evolution at **580 total / 30 with** `discount_code` **/ 550 NULL**.

## Produce protobuf events

Proto definitions and generated `*_pb2.py` already live in `proto/`. Producer script is `scripts/produce-protobuf.py`.

With the playground running and the venv activated:

```bash
source .venv/bin/activate
python scripts/produce-protobuf.py --broker localhost:9092 --count 200 --with-discount
```

Expected success output:

```
  clickstream:            200 events sent
  orders:                 200 events sent (with discount_code)
  inventory_updates:      200 events sent
Done. Total: 600 protobuf messages produced.
```

You may also see a librdkafka telemetry log line (`GETSUBSCRIPTIONS`). That is harmless.

`--with-discount` sets protobuf field 9 on orders. Before schema evolution, Tableflow config does not include that field (`dlq_mode: skip`), so early orders land in Iceberg without `discount_code`. After evolution, new rows get values and old rows stay `NULL`. That is the demo.

Optional continuous produce (not required; we mostly used one-shot batches):

```bash
while true; do python scripts/produce-protobuf.py --broker localhost:9092 --count 50 --with-discount; sleep 30; done
```

Verify bytes are in the topics:

```bash
kcat -b localhost:9092 -t clickstream -C -o beginning -c 3 -f 'Partition: %p | Offset: %o | Bytes: %S\n'
kcat -b localhost:9092 -t orders -C -o beginning -c 3 -f 'Partition: %p | Offset: %o | Bytes: %S\n'
kcat -b localhost:9092 -t inventory_updates -C -o beginning -c 3 -f 'Partition: %p | Offset: %o | Bytes: %S\n'
```

Producer uses shared `user-{1..200}` for clickstream `user_id` and orders `customer_id` so attribution joins can match.

## Configure Tableflow via Pipeline API

Reuse or create a `data_lake` pipeline, deploy the ecommerce YAML config, and start it:

```bash
set -a && source .env && set +a
./scripts/configure-tableflow.sh
```

The script:

1. Lists existing pipelines and reuses a `data_lake` pipeline if present
2. Creates a pipeline configuration for `clickstream`, `orders`, and `inventory_updates`
3. Sets desired state to `running`
4. Upserts `PIPELINE_ID` and `CONFIG_ID` in `.env`

Important API detail: current Tableflow expects raw `.proto` text under `input_schema: |`. The older workshop shape (`schema.fields: [{proto_field_number...}]`) is outdated and will not configure correctly. Our scripts already use `input_schema`.

Expected `describe_pipeline` overview shape:

```json
{
  "id": "...",
  "name": "ecommerce-lakehouse",
  "state": "running",
  "type": "data_lake",
  "deployed_configuration_id": "..."
}
```

(If a default pipeline already existed, the name may differ; state/type/`deployed_configuration_id` are what matter.)

Parquet files appear within seconds. Iceberg metadata (`v*.metadata.json`) lagged longer on playground (a few minutes here; workshop cites up to ~10). Inspect output:

```bash
ls /tmp/warpstream-tableflow-iceberg/warpstream/_tableflow/

CLICKS_TABLE=$(ls -d /tmp/warpstream-tableflow-iceberg/warpstream/_tableflow/ecommerce_kafka__clickstream-*)
ORDERS_TABLE=$(ls -d /tmp/warpstream-tableflow-iceberg/warpstream/_tableflow/ecommerce_kafka__orders-*)
INVENTORY_TABLE=$(ls -d /tmp/warpstream-tableflow-iceberg/warpstream/_tableflow/ecommerce_kafka__inventory_updates-*)

find /tmp/warpstream-tableflow-iceberg -name "*.parquet" | wc -l
duckdb -c "SELECT * FROM read_parquet('$ORDERS_TABLE/data/*.parquet') LIMIT 5;"
find /tmp/warpstream-tableflow-iceberg -name "v*.metadata.json"
```

This session wrote tables under `/tmp/warpstream-tableflow-iceberg/warpstream/_tableflow/` named like:

- `ecommerce_kafka__clickstream-<uuid>`
- `ecommerce_kafka__orders-<uuid>`
- `ecommerce_kafka__inventory_updates-<uuid>`



## Query Iceberg tables with DuckDB

Once metadata exists, set the table path variables (from above) and run:

```bash
export PATH="$HOME/.duckdb/cli/latest:$PATH"
# CLICKS_TABLE / ORDERS_TABLE / INVENTORY_TABLE already set

duckdb -c "
LOAD iceberg;
SELECT 'clickstream' AS tbl, COUNT(*) AS rows FROM iceberg_scan('$CLICKS_TABLE')
UNION ALL SELECT 'orders', COUNT(*) FROM iceberg_scan('$ORDERS_TABLE')
UNION ALL SELECT 'inventory', COUNT(*) FROM iceberg_scan('$INVENTORY_TABLE');
"
```

Expected shape (counts grow as you keep producing):

```
┌─────────────┬───────┐
│     tbl     │ rows  │
├─────────────┼───────┤
│ clickstream │   400 │
│ orders      │   400 │
│ inventory   │   400 │
└─────────────┴───────┘
```

Revenue by payment method:

```bash
duckdb -c "
LOAD iceberg;
SELECT payment_method, COUNT(*) AS order_count,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    ROUND(AVG(total_amount), 2) AS avg_order_value
FROM iceberg_scan('$ORDERS_TABLE')
GROUP BY payment_method
ORDER BY total_revenue DESC;
"
```

Example result:

```
┌────────────────┬─────────────┬───────────────┬─────────────────┐
│ payment_method │ order_count │ total_revenue │ avg_order_value │
├────────────────┼─────────────┼───────────────┼─────────────────┤
│ paypal         │         107 │      27554.87 │          257.52 │
│ card           │         102 │       25428.3 │           249.3 │
│ bank_transfer  │         100 │      24929.09 │          249.29 │
│ apple_pay      │          91 │      21169.09 │          232.63 │
└────────────────┴─────────────┴───────────────┴─────────────────┘
```

More queries (attribution join, inventory snapshot, unified customer view) live in `scripts/queries.sql`. Paths there are hardcoded to this session's table UUIDs. Refresh them with:

```bash
ls /tmp/warpstream-tableflow-iceberg/warpstream/_tableflow/
```

Then paste into an interactive `duckdb` session after `LOAD iceberg;`, or edit the paths in `queries.sql`.

## Schema evolution: add `discount_code`

Add optional `discount_code` (protobuf field 9) to the orders table without rebuilding existing data.

```bash
set -a && source .env && set +a
./scripts/evolve-schema.sh
```

The script pauses the pipeline, deploys a new config (orders `input_schema` includes `string discount_code = 9`), resumes with that config, and updates `CONFIG_ID` in `.env`.

Then produce events that include the new field:

```bash
python scripts/produce-protobuf.py --broker localhost:9092 --count 50 --with-discount
```

What happened in this session:

1. New Parquet files showed `discount_code` within seconds (`DESCRIBE SELECT * FROM read_parquet(...)`).
2. `iceberg_scan` still showed all-NULL for a while. Iceberg metadata lagged Parquet.
3. We produced another `--count 30` batch and polled `version-hint.text` / `v*.metadata.json` every ~20s until non-nulls appeared (~1-2+ minutes after the new Parquet).

Poll helper:

```bash
ORDERS_TABLE=$(ls -d /tmp/warpstream-tableflow-iceberg/warpstream/_tableflow/ecommerce_kafka__orders-*)
cat "$ORDERS_TABLE/metadata/version-hint.text"
ls -lt "$ORDERS_TABLE/metadata" | head

duckdb -c "
LOAD iceberg;
SELECT
  COUNT(*) AS total_orders,
  COUNT(discount_code) AS with_discount,
  COUNT(*) - COUNT(discount_code) AS null_discount
FROM iceberg_scan('$ORDERS_TABLE');
"
```

Final counts from this session:

```
┌──────────────┬───────────────┬───────────────┐
│ total_orders │ with_discount │ null_discount │
├──────────────┼───────────────┼───────────────┤
│          580 │            30 │           550 │
└──────────────┴───────────────┴───────────────┘
```

Older rows stay `NULL`; newer rows have values:

```bash
duckdb -c "
LOAD iceberg;
SELECT order_id, customer_id, total_amount, discount_code
FROM iceberg_scan('$ORDERS_TABLE')
ORDER BY created_at_ms DESC
LIMIT 8;
"
```

```
┌──────────────────────────────────────┬─────────────┬──────────────┬───────────────┐
│               order_id               │ customer_id │ total_amount │ discount_code │
├──────────────────────────────────────┼─────────────┼──────────────┼───────────────┤
│ 4de344a0-2f2b-455d-b64c-20d6469ab873 │ user-36     │       229.08 │ FREESHIP      │
│ 0c7cdf71-c827-49aa-bd93-79a2140a282a │ user-126    │       314.94 │ WELCOME20     │
│ 06025d54-876b-40be-abba-6fe02d5bd9ec │ user-87     │       177.44 │ WELCOME20     │
│ ...                                  │ ...         │         ... │ ...           │
└──────────────────────────────────────┴─────────────┴──────────────┴───────────────┘
```

No table rebuild. Tableflow migrates the Iceberg schema when the new config is deployed.

## Key takeaways

- **One config, multiple topics**: a single Tableflow YAML replaces separate Kafka Connect + S3 sink + Spark compaction pipelines per topic
- **Protobuf-native**: raw protobuf wire format works without a schema registry; inline `input_schema` maps proto fields to Iceberg columns
- **Schema evolution without downtime**: add fields via the Pipeline API; old rows return `null` for new columns
- **Zero operational overhead**: compaction, snapshot cleanup, orphan file cleanup, and retention run in the background



## Cleanup (optional)

Cleanup is optional for this playground lab. Temporary playground clusters expire on their own.

Minimum teardown we used:

```bash
rm -rf /tmp/warpstream-tableflow-iceberg
# Ctrl+C the `warpstream playground` terminal
```

Full API cleanup via script:

```bash
set -a && source .env && set +a
./scripts/cleanup.sh
```

`cleanup.sh` tries to:

1. Pause the `data_lake` pipeline
2. Delete each table via `POST /api/v1/dl/delete_table`
3. Delete the pipeline via `POST /api/v1/delete_pipeline`
4. Remove `/tmp/warpstream-tableflow-iceberg`
5. Clear `PIPELINE_ID` / `CONFIG_ID` in `.env`

On this playground / demo account, step 2 returned `demo_not_allowed`, so step 3 then failed with `cannot_delete_tableflow_pipeline_with_tables`. Pause + local `rm` + stopping playground is enough for local replication.

If your account allows table delete, pause first, delete tables, then delete the pipeline (manual curls in the appendix table below).

## Appendix: supported schema migrations (Protobuf)


| Migration                              | Supported | Notes                                           |
| -------------------------------------- | --------- | ----------------------------------------------- |
| Add new field                          | Yes       | New Iceberg column; existing rows return `null` |
| Make required field optional           | Yes       | Safe                                            |
| Widen numeric type (`int32` → `int64`) | Yes       | Iceberg column type widens                      |
| Add new enum value                     | Yes       | Stored as string name                           |
| Remove field                           | No        | Drop and recreate table                         |
| Rename enum value                      | No        | Would make old/new rows inconsistent            |
| Change field type (non-widening)       | No        | Drop and recreate table                         |


Deploy schema changes **before** producing data with the new schema. Unknown fields are skipped or halt ingestion depending on `dlq_mode` (`skip` / `stop`).

## Appendix: Pipeline API quick reference


| Operation            | Endpoint                                     | Key fields                                       |
| -------------------- | -------------------------------------------- | ------------------------------------------------ |
| List Pipelines       | `POST /api/v1/list_pipelines`                | `virtual_cluster_id`                             |
| Create Pipeline      | `POST /api/v1/create_pipeline`               | `pipeline_name`, `pipeline_type: "data_lake"`    |
| Create Configuration | `POST /api/v1/create_pipeline_configuration` | `pipeline_id`, `configuration_yaml`              |
| Change State         | `POST /api/v1/change_pipeline_state`         | `desired_state`, `deployed_configuration_id`     |
| Describe Pipeline    | `POST /api/v1/describe_pipeline`             | `pipeline_id`                                    |
| Delete Pipeline      | `POST /api/v1/delete_pipeline`               | `pipeline_id` (pause first; delete tables first) |
| List Tables          | `POST /api/v1/dl/list_tables`                | `virtual_cluster_id`                             |
| Delete Table         | `POST /api/v1/dl/delete_table`               | `table_uuid`                                     |




## References

- [Tableflow GA Announcement](https://www.warpstream.com/blog/warpstream-tableflow-is-now-generally-available)
- [Tableflow Configuration Docs](https://docs.warpstream.com/warpstream/tableflow/tableflow)
- [Query Engine & Catalog Integrations](https://docs.warpstream.com/warpstream/tableflow/catalogs-and-query-engines)
- [Iceberg REST Catalog](https://docs.warpstream.com/warpstream/tableflow/iceberg-catalog)
- [Protobuf with Schema Registry and Tableflow](https://www.warpstream.com/blog/going-all-in-on-protobuf-with-schema-registry-and-tableflow)

