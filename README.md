# Fall 2026 WarpStream Tableflow Demo

This is a hands on workshop for WarpStream Tableflow, created in fall of 2026. Once we use WarpStream Tableflow to create Apache Iceberg tables out of WarpStream kafka topics, we will then query them with DuckDB.

## What is WarpStream Tableflow

[WarpStream Tableflow](https://www.warpstream.com/tableflow) automatically materializes Kafka topics as Apache Iceberg tables in your object storage. It is the simplest path from streaming data to a queryable lakehouse.

- Reads from **[any Kafka-compatible source](https://docs.warpstream.com/warpstream/tableflow/tableflow#configure-source-clusters)** (OSS Kafka, MSK, Confluent Cloud, WarpStream)
- Supports **[JSON, Avro, and Protobuf](https://docs.warpstream.com/warpstream/tableflow/tableflow#schema-definitions)** formats with [inline schema definitions](https://docs.warpstream.com/warpstream/tableflow/tableflow#schema-definitions) ([Schema Registry mode](https://docs.warpstream.com/warpstream/tableflow/tableflow#schema-registry-mode) also supported)
- Deploys as a **[single stateless binary](https://docs.warpstream.com/warpstream/agent-setup/deploy)** that auto-scales via [Kubernetes HPA](https://docs.warpstream.com/warpstream/kafka/advanced-agent-deployment-options/reducing-infrastructure-costs)
- **[Zero-ops table maintenance](https://docs.warpstream.com/warpstream/tableflow/tableflow#introduction)**: compaction, snapshot cleanup, orphan file cleanup, and [data expiration](https://docs.warpstream.com/warpstream/tableflow/tableflow#data-retention-and-ttl) run automatically in the background
- **[BYOC deployment](https://docs.warpstream.com/warpstream/tableflow/tableflow#introduction)**: agents run in your VPC; data stays in your [object storage bucket](https://docs.warpstream.com/warpstream/agent-setup/different-object-stores)
- [Integrates](https://docs.warpstream.com/warpstream/tableflow/catalogs-and-query-engines) with **BigQuery, Databricks, Snowflake, AWS Glue, DuckDB, ClickHouse, Trino, Athena**

WarpStream Tableflow handles the entire lifecycle — ingestion, schema evolution, partitioning, compaction, and retention — in a single declarative YAML configuration.

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

WarpStream Tableflow exposes a built-in [Iceberg REST Catalog](https://docs.warpstream.com/warpstream/tableflow/iceberg-catalog) and pushes metadata to external catalogs: BigQuery, Databricks Unity Catalog, AWS Glue, HMS (WIP), and Snowflake. Note support for Delta format in WarpStream tableflow is a WIP and should be GA before the end of 2026. 

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


All topics use **protobuf** with `wire_format: raw` (no schema registry required). Tableflow ingests `clickstream` and `orders` into Iceberg tables partitioned by hour; `inventory_updates` is unpartitioned (matches `scripts/configure-tableflow.sh`).

## Lab setup steps and flow summary

1. Clone the repo and install tooling (Python, DuckDB, WarpStream CLI, `jq`)
2. Run a local, ephermeral/free WarpStream demo environment called `warpstream playground`
3. Produce events, configure Tableflow, wait for Parquet + Iceberg metadata, query with DuckDB (`run-demo.sh` cleans the local Iceberg dir for you)

By this point, you have WarpStream Tableflow up and running. You can optionally continue on with these two steps, to see some additionally queries that interrogate the actual parquet files and and what happens when you evolve a table against the Tableflow pipeline. 

1. Run additionally queries (`query-retail.sh`)
2. Evolve the orders schema → poll until `discount_code` shows up (`evolve-schema.sh`, `poll-discount.sh`)



## Prerequisites


| Tool                           | Purpose                                    |
| ------------------------------ | ------------------------------------------ |
| Python 3.9+ + `pip`            | Protobuf producer scripts                  |
| `curl`                         | DuckDB installer + WarpStream Pipeline API |
| WarpStream CLI                 | Local playground                           |
| DuckDB CLI + Iceberg extension | Query materialized Iceberg tables          |
| `jq`                           | JSON formatting for API responses          |


Follow the sections below in order.

Clone the repo: 

```bash
git clone https://github.com/sami2ahmed/ws-tableflow-lab.git
cd ws-tableflow-lab
```



## 1. Python setup

Create a project-local virtual environment and install dependencies from `requirements.txt` (`confluent-kafka`, `protobuf`):

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

To activate the venv again if needed in other shells etc.: 

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

Verify (in a new shell, re-export PATH if needed):

```bash
export PATH="$HOME/.duckdb/cli/latest:$PATH"
duckdb --version
```

Install the Iceberg extension (one-time). Load it again in each DuckDB session:

```bash
duckdb -c "INSTALL iceberg; LOAD iceberg;"
```



## 3. WarpStream CLI

Install the WarpStream Agent / CLI with Homebrew or the installation script:

[Install the WarpStream Agent / CLI](https://docs.warpstream.com/warpstream/getting-started/install-the-warpstream-agent?select=brew,installation-script#brew)

## 4. jq (JSON formatter)

Used by the configure / evolve scripts to parse Pipeline API responses:

```bash
# macOS
brew install jq

# Linux
apt-get install jq
```



## 5. Start WarpStream Playground

Keep this process running in a dedicated terminal (do this before filling `.env`):

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

Copy and paste the URL into your browser, and keep the playground running in a shell. 

## 6. Environment variables

Create your own `.env`: 

```bash
cp .env.example .env
```

Fill these from values in the Warpstream console (in your browser): 


| Variable             | Value                                                 |
| -------------------- | ----------------------------------------------------- |
| `BASE_URL`           | `https://api.warpstream.com` (do not edit)            |
| `API_KEY`            | Console → **API Keys** → (`aks_...`)                  |
| `VIRTUAL_CLUSTER_ID` | The **Tableflow** cluster id (`vci_dl_...`)           |
| `BROKER`             | `localhost:9092` (playground Kafka port, do not edit) |
| `PIPELINE_ID`        | Leave blank; `configure-tableflow.sh` writes them     |
| `CONFIG_ID`          | Leave blank; `configure-tableflow.sh` writes them     |


- Note the console landing URL opens with the **Kafka** cluster (`vci_...`). For `VIRTUAL_CLUSTER_ID`, switch to the **Tableflow** cluster (`vci_dl_...`) tab within the Warpstream console to avoid copying the wrong id.

`.env` is gitignored. But make sure you do not commit real keys to a repo, in case you clone this and push etc. 

## Setup checklist

- [ ] Repo cloned and `.venv` + `pip install -r requirements.txt` succeeded
- [ ] `duckdb --version` works (`export PATH="$HOME/.duckdb/cli/latest:$PATH"` if needed) and `INSTALL iceberg` succeeded
- [ ] `warpstream` CLI installed
- [ ] `jq` installed
- [ ] `warpstream playground` running (ports 9092 / 9094 / 8081)
- [ ] `.env` filled out

With playground already running in another terminal, tooling installed, and `.env` filled:

## Run the demo (scripts)



### Demo scripts (second shell, WarpStream Playground should be running in another shell)

Activate the venv, then run the scripts in order (they load `.env` themselves):

```bash
source .venv/bin/activate
export PATH="$HOME/.duckdb/cli/latest:$PATH"

cd scripts
bash run-demo.sh
bash query-retail.sh
bash evolve-schema.sh
bash poll-discount.sh
```


| Script             | What it does                                                                                              |
| ------------------ | --------------------------------------------------------------------------------------------------------- |
| `run-demo.sh`      | Wipe/seed Iceberg dir, produce events, configure Tableflow, wait for Parquet + metadata, print row counts |
| `query-retail.sh`  | Extra DuckDB retail queries (GMV/AOV, payment mix, etc.)                                                  |
| `evolve-schema.sh` | Add optional `discount_code` to the orders Tableflow config                                               |
| `poll-discount.sh` | Poll until non-null `discount_code` appears in `iceberg_scan`                                             |




## Teardown (do this each time you want to rerun the demo)

within the scripts dir: 

```bash
bash cleanup.sh
```

within the shell running the WarpStream Playground, quit the process e.g. 

```bash
control + c
```



## To rerun the demo

1. Start from [Step 5](#5-start-warpstream-playground) (e.g. spin up `warpstream playground` again in a shell)
2. Fill out the unique values for you `.env` again
3. Then, in another shell (keep playground running in the other), [rerun the demo scripts](#run-the-demo-scripts)



## Key takeaways

- **One config, multiple topics**: a single Tableflow YAML replaces separate Kafka Connect + S3 sink + Spark compaction pipelines per topic
- **Protobuf-native**: raw protobuf wire format works without a schema registry; inline `input_schema` maps proto fields to Iceberg columns
- **Schema evolution without pipeline downtime**: added fields via the Pipeline API; old rows return `null` for new columns
- **Zero operational overhead**: compaction, snapshot cleanup, orphan file cleanup, and retention run in the background

## Some notes on the demo

You might notice that the `poll-discount.sh` takes a while to complete. This is because Parquet files appear within seconds and the Iceberg metadata (`v*.metadata.json`) takes longer. 

**Parquet** is the data layer. Once Tableflow reads Kafka and flushes a batch, it writes `.parquet` under the table’s `data/` dir. That shows up on disk quickly (often seconds). 

`v*.metadata.json` is the Iceberg catalog/snapshot layer. Analytical engines like DuckDB, Snow, Athena etc.`iceberg_scan` do not discover Parquet files by scanning the folder — they read Iceberg metadata (metadata JSON → manifests → data files), and that metadata is only written when Tableflow **commits a snapshot.** 

## References

- [Tableflow GA Announcement](https://www.warpstream.com/blog/warpstream-tableflow-is-now-generally-available)
- [Tableflow Configuration Docs](https://docs.warpstream.com/warpstream/tableflow/tableflow)
- [Query Engine & Catalog Integrations](https://docs.warpstream.com/warpstream/tableflow/catalogs-and-query-engines)
- [Iceberg REST Catalog](https://docs.warpstream.com/warpstream/tableflow/iceberg-catalog)
- [Protobuf with Schema Registry and Tableflow](https://www.warpstream.com/blog/going-all-in-on-protobuf-with-schema-registry-and-tableflow)

