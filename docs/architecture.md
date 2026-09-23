# Architecture

This document defines the target pipeline architecture, the responsibility of each component,
and the interfaces between components. It is the output of roadmap task M1.2.
Later tasks fill in the details of each interface. The owner task of each interface is
named in the [interface table](#interfaces).

[![Architecture diagram. exo1 publishes local TLC Parquet into nyc_raw, reference snapshots into nyc_reference, and provenance into nyc_metadata in the RustFS bucket nyc-taxi. Direct retrieval remains M2.4. The exo2 Spark job validates once and feeds cleaned Parquet and PostgreSQL branches. The dashboard reads the warehouse, while prediction trains on cleaned files and saves a model artifact.](diagrams/architecture.svg)](diagrams/architecture.svg)

[Editable Excalidraw source](diagrams/architecture.excalidraw).

## Data Flow

1. `exo1_data_retrieval` retrieves the monthly TLC yellow taxi Parquet files into the RustFS
   `nyc_raw/` prefix. Exercise 1 requires two retrieval modes:
   - **Direct:** the files are downloaded straight into `nyc_raw/`.
   - **Local staging:** the files are downloaded to local `data/raw`, then uploaded to `nyc_raw/`.
2. `exo2_data_ingestion` runs as one Spark job. It reads `nyc_raw/` and validates the records
   once against the TLC data dictionary.
3. Branch 1 of the job writes the validated data as cleaned Parquet to `nyc_cleaned/`.
4. Branch 2 of the job transforms the same validated data in memory and writes it to the
   PostgreSQL warehouse through JDBC.
5. `exo3_sql_olap` creates the warehouse tables and inserts reference data before branch 2 runs.
6. `exo4_dashboard` reads the warehouse. Its exploratory analysis reads `nyc_cleaned/`.
7. `exo5_ml_prediction_service` trains on `nyc_cleaned/` and saves a model artifact.
   Its inference script loads that artifact.

Both branches consume the same validated dataset. Neither branch reads the output of the other.

Execution dependencies for one complete run:

```text
infrastructure up -> creation.sql -> insertion.sql -> exo2
infrastructure up -> exo1 -> exo2
exo2 branch 1 -> exo5, exo4 exploration
exo2 branch 2 -> exo4 dashboard
```

## Components

| Component | Responsibility | Out of scope | Technology | Tasks |
|---|---|---|---|---|
| TLC trip record data | External source of monthly trip files, data dictionary, and zone lookup | Controlled by the project | HTTPS download | M2.1 |
| `exo1_data_retrieval` | Retrieve the selected months and store them unchanged in `nyc_raw/` | Validation, cleaning, schema changes | Scala, Hadoop S3A | M2.2–M2.4 |
| RustFS | Data lake storing raw files, cleaned files, and reference snapshots | Business logic | S3-compatible object storage | M1.3 |
| Spark cluster | Execute the Spark jobs in the cluster profile | Storage | Spark 4.2.0, one master, two workers | M1.3 |
| `exo2_data_ingestion` | Validate raw data once, then run branch 1 and branch 2 in one job | Creating warehouse tables | Scala, Spark SQL, JDBC | M3.2, M3.3, M4.3 |
| `exo3_sql_olap` | Define the dimensional model, its tables, constraints, reference data, and analytical queries | Loading trip data | PostgreSQL SQL | M4.1, M4.2, M4.4 |
| PostgreSQL | Data warehouse serving analytical queries | Storing raw files | PostgreSQL | M1.3, M4.2 |
| `exo4_dashboard` | Explore cleaned data and visualize warehouse indicators | Writing to the warehouse | Tool selected in M5.2, Marimo for notebooks | M5.1–M5.3 |
| `exo5_ml_prediction_service` | Train and evaluate the fare prediction model, save it, and run inference | Reading the warehouse | Python scripts with UV | M6.1–M6.3 |

## Interfaces

Each interface has one owner task. The owner defines the remaining details and keeps
this table accurate. Consumers depend only on what the interface states.

| ID | Producer to consumer | Contract | Owner |
|---|---|---|---|
| I1 | TLC to `exo1` | May, June, and July 2026 files named `yellow_tripdata_YYYY-MM.parquet`. The [source catalog](data-sources.md) records verified URLs, reported sizes, the dictionary, and the zone lookup. | M2.1 |
| I2 | `exo1` to local disk | Repository-root `data/raw/`, overridable through `RAW_DIR`. Original filenames and bytes, with adjacent version 1 JSON provenance sidecars recording URLs, timestamps, size, SHA-256, schemas, row count, and verification runtimes. Downloads are promoted after a complete local Spark read. Normal reruns verify and reuse matching pairs, while explicit refresh replaces them and retains previous metadata. All staging artifacts are ignored by Git. See the [retrieval workflow](../exo1_data_retrieval/README.md). | M2.2 |
| I3 | `exo1` to RustFS | Bucket `nyc-taxi`. Trip files at `s3a://nyc-taxi/nyc_raw/yellow_tripdata_YYYY-MM.parquet`, identical to verified local sources. Conditional creation prevents overwrites. Uploads and reuse require remote SHA-256 and complete Spark decoding matching local schemas and counts. Versioned provenance and successful-run receipts live under `nyc_metadata/tlc/`. Later stages must not modify accepted objects. | M2.3, M2.4 |
| I4 | RustFS to `exo2` | `exo2` reads the `nyc_raw/` objects of the requested months. | M3.2 |
| I5 | `exo2` branch 1 to RustFS | Cleaned Parquet dataset at `s3a://nyc-taxi/nyc_cleaned/yellow_tripdata/`. Column names, types, and validation rules form the cleaned data contract. | M3.1 (contract), M3.3 (output) |
| I6 | `exo3` to PostgreSQL | Database `nyc_taxi`, schema `dw`. Scripts `exo3_sql_olap/creation.sql` then `exo3_sql_olap/insertion.sql`. Tables and constraints follow the model chosen in M4.1. | M4.2 |
| I7 | `exo2` branch 2 to PostgreSQL | JDBC writes into the existing `dw` tables only. Branch 2 never creates or alters tables. | M4.3 |
| I8 | PostgreSQL to `exo4` | Read-only access to `dw` with the connection settings below. Analytical queries are stored in `exo3_sql_olap/`. | M5.2 (connection), M4.4 (queries) |
| I9 | RustFS to `exo4` and `exo5` | Read-only access to `nyc_cleaned/` following the I5 contract. | M5.1, M6.1 |
| I10 | `exo5` training to inference | Training saves the model artifact in `exo5_ml_prediction_service/models/`. The inference script loads it from there. | M6.3 |
| I11 | Reference staging to RustFS | Original dictionary PDF and zone CSV staged at `data/reference/tlc/<snapshot-id>/`, then published unchanged to `s3a://nyc-taxi/nyc_reference/tlc/<snapshot-id>/` with source filenames. The catalog and machine-readable reference descriptor identify the snapshot and SHA-256 checksums. The exact snapshot was published and remotely verified in M2.3. Its descriptor lives under `nyc_metadata/tlc/references/`. | M2.1 (inventory), M2.3 (publication) |

## Configuration

Components read the connection values below from environment variables, with the local
defaults shown. The local profile runs on the host. The cluster profile runs inside the
Docker Compose network.

| Variable | Local profile | Cluster profile | Used by |
|---|---|---|---|
| `TAXI_MONTHS` | `2026-05,2026-06,2026-07` | same | `exo1`, `exo2` |
| `SPARK_MASTER` | `local[*]` | `spark://spark-master:7077` | `exo1`, `exo2` |
| `S3_ENDPOINT` | `http://localhost:9000` | `http://rustfs:9000` | `exo1`, `exo2`, `exo4`, `exo5` |
| `S3_ACCESS_KEY`, `S3_SECRET_KEY` | `rustfsadmin` | same | `exo1`, `exo2`, `exo4`, `exo5` |
| `S3_BUCKET` | `nyc-taxi` | same | `exo1`, `exo2`, `exo4`, `exo5` |
| `RAW_DIR` | `data/raw` at the repository root | not used | `exo1` |
| `PG_HOST` | `localhost` | `postgres` | `exo2`, `exo3`, `exo4` |
| `PG_PORT` | `5432` | same | `exo2`, `exo3`, `exo4` |
| `PG_DATABASE` | `nyc_taxi` | same | `exo2`, `exo3`, `exo4` |
| `PG_USER` | `nyc_taxi` | same | `exo2`, `exo3`, `exo4` |
| `PG_PASSWORD` | no default | same | `exo2`, `exo3`, `exo4` |
| `PG_SCHEMA` | `dw` | same | `exo2`, `exo3`, `exo4` |

Both M2.2 entry points use the same retrieval engine. The `retrieve` task accepts
optional positional months and directory, overriding the process environment.
The `runMain` entry point uses the environment only. Neither loads `.env`.
Both accept nonempty subsets of the three catalog months and local Spark masters
only. Both sbt launches pass the repository root as `nyctaxi.repositoryRoot`, so
relative `RAW_DIR` values have the same meaning when launched inside the module.
HTTP transfer, JSON provenance, Spark verification, and command orchestration
are separate components. The JDK HTTP client downloads candidates, Spark reads
all columns without transformations, and the provenance store promotes accepted
file/sidecar pairs. A pending marker detects interrupted promotion.

M2.3 adds `upload [months] [rawDir]` and the environment-based
`runMain nyctaxi.publication.RustFsUpload` entry point. Both use the same strict
selection and root-relative staging paths, with `local[2]` as the upload default.
Each run includes the fixed M2.1 reference snapshot. Neither loads `.env`.
The existing SDK handles bucket creation, S3A streams unchanged bytes, and the
shared Spark verifier checks individual remote Parquet objects. HTTP(S) endpoint
settings use path-style access and signing region `us-east-1`. SDK and S3A retry
loops are disabled so publication owns the three-attempt policy.

Publication holds the raw-directory lock and accepts only matching local
provenance. Matching remote objects are verified and reused. Different bytes
fail without replacement. There is no transaction across objects, so a failed
run may leave successfully published objects available for a later rerun.
Only a completely verified run receives a new immutable receipt. The
[publication guide](../exo1_data_retrieval/docs/publication.md) defines metadata
keys, receipt fields, failure behavior, and recovery.

The Spark JDBC URL is `jdbc:postgresql://${PG_HOST}:${PG_PORT}/${PG_DATABASE}`.
A dashboard tool that does not read environment variables uses the same connection values.
The RustFS credentials are the development values of the local stack. `PG_PASSWORD` is read
from `.env`, which each collaborator creates from the committed `.env.example` and never
commits. Docker Compose refuses to start when it is unset.

## Design Requirements

These requirements apply to every stage. They follow from the repeatable execution asked in
M2.4 and the full reproduction in M8.1. Each owner task implements them, and M8.1 verifies them.

- A month is written `YYYY-MM` in configuration and file names.
- Rerunning a stage for a month must not duplicate data in `nyc_cleaned/` or in the warehouse.
- Raw objects in `nyc_raw/` must not be modified after retrieval.

## Decisions

| ID | Decision | Rationale |
|---|---|---|
| D1 | The assignment's `nyc_raw` folder is the `nyc_raw/` prefix of the bucket `nyc-taxi`, which also holds `nyc_cleaned/` | S3 storage has no real folders, only key prefixes inside a bucket, and S3 bucket names cannot contain `_`. |
| D2 | Raw objects are unchanged copies of the source | Later stages can be rebuilt from the lake without downloading again. |
| D3 | One Spark job validates once and feeds both branches | The assignment requires a single branching job, and both branches must share the same validated data. |
| D4 | Warehouse tables are created only by the `exo3` SQL scripts | The assignment requires the tables from Exercise 3 to exist before branch 2 runs. |
| D5 | The prediction service reads cleaned Parquet, not the warehouse | The assignment places ML training data on RustFS, which keeps ML independent of the dimensional model. |
| D6 | The dashboard has read-only access to the warehouse | Branch 2 stays the only writer of trip data, so warehouse content is traceable to the ingestion job. |
| D7 | Shared environment variable names across components | Local runs, cluster runs, and optional orchestration in M7.1 use the same configuration. |
| D8 | Each Python component is its own UV project | The exercise READMEs require UV for any Python component. |
| D9 | The model artifact lives in `exo5_ml_prediction_service/models/` | Training and inference are separate scripts that exchange a saved model, as required by the assignment and M6.3. |
| D10 | Dictionary and zone lookup snapshots live in `nyc_reference/tlc/<snapshot-id>/` in the RustFS bucket `nyc-taxi`. Snapshot IDs use UTC `YYYYMMDDTHHMMSSZ`. | Timestamped, unchanged references preserve provenance for contract design and warehouse reference data. Updates create new snapshots. Local staging is ignored by Git. |

## Open Points

These points belong to later tasks and are not resolved here.

- **M2.4** defines how `exo1` runs in the cluster profile. **M3.2** and **M4.3** do the same for `exo2` and its JDBC driver.
- **M3.1** defines the cleaned data contract, including partitioning, time handling, and whether rejected records are kept.
- **M4.1** chooses a star, snowflake, or constellation model and justifies it in the report.
- **M4.3** defines how a month is reloaded in the warehouse without duplicates.
- **M5.1** chooses where the Marimo notebooks are stored.
- **M5.2** selects the dashboard tool.
- **M6.1** chooses the fare column used as the prediction target and the input features.
- **M6.3** defines the artifact format. **M6.2** and **M6.3** implement the input tests required for training and inference.
