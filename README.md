# NYC Taxi Data Engineering

A CY Tech Big Data coursework project using New York City taxi trip records.
The project covers data retrieval, ingestion and validation, SQL analytics,
a dashboard, and a fare prediction service.

## Current Status

The repository contains exercise scaffolding, a pinned Scala toolchain with
local Spark smoke tests, a verified Docker Compose stack for RustFS,
PostgreSQL, and a Spark cluster, and local retrieval and RustFS publication for exercise 1.
The remaining application code and the end-to-end pipeline are not implemented
yet. Independent UV projects provide pinned Python development
tools and a verified Marimo smoke notebook. M1.1 development toolchain checks and
M1.3 infrastructure checks are complete. M2.1 source identification is complete,
and M2.2 stages and fully verifies the three monthly trip files locally. M2.3
publishes the trip files, dictionary, zone lookup, and provenance to RustFS,
with remote checksum checks and full Spark reads. M2.4 direct retrieval is ready
to start.

## Project Structure

![Project folder map showing data retrieval, ingestion, SQL analytics, dashboard, and fare prediction modules.](docs/diagrams/project-structure.svg)

See the [assignment specification](docs/instructions/instructions.pdf) and
[course instructions](docs/instructions/README.md) for requirements and submission details.

## Architecture

[![Pipeline architecture showing verified trip, reference, and provenance publication into RustFS, one Spark ingestion job with cleaned Parquet and PostgreSQL branches, the dashboard, and fare prediction.](docs/diagrams/architecture.svg)](docs/architecture.md)

See the [architecture document](docs/architecture.md) for component responsibilities,
interfaces, configuration, and design decisions.

## Data Source

The base dataset is the [NYC Taxi and Limousine Commission (TLC) Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).
TLC publishes monthly Parquet files and provides data dictionaries and taxi zone
lookup tables on that page. The project will use yellow taxi trip records for
**May, June, and July 2026**.

See the [source catalog](docs/data-sources.md) for verified monthly URLs and
actual sizes, checksums, row counts, and schemas, plus dictionary definitions,
zone lookup details, and reference snapshot checksums and destinations.

## Setup

Both Scala modules use **JDK 21 LTS**, **sbt 2.0.9**, **Scala 2.13.18**,
and **Apache Spark 4.2.0**. Follow the [Scala toolchain setup](docs/scala-toolchain.md)
to select `JAVA_HOME`, install the sbt runner, and verify the actual runtime.

Build and test each module independently from the repository root:

```bash
(cd exo1_data_retrieval && sbt --batch 'compile; testFull; shutdown')
(cd exo2_data_ingestion && sbt --batch 'compile; testFull; shutdown')
```

Each module uses ScalaTest **3.2.19** to start Spark with `local[2]`, count ten
generated rows, and stop Spark. These checks require no Docker services or taxi
data. `testFull` executes all tests even after a previous successful run.

For infrastructure work, install Docker with Docker Compose. Copy the committed
template to `.env` and set `PG_PASSWORD`. Compose refuses to start without it, and
`.env` is never committed. Run from the repository root:

```bash
cp .env.example .env
docker compose up -d
```

The configuration defines RustFS, PostgreSQL, one Spark master, and two Spark
workers with 2 cores and 2g each. The RustFS console is at <http://localhost:9001>
and the Spark master UI is at <http://localhost:8080>, with the worker UIs on
`8081` and `8082`. RustFS uses `rustfsadmin` for both the development access key
and secret key. Its S3 API is exposed on port `9000`. PostgreSQL serves the
`nyc_taxi` database on port `5432`.

The containers receive the cluster profile of the
[configuration table](docs/architecture.md#configuration): `http://rustfs:9000`,
`spark://spark-master:7077`, and the `postgres` host. Tools running on the host
keep the local profile values from `.env`. The Spark containers mount
`spark-defaults.conf`, which configures S3A access to RustFS.

Python components in exercises 4 and 5 use **CPython 3.14.7** managed by
**UV 0.12.17**. Each project owns its environment and committed lockfile.
Follow the [Python toolchain setup](docs/python-toolchain.md) to install the
tools, verify the runtime, and open the dashboard's **Marimo** smoke notebook.

Run inside either Python project:

```bash
uv python install 3.14.7
uv sync --locked
uv run --locked pytest
uv run --locked flake8 .
```

These checks require no Docker services or taxi data. Application dependencies
will be added with the dashboard and prediction features.

Stop the infrastructure while retaining the RustFS and PostgreSQL data volumes:

```bash
docker compose down
```

## Data Retrieval

Exercise 1 stages the three monthly files in repository-root `data/raw/`,
retaining source names and bytes. Each download receives a complete local Spark
read and a JSON provenance sidecar. Existing files require matching metadata,
size, and SHA-256 before another full read. Staging files are ignored by Git.

After selecting JDK 21, run from the repository root:

```bash
(cd exo1_data_retrieval && sbt --batch 'retrieve; shutdown')
```

`TAXI_MONTHS` and `RAW_DIR` override the defaults. Optional positional arguments
have priority over these environment variables:

```bash
(cd exo1_data_retrieval && sbt --batch 'retrieve 2026-05 data/raw; shutdown')
```

See the [retrieval guide](exo1_data_retrieval/README.md) for PowerShell commands,
local Spark configuration, explicit refresh, recovery, and verification.
Existing downloads without sidecars require explicit refresh. With the project's
RustFS service running and the cataloged reference files staged locally, publish
verified sources from inside `exo1_data_retrieval`:

```bash
sbt --batch 'upload; shutdown'
```

Matching remote objects are verified and reused. Conflicting objects are never
overwritten. See the [publication guide](exo1_data_retrieval/docs/publication.md)
for configuration, provenance, recovery, and RustFS integration tests.

## Roadmap

[![Project roadmap showing eight milestones, parallel dashboard and prediction branches, optional extensions, and transversal activities. A red dot marks M2 Data Retrieval as the current stage.](docs/diagrams/roadmap.svg)](docs/roadmap.md)

See the [detailed roadmap](docs/roadmap.md) for the task Gantt, prerequisites, and expected outputs.

## Collaborators

- [Maxime CRAYSSAC](https://github.com/mcrayssac)
- [PAUL PITIOT](https://github.com/Paul-Pitiot-Cytech-Grp7-Mi)

## License

Project-authored code and documentation are licensed under the
[Apache License 2.0](LICENSE).

Copyright 2026 Maxime CRAYSSAC and PAUL PITIOT.

External TLC datasets and supplied course materials retain their respective
terms and are not covered by this project license.
