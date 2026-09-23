# RustFS Publication

M2.3 publishes verified local May–July 2026 Parquet files and reference snapshot
`20260919T204934Z`. It preserves original bytes and filenames. It performs no
TLC download, cleaning, schema harmonization, or warehouse loading.

## Prerequisites and Run

Select JDK 21 through `JAVA_HOME` using the [toolchain guide](../../docs/scala-toolchain.md).
Run [local retrieval](../README.md) first. Keep the reference PDF and CSV at
repository-root `data/reference/tlc/20260919T204934Z/`. Their expected checksums
are in the [source catalog](../../docs/data-sources.md#reference-snapshot) and
the committed [reference descriptor](../src/main/resources/tlc-reference-snapshot.json).
The files themselves are ignored and are not included in a clone.

Start the project's RustFS service. On a fresh setup, create the ignored `.env`
as described in the [root setup](../../README.md#setup), then run
`docker compose up -d rustfs` from the repository root. If this project's
container already exists, start that container instead. A container-name
collision with another project requires a local Compose override for this
project's `container_name`, preserving the `rustfs` service and existing volume.
The verified workstation uses `ing3_iaa_gb_bd-rustfs`. PostgreSQL and the Spark
containers are not needed because verification uses local Spark on the host.

From the repository root, use Bash or Zsh:

```bash
cd exo1_data_retrieval
sbt shutdown
export TAXI_MONTHS='2026-05,2026-06,2026-07'
export RAW_DIR='data/raw'
export SPARK_MASTER='local[2]'
export S3_ENDPOINT='http://localhost:9000'
export S3_BUCKET='nyc-taxi'
sbt --batch 'upload; shutdown'
```

For PowerShell:

```powershell
Set-Location exo1_data_retrieval
sbt shutdown
$env:TAXI_MONTHS = '2026-05,2026-06,2026-07'
$env:RAW_DIR = 'data/raw'
$env:SPARK_MASTER = 'local[2]'
$env:S3_ENDPOINT = 'http://localhost:9000'
$env:S3_BUCKET = 'nyc-taxi'
sbt --batch 'upload; shutdown'
```

The default access and secret keys are the stack's documented development
credentials. Export `S3_ACCESS_KEY` and `S3_SECRET_KEY` when using different
credentials. The application reads process environment variables and does not
load `.env`. Restart the sbt server after environment or JDK changes.

Commands supported in either shell:

```bash
sbt --batch 'upload 2026-05 data/raw; shutdown'
sbt --batch 'upload --help; shutdown'
sbt --batch 'runMain nyctaxi.publication.RustFsUpload; shutdown'
sbt --batch 'runMain nyctaxi.publication.RustFsUpload --help; shutdown'
```

`upload [months] [rawDir]` overrides the corresponding environment values.
`runMain` accepts only standalone `--help` or no arguments and uses the
environment. Both reject `--refresh`. Every run includes both reference files,
even when selecting only one month.

| Setting | Default | Constraint |
|---|---|---|
| `TAXI_MONTHS` | `2026-05,2026-06,2026-07` | Nonempty subset without duplicates |
| `RAW_DIR` | Repository-root `data/raw` | Existing verified local downloads |
| `SPARK_MASTER` | `local[2]` | Local masters only |
| `S3_ENDPOINT` | `http://localhost:9000` | HTTP(S) origin without embedded credentials |
| `S3_BUCKET` | `nyc-taxi` | Valid bucket name |
| `S3_ACCESS_KEY`, `S3_SECRET_KEY` | `rustfsadmin` | Nonempty, never recorded in receipts |

S3A uses path-style addressing, signing region `us-east-1`, and TLS when the
endpoint uses HTTPS. Both launch paths use the selected JDK, a 2 GB forked JVM,
Spark module options, visible output, and foreground completion before shutdown.

## Objects and Acceptance

The command creates the bucket if absent. Local provenance must match the
selected source, size, and SHA-256, with no pending promotion marker. The raw
directory's existing lock prevents simultaneous retrieval and publication.

| Objects | Key inside the bucket |
|---|---|
| Original trip files | `nyc_raw/yellow_tripdata_YYYY-MM.parquet` |
| Original PDF and CSV | `nyc_reference/tlc/20260919T204934Z/<filename>` |
| Unchanged trip sidecars | `nyc_metadata/tlc/trips/<sidecar-sha256>/<filename>.metadata.json` |
| Reference descriptor | `nyc_metadata/tlc/references/20260919T204934Z.json` |
| Successful run receipt | `nyc_metadata/tlc/publications/<UTC-timestamp>-<UUID>.json` |

Conditional S3A creation prevents overwrites, including competing writers.
Copies are streamed and checked before completion. Every uploaded or reused
object receives an independent remote size and SHA-256 check. Each trip file
also receives a complete Spark read of every column and row, compared with
the recorded schemas and row count. Neither ETags nor a standalone SQL count
are treated as proof of these checks.

Only a fully successful run writes a version 1 receipt. It records source and
provenance keys, sizes, checksums, uploaded/reused outcomes, verification times,
schemas, counts, and runtime versions. Metadata and receipts are also read back
and checked. Reference descriptor version 1 pins the M2.1 retrieval time,
filenames, URLs, sizes, hashes, and dictionary revision.

## Reruns and Recovery

- Repeat the command to verify and reuse matching objects. A successful rerun
  adds one receipt without replacing existing data or provenance.
- Different remote bytes fail the item. Inspect the conflict and preserve the
  existing object. This command never repairs conflicts by overwriting or deleting
  them. A source revision requires a separately agreed storage revision.
- Missing local metadata or a pending local promotion must be resolved through
  the [retrieval recovery workflow](../README.md#recovery) before publication.
- A changed reference file cannot represent this snapshot. Restore matching
  staged bytes or define a new snapshot in a separate task.
- Publication is not a transaction across all objects. Successful objects remain
  after a later failure. A rerun verifies them and can fill missing provenance.
  A run without a verified receipt is incomplete.
- Transient storage failures receive three attempts with one- and two-second
  backoff. Each operation has a five-minute deadline, including streamed bodies,
  and a 30-second connection timeout. Nested SDK and S3A retries are disabled.
  A lost write response is reconciled by checking the destination before retrying.
- Handled failures and interruption abort incomplete streams. A forced process
  kill can leave an incomplete multipart upload. Inspect and remove only that
  abandoned upload when no writer is active. There is no bucket-wide automatic purge.
- Item failures allow later independent files to proceed. Authentication failure
  and interruption stop the run. Local files and accepted objects are retained.

Exit codes are 0 for complete success, 1 for publication failure, 2 for invalid
configuration, and 130 for interruption. sbt can translate child failures to
its own nonzero status. After a failed command, run `sbt shutdown` separately.
Spark logs can appear under sbt's `[error]` prefix even for informational output.
Use the per-file outcomes, final summary, and exit status.

## Tests and Verified Platform

Ordinary tests need no Docker or TLC access:

```bash
sbt --batch 'compile; testFull; shutdown'
sbt --batch 'testFull; shutdown'
```

With RustFS running, explicitly invoke the integration suite:

```bash
sbt --batch 'testOnly nyctaxi.publication.RustFsUploadIntegrationSpec; shutdown'
```

It is excluded from ordinary discovery. It creates unique `m23-test-*` buckets
and removes its own fixtures. Run it before publishing against an unverified
RustFS installation. Failure of conditional-write checks blocks publication.

On **22 September 2026**, all **31 ordinary tests** passed in two fresh sbt
processes, and all **3 RustFS integration tests** passed. Integration covered
competing single and multipart writes, abort cleanup, and remote corrupt-column
rejection with a readable Parquet footer. Both command entry points verified
the five real sources, including **11,458,193 trip rows**. Independent readback
confirmed matching hashes, unchanged accepted remote objects, and unchanged
local file bytes and timestamps. See the [publication evidence](../../docs/data-sources.md#verified-rustfs-publication--m23).

Tested: macOS 26.5 ARM64, JDK 21.0.12, Scala 2.13.18, sbt 2.0.9, Spark 4.2.0,
Hadoop S3A 3.5.0, AWS SDK 2.35.4, and RustFS 1.0.0 in a Linux x86-64 container.
Other operating systems and storage implementations have not been tested.
