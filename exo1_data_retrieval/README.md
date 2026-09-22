# Local TLC Retrieval

M2.2 retrieves the original yellow-taxi Parquet files for May–July 2026 into
repository-root `data/raw/`. HTTP transfer, provenance storage, and local Spark
verification have separate responsibilities. No Docker services are needed.
RustFS uploads belong to M2.3 and direct retrieval into RustFS to M2.4.

## Run

Select JDK 21 through `JAVA_HOME` using the [Scala toolchain guide](../docs/scala-toolchain.md).
From the repository root, use Bash or Zsh:

```bash
cd exo1_data_retrieval
sbt shutdown
export TAXI_MONTHS='2026-05,2026-06,2026-07'
export RAW_DIR='data/raw'
export SPARK_MASTER='local[2]'
sbt --batch 'retrieve; shutdown'
```

For PowerShell:

```powershell
Set-Location exo1_data_retrieval
sbt shutdown
$env:TAXI_MONTHS = '2026-05,2026-06,2026-07'
$env:RAW_DIR = 'data/raw'
$env:SPARK_MASTER = 'local[2]'
sbt --batch 'retrieve; shutdown'
```

The `retrieve` task accepts optional positional months and directory, followed
by optional `--refresh`. Positional values override only their corresponding
environment variables. For example, these commands work in either shell:

```bash
sbt --batch 'retrieve 2026-05 data/raw; shutdown'
sbt --batch 'retrieve --help; shutdown'
```

The environment-only entry point remains available and uses the same retrieval
engine and validation:

```bash
sbt --batch 'runMain nyctaxi.retrieval.LocalRetrieval; shutdown'
sbt --batch 'runMain nyctaxi.retrieval.LocalRetrieval --help; shutdown'
```

Stop the sbt server before changing environment variables or `JAVA_HOME`.
The application reads process environment variables, not `.env`. The launch
configuration passes the repository root as a JVM property, so relative
`RAW_DIR` values resolve there even though sbt starts inside this module.
Absolute paths are also accepted. Custom directories outside `data/raw/` need
their own Git ignore rules if they are inside a repository.

| Variable | Default | Accepted values |
|---|---|---|
| `TAXI_MONTHS` | `2026-05,2026-06,2026-07` | A nonempty subset, without duplicates, in requested order |
| `RAW_DIR` | `data/raw` relative to repository root | A writable local directory supporting atomic file replacement |
| `SPARK_MASTER` | `local[*]` | Local Spark masters such as `local[2]`, never a cluster URL |

The command uses the selected JDK in a forked JVM with a 2 GB heap and Spark's
module-access options. `retrieve` resolves sbt's classpath and waits for its
child JVM. `runMain` delegates to sbt's foreground forked runner, which also
completes before `shutdown` can remove a background-job classpath.
The application processes months sequentially and prints their outcomes.
It returns 0 on complete success, 1 on retrieval failure,
2 on invalid configuration, and 130 on interruption. sbt can translate a failed
child exit into its own nonzero status. After a failed sbt command, run
`sbt shutdown` separately because subsequent commands may not execute.
Spark writes its logs to stderr, which sbt may label `[error]` even for INFO
messages. Check the per-month results, summary, and exit status.

## Verification and Reruns

Transfers use the official [catalog URLs](../docs/data-sources.md), retaining
filenames and content. Each attempt has a 30-second connection timeout and a
five-minute deadline covering the complete body. Transient connection failures,
timeouts, HTTP 429, and HTTP 5xx receive at most three attempts, with one- and
two-second delays. Other HTTP errors fail immediately. A failed month does not
prevent later months from running. User interruption stops the run.

Each candidate must have HTTP 200, nonempty content, and the advertised GET
`Content-Length` when present. Spark then reads every column of every row,
without cleaning, filtering, merging monthly schemas, or rewriting the file.
Missing and corrupt files are never skipped. A SQL `count()` alone is not the
verification method.

Successful downloads have adjacent `<filename>.metadata.json` sidecars with
format version 1, source and final URLs, UTC retrieval time, size, SHA-256,
Parquet and Spark schemas, decoded row count, verification time, and runtime
versions. These observations describe source data rather than define the M3.1
cleaning contract. A computed checksum identifies local bytes, not an
independently authenticated upstream checksum.

Repeat the same command from a fresh sbt process. Existing files are reused
only when their metadata, byte sizes, and checksums match. Every reused file
receives another full Spark read, and its schemas and row count must match.
Normal reuse makes no HTTP request and leaves accepted sidecars unchanged.

For an explicit refresh, set `TAXI_MONTHS` to the desired subset before starting
a fresh sbt process, then run:

```bash
sbt --batch 'retrieve --refresh; shutdown'
```

Alternatively, override the selection with positional arguments:

```bash
sbt --batch 'retrieve 2026-05 data/raw --refresh; shutdown'
```

The alternative entry point accepts
`runMain nyctaxi.retrieval.LocalRetrieval --refresh` and takes months and directory
from the environment.

A refresh downloads and verifies candidates before replacing existing files.
Transfer or verification failure preserves the accepted pair. Previous metadata
is retained in `.history/<filename>/`. Previous Parquet bytes are not archived,
so copy them elsewhere first if that source revision must be retained. Review
changed checksums and update the Git-tracked source catalog explicitly.

## Recovery

- Downloads from the earlier existence-only workflow have no sidecars. Inspect
  them, then explicitly refresh to obtain verified file/metadata pairs. No metadata
  is fabricated for existing bytes.
- A missing sidecar, changed checksum, unsupported metadata version, or incomplete
  pair fails that month. Inspect the cause before using `--refresh`.
- File and sidecar replacements are individually atomic. A `<filename>.pending`
  marker detects a crash between replacements, including refreshes with identical
  content. A subsequent normal run refuses that pair. A successful explicit
  refresh repairs it and removes the marker.
- Handled failures remove their temporary files. After a forced process kill,
  abandoned `.part` files may remain. They are ignored by subsequent runs and can
  be removed when no retrieval process is running.
- An OS file lock prevents concurrent writers to one `RAW_DIR`. The persistent
  `.retrieval.lock` file is harmless after exit. The OS releases its lock.
- Inspect individual month results even when some succeed. The final status is
  failure if any requested month failed.

Raw files, sidecars, history, and temporary files under `data/raw/` are ignored.
They are not included in a clone. The [source catalog](../docs/data-sources.md)
records the project's accepted download evidence for collaborators.

## Tests

Inside this module:

```bash
sbt --batch 'compile; testFull; shutdown'
sbt --batch 'testFull; shutdown'
```

Tests use a local HTTP fixture server and tiny generated Parquet files. They
cover retries, deadlines, interrupted and truncated transfers, provenance
conflicts, reuse, refresh, and continuing after a month fails. A damaged column
chunk with a readable footer proves the verifier actually decodes columns.
That negative test deliberately emits a Spark error before passing. Adapter tests
cover positional precedence, defaults, refresh syntax, and shared validation.
The existing `ToolchainSpec` still verifies pinned runtime versions.

Initial downloads were verified on 21 September 2026. The integrated workflow
was verified on **22 September 2026** with macOS 26.5 ARM64, JDK 21.0.12,
sbt 2.0.9, Scala 2.13.18, and Spark 4.2.0. All **20 tests** passed twice in
fresh processes. Both entry points fully reread all three monthly files with
`local[2]`, preserving data and sidecar bytes and modification times. CLI checks
also verified help, positional precedence, nonzero error exits, and refusal to
reuse files without metadata. See the
[catalog evidence](../docs/data-sources.md#verified-local-downloads--m22).
Windows, Linux, and other JDK distributions have not been tested.
