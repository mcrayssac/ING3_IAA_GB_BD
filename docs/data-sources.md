# TLC Data Sources

M2.1 identifies the inputs for yellow taxi trips in **May, June, and July 2026**.
The [official TLC source page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
lists all three monthly files, the yellow-taxi dictionary, and the zone lookup.
Source checks passed on **2026-09-19**. M2.2 downloaded and fully read the monthly
files on **2026-09-21**. The reference snapshot remains staged locally.
**RustFS publication is pending M2.3. M1.3 infrastructure verification is complete.**

## Monthly Trip Files

The following direct URLs returned HTTP 200 to HTTPS HEAD requests on
2026-09-19 at approximately 20:47 UTC. Sizes are the reported `Content-Length`
values in bytes.

| Month | Official file and download URL | Reported bytes | HTTP status |
|---|---|---:|---|
| 2026-05 | [yellow_tripdata_2026-05.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-05.parquet) | 69,699,174 | 200 |
| 2026-06 | [yellow_tripdata_2026-06.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-06.parquet) | 65,465,637 | 200 |
| 2026-07 | [yellow_tripdata_2026-07.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-07.parquet) | 61,685,033 | 200 |

No monthly file bodies were downloaded during M2.1. Those availability and size
checks alone did not verify Parquet readability, row counts, or column types.
The M2.2 observations below describe the subsequently downloaded files. These
observations are dated because TLC can replace files at the same URLs.
Local staging and raw object names follow
[architecture interfaces I2 and I3](architecture.md#interfaces).

### Verified Local Downloads — M2.2

All three HTTPS GET requests returned HTTP 200 on **2026-09-21**. Original and
final URLs are the monthly URLs above. Actual lengths matched both their GET
`Content-Length` headers and the earlier HEAD observations. Source bytes and
filenames are unchanged under repository-root `data/raw/`.

| Month | Retrieval time on 2026-09-21 (UTC) | Actual bytes | Fully decoded rows | Columns |
|---|---|---:|---:|---:|
| 2026-05 | 20:42:14.463509 | 69,699,174 | 4,090,836 | 20 |
| 2026-06 | 20:42:21.109842 | 65,465,637 | 3,837,248 | 21 |
| 2026-07 | 20:42:24.413242 | 61,685,033 | 3,530,109 | 21 |

SHA-256 checksums, independently recomputed from the accepted files:

```text
9aa5a1609e2bf07d9051b7d530de05b1019e12a560ecb2c59c137c8b3a8b6750  yellow_tripdata_2026-05.parquet
f340c62a18885c56176433c9db28e97d0a8a14e6e49869276ad8afb20ebf088b  yellow_tripdata_2026-06.parquet
3260bb629c2f334055f8a566cbb02c15476b15120c45d7ad0aae46fc4bb06612  yellow_tripdata_2026-07.parquet
```

Each file has a version 1 `<filename>.metadata.json` sidecar containing the
complete Parquet and Spark schemas, provenance, and verification results.
Those sidecars and the data are ignored by Git. This catalog preserves the
accepted observations for collaborators. SHA-256 identifies downloaded bytes,
not an independently authenticated checksum supplied by TLC.

The observed schema groups below preserve source spelling. All Parquet fields
are optional, and Spark marks every field nullable. This does not establish
which fields actually contain null values.

| Source columns | Parquet physical / logical type | Spark type | Months |
|---|---|---|---|
| `VendorID`, `PULocationID`, `DOLocationID` | INT32 | integer | All |
| `tpep_pickup_datetime`, `tpep_dropoff_datetime` | INT64 / TIMESTAMP(MICROS, isAdjustedToUTC=false) | timestamp_ntz | All |
| `passenger_count`, `RatecodeID`, `payment_type` | INT64 | long | All |
| `store_and_fwd_flag` | BINARY / STRING | string | All |
| `trip_distance`, `fare_amount`, `extra`, `mta_tax`, `tip_amount`, `tolls_amount`, `improvement_surcharge`, `total_amount`, `congestion_surcharge`, `Airport_fee`, `cbd_congestion_fee` | DOUBLE | double | All |
| `request_source` | BINARY / STRING | string | June and July only |

May has no `request_source` column. June and July share the same schema and add
that column after the 20 common fields. The files use `Airport_fee`, whereas
the dictionary summary below uses `airport_fee`. No column was renamed or
added to harmonize these differences. Their treatment belongs to M3.1.

Verification used Spark **4.2.0** with `local[2]`, Scala **2.13.18**, JDK
**21.0.12**, and sbt **2.0.9** on macOS ARM64. Every column of every row was
decoded without cleaning or filtering. No RustFS objects were created.
All 16 automated Scala tests passed and passed again from a fresh sbt process.
A fresh application process reused and fully reread all three accepted files.
Independent hashing confirmed unchanged Parquet and sidecar bytes and
modification times after reuse. No partial files or pending markers remained.
The upgrade was checked on **2026-09-22** on macOS **26.5 ARM64** with the same
runtime versions. All **20 tests** passed twice in fresh sbt processes, including
the positional command adapter tests. Both `retrieve` and
`runMain nyctaxi.retrieval.LocalRetrieval` reused and fully decoded the same three
files with `local[2]`. Independent hashes and modification times remained
unchanged for all six Parquet and metadata files. Help, positional precedence,
nonzero error exits, and refusal of files without metadata were also checked.

See the [local retrieval guide](../exo1_data_retrieval/README.md) for execution,
reuse, explicit refresh, and recovery commands. Refreshes require reviewing
new provenance and explicitly updating this catalog.

## Reference Snapshot

Snapshot ID: **`20260919T204934Z`**, a UTC timestamp in `YYYYMMDDTHHMMSSZ` format.
Both original files returned HTTP 200 and finished downloading at
**2026-09-19 20:49:35 UTC**. Their bytes were preserved without conversion.

| Original filename and source URL | Bytes | Reference revision |
|---|---:|---|
| [data_dictionary_trip_records_yellow.pdf](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf) | 148,556 | March 18, 2025, printed on page 1 |
| [taxi_zone_lookup.csv](https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv) | 12,331 | No embedded revision, identified by retrieval time and checksum |

SHA-256 checksums, recomputed from the staged files:

```text
b9e9f0ecfa6bd9cc847283379173fd764fe1ab76d2fa143329f541cb1f479993  data_dictionary_trip_records_yellow.pdf
1a99e105092230f8620f301edcca7f80d3080642ff404d28ed957d3fa222c8ed  taxi_zone_lookup.csv
```

The directory is relative to the repository root and is ignored by Git:

```text
data/reference/tlc/20260919T204934Z/
```

M2.3 publishes the exact staged files to these future destinations:

```text
s3a://nyc-taxi/nyc_reference/tlc/20260919T204934Z/data_dictionary_trip_records_yellow.pdf
s3a://nyc-taxi/nyc_reference/tlc/20260919T204934Z/taxi_zone_lookup.csv
```

The catalog is versioned in Git. The PDF and CSV are not included in a clone,
and neither RustFS object has been uploaded or verified yet. Until publication,
retain the staged copies. A fresh download must match the recorded checksum
before it can represent this snapshot. If the source changes, create a new
timestamped snapshot and update the catalog explicitly. Do not overwrite an
existing snapshot with different bytes.

## Yellow-Taxi Dictionary

The one-page PDF opens and identifies itself as the yellow-taxi dictionary.
Use its printed revision above, rather than its PDF creation timestamp.
The following is a navigation summary of its field meanings. The original PDF
is the reference for M3.1, which defines the actual cleaning contract.

| Fields | Meaning |
|---|---|
| `VendorID` | Trip-record provider code |
| `tpep_pickup_datetime`, `tpep_dropoff_datetime` | Meter start and stop timestamps |
| `passenger_count`, `trip_distance` | Passenger count and metered distance in miles |
| `RatecodeID` | Rate category at trip completion |
| `store_and_fwd_flag` | Whether the vehicle buffered the trip before sending it |
| `PULocationID`, `DOLocationID` | Pickup and drop-off taxi-zone identifiers |
| `payment_type` | Payment category |
| `fare_amount` | Metered fare based on time and distance |
| `extra`, `mta_tax` | Other extras and surcharges, and the rate-triggered MTA tax |
| `tip_amount`, `tolls_amount` | Recorded card tips and toll charges. Cash tips are excluded |
| `improvement_surcharge` | Improvement charge applied at meter start |
| `total_amount` | Passenger charge total, excluding cash tips |
| `congestion_surcharge` | New York State congestion charge |
| `airport_fee` | Fee for pickups at LaGuardia or JFK |
| `cbd_congestion_fee` | Congestion Relief Zone charge introduced in January 2025 |

Categorical definitions are in the corresponding rows on page 1:

- `VendorID`: 1 = Creative Mobile Technologies, 2 = Curb Mobility,
  6 = Myle Technologies, 7 = Helix.
- `RatecodeID`: 1 = standard, 2 = JFK, 3 = Newark, 4 = Nassau or Westchester,
  5 = negotiated, 6 = group, 99 = null/unknown.
- `payment_type`: 0 = Flex Fare, 1 = credit card, 2 = cash, 3 = no charge,
  4 = dispute, 5 = unknown, 6 = voided.
- `store_and_fwd_flag`: `Y` = buffered before transmission, `N` = not buffered.

These definitions do not prove which values occur in the selected monthly files.
The choice of accepted values and handling of missing or invalid data belongs to M3.1.

## Taxi-Zone Lookup

Observed CSV columns: `LocationID`, `Borough`, `Zone`, `service_zone`.
The staged file parses into **265 rows**, with **265 unique, nonempty
`LocationID` values**, covering integer IDs 1 through 265.

`PULocationID` and `DOLocationID` each reference `LocationID`, using separate
pickup and drop-off joins. `Borough` and `Zone` describe the location, while
`service_zone` records its service-area category. Join and missing-key policies
remain part of M3.1 and M4.1.

Preserve these special rows exactly as supplied:

| LocationID | Borough | Zone | service_zone |
|---|---|---|---|
| 1 | EWR | Newark Airport | EWR |
| 264 | Unknown | N/A | N/A |
| 265 | N/A | Outside of NYC | N/A |

`N/A` above is literal source text. It has not been converted to a null value.
No geographic boundary files are selected in M2.1.

## Reference and Source Identification Verification — M2.1

- Checked the official page for the five source links and monthly file selection.
- Verified HTTP 200 and reported lengths for all three monthly URLs using HEAD.
- Opened and rendered the original PDF with Poppler, confirming its title and revision.
- Parsed the CSV with Python's standard CSV reader and checked row shape and ID uniqueness.
- Recomputed both SHA-256 checksums and file sizes from the staged bytes.

These M2.1 checks ran on macOS ARM64 on 2026-09-19. NYC rejected the default curl
user-agent for the dictionary download with HTTP 403. Retrying with
`--user-agent 'Mozilla/5.0'` succeeded. M2.1 tested no monthly content, Spark
processing, RustFS access, or warehouse loading. Subsequent M2.2 monthly
download verification is recorded above.

External TLC materials retain their own terms, as described in the root
[license notes](../README.md#license).
