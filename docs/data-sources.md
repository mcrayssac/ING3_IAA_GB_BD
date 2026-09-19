# TLC Data Sources

M2.1 identifies the inputs for yellow taxi trips in **May, June, and July 2026**.
The [official TLC source page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
lists all three monthly files, the yellow-taxi dictionary, and the zone lookup.
Source checks passed on **2026-09-19**. The reference snapshot is staged locally.
**RustFS publication is pending M2.3, after infrastructure verification in M1.3.**

## Monthly Trip Files

The following direct URLs returned HTTP 200 to HTTPS HEAD requests on
2026-09-19 at approximately 20:47 UTC. Sizes are the reported `Content-Length`
values in bytes.

| Month | Official file and download URL | Reported bytes | HTTP status |
|---|---|---:|---|
| 2026-05 | [yellow_tripdata_2026-05.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-05.parquet) | 69,699,174 | 200 |
| 2026-06 | [yellow_tripdata_2026-06.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-06.parquet) | 65,465,637 | 200 |
| 2026-07 | [yellow_tripdata_2026-07.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-07.parquet) | 61,685,033 | 200 |

No monthly file bodies were downloaded. Availability and reported size do not
verify Parquet integrity, row counts, or physical column types. M2.2 downloads
the complete files. These observations are dated because TLC can replace files
at the same URLs. Local staging and raw object names follow
[architecture interfaces I2 and I3](architecture.md#interfaces).

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

## Verification Record

- Checked the official page for the five source links and monthly file selection.
- Verified HTTP 200 and reported lengths for all three monthly URLs using HEAD.
- Opened and rendered the original PDF with Poppler, confirming its title and revision.
- Parsed the CSV with Python's standard CSV reader and checked row shape and ID uniqueness.
- Recomputed both SHA-256 checksums and file sizes from the staged bytes.

Checks ran on macOS ARM64 on 2026-09-19. NYC rejected the default curl
user-agent for the dictionary download with HTTP 403. Retrying with
`--user-agent 'Mozilla/5.0'` succeeded. No monthly content, Spark processing,
RustFS access, or warehouse loading was tested.

External TLC materials retain their own terms, as described in the root
[license notes](../README.md#license).
