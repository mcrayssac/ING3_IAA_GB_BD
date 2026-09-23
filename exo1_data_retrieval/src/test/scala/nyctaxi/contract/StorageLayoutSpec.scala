package nyctaxi.contract

import java.nio.file.Paths
import org.scalatest.funsuite.AnyFunSuite

/** Keys and names are published contracts, so every layout is pinned here. */
class StorageLayoutSpec extends AnyFunSuite {
  private val may = Month("2026-05")

  test("trip, reference, metadata, and receipt keys follow interfaces I1, I3, and I11") {
    assert(StorageLayout.tripFilename(may) == "yellow_tripdata_2026-05.parquet")
    assert(StorageLayout.tripSource(may).toString ==
      "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-05.parquet")
    assert(StorageLayout.rawTrip(may).value == "nyc_raw/yellow_tripdata_2026-05.parquet")
    assert(StorageLayout.tripProvenance(may, "ab").value ==
      "nyc_metadata/tlc/trips/ab/yellow_tripdata_2026-05.parquet.metadata.json")
    assert(StorageLayout.reference("S", "zones.csv").value == "nyc_reference/tlc/S/zones.csv")
    assert(StorageLayout.referenceDescriptor("S").value == "nyc_metadata/tlc/references/S.json")
    assert(StorageLayout.receipt("R").value == "nyc_metadata/tlc/publications/R.json")
    assert(StorageLayout.localReference(Paths.get("/repo"), "S", "zones.csv") ==
      Paths.get("/repo/data/reference/tlc/S/zones.csv"))
    assert(StorageLayout.uri("nyc-taxi", StorageKey("nyc_raw/a")) == "s3a://nyc-taxi/nyc_raw/a")
  }

  test("invalid months and keys are rejected at construction") {
    for (text <- Vector("2026-5", "2026-13", "26-05", "")) intercept[IllegalArgumentException](Month(text))
    for (text <- Vector("", "/nyc_raw/a", "s3a://nyc-taxi/a")) intercept[IllegalArgumentException](StorageKey(text))
  }
}
