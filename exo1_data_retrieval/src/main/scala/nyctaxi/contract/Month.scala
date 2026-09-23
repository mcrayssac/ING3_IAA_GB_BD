package nyctaxi.contract

/** Trip month of the TLC selection (interface I1, task M2.1).
  *
  * Input: `YYYY-MM` text already checked against the selection by configuration parsing.
  * Output: a typed month used for filenames, source URLs, and storage keys.
  * Failure: construction throws `IllegalArgumentException` for text that is not `YYYY-MM`.
  */
final case class Month(value: String) {
  require(value.matches("[0-9]{4}-(0[1-9]|1[0-2])"), s"Month must use YYYY-MM: $value")
  override def toString: String = value
}

object Month {
  /** The three months selected in M2.1, in processing order. */
  val selected: Vector[Month] = Vector("2026-05", "2026-06", "2026-07").map(Month(_))
}
