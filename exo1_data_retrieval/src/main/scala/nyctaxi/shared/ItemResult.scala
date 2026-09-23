package nyctaxi.shared

/** Outcome of one month or source in a run. A failed item carries a readable, redacted message. */
final case class ItemResult(name: String, success: Boolean, message: String)
