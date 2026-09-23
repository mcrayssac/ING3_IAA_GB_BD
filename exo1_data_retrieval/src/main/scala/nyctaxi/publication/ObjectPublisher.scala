package nyctaxi.publication

import java.io.IOException
import nyctaxi.contract.StorageKey
import nyctaxi.shared.Digest
import scala.util.Using

/** Idempotent object creation with independent remote verification (interfaces I3 and I11, task M2.3).
  *
  * Input: an object key and a reopenable local payload.
  * Output: "uploaded" or "reused", only after an independent read of the remote bytes matches the payload.
  * Failure: `PublicationConflict` when different bytes already exist. Accepted objects are never overwritten.
  */
final class ObjectPublisher(store: ObjectStore, operations: StorageOperations) {
    /** Publishes one payload. Each attempt checks the destination first, covering a lost commit response. */
    def publish(key: StorageKey, payload: Payload): String = operations.run { scope =>
        if (store.stat(key).nonEmpty) { verifyRemoteBytes(key, payload, scope); "reused" }
        else { uploadNew(key, payload, scope); verifyRemoteBytes(key, payload, scope); "uploaded" }
    }

    /** Streams the payload into a conditional create, aborting instead of committing on any failure. */
    private def uploadNew(key: StorageKey, payload: Payload, scope: OperationScope): Unit = {
        val pending = store.create(key)
        var committed = false
        scope.onCancel(() => pending.abort())
        try {
            Using.resource(payload.open()) { input =>
                scope.onCancel(() => input.close())
                val observed = Digest.stream(input, () => scope.check(), Some(pending.output))
                require(observed == ((payload.bytes, payload.sha256)), s"Source changed during upload: $key")
            }
            scope.check()
            pending.commit()
            committed = true
        } finally if (!committed) pending.abort()
    }

    /** Accepts a remote object only when its size and a full SHA-256 read match the payload. */
    private def verifyRemoteBytes(key: StorageKey, payload: Payload, scope: OperationScope): Unit = {
        val info = store.stat(key).getOrElse(throw new IOException(s"Missing remote object: $key"))
        if (info.bytes != payload.bytes) throw new PublicationConflict(s"Remote size conflict: $key")
        // LIMIT: acceptance re-reads every remote byte. Each upload costs twice the object size in transfer.
        Using.resource(store.open(key)) { input =>
            scope.onCancel(() => input.close())
            val observed = Digest.stream(input, () => scope.check())
            if (observed != ((payload.bytes, payload.sha256)))
                throw new PublicationConflict(s"Remote SHA-256 conflict: $key")
        }
    }
}
