package nyctaxi.retrieval

import java.nio.file.{Files, Path, StandardCopyOption, StandardOpenOption}
import java.time.Instant
import java.util.UUID
import nyctaxi.shared.{Provenance, ProvenanceCodec, ProvenanceFiles}

/** Promotes verified candidates and detects a crash between the two atomic file replacements. */
final class ProvenanceStore {
    /** Candidates are already verified. A marker makes any incomplete promotion fail closed. */
    def publish(candidate: Path, file: Path, saved: Provenance): Unit = {
        val sidecar = ProvenanceFiles.metadata(file)
        val pending = ProvenanceFiles.pending(file)
        val stagedMetadata = Files.createTempFile(file.getParent, file.getFileName.toString, ".metadata.part")
        try {
            Files.write(stagedMetadata, ProvenanceCodec.toBytes(saved))
            if (Files.exists(sidecar)) {
                val history = Files.createDirectories(
                    file.getParent.resolve(".history").resolve(file.getFileName.toString)
                )
                // Portable names avoid ':' on Windows and keep even malformed prior metadata for inspection.
                Files.copy(
                    sidecar,
                    history.resolve(s"${Instant.now().toEpochMilli}-${UUID.randomUUID()}.json")
                )
            }
            Files.writeString(
                pending,
                saved.sha256 + "\n",
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
            )
            Files.move(candidate, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
            Files.move(
                stagedMetadata,
                sidecar,
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING
            )
            Files.delete(pending)
        } finally Files.deleteIfExists(stagedMetadata)
    }
}
