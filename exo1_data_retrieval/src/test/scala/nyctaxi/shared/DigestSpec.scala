package nyctaxi.shared

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import org.scalatest.funsuite.AnyFunSuite

/** One SHA-256 implementation must agree across memory, files, and copied streams. */
class DigestSpec extends AnyFunSuite {
    private val abc = "abc".getBytes(StandardCharsets.UTF_8)
    private val abcSha256 = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"

    test("memory, file, and stream hashing agree and copying preserves bytes") {
        assert(Digest.sha256(abc) == abcSha256)
        Fixtures.directory { root =>
            assert(Digest.sha256(Files.write(root.resolve("abc"), abc)) == abcSha256)
        }
        val copy = new ByteArrayOutputStream()
        assert(Digest.stream(new ByteArrayInputStream(abc), () => (), Some(copy)) == ((3L, abcSha256)))
        assert(copy.toByteArray.sameElements(abc))
    }

    test("the check callback can stop hashing") {
        intercept[InterruptedException](Digest.stream(
            new ByteArrayInputStream(abc),
            () => throw new InterruptedException()
        ))
    }
}
