import java.io.{IOException, InputStream}
import java.util.Arrays

import sbtassembly.Assembly.{Dependency, JarEntry, Project}

/** Run with -Xmx64m against the compiled sbt build definition. */
object NativeMergeProbe {
  private val target = "native/milvus/1/linux-x86_64/libfixture.so"

  private final class Fixture(
      length: Long,
      chunk: Int = 65536,
      changedByte: Long = -1L,
      failAt: Long = -1L
  ) {
    var opened = 0
    var closed = 0
    var bytesRead = 0L

    def dependency(name: String): Dependency =
      Project(name, target, target, () => {
        opened += 1
        new InputStream {
          private var position = 0L

          override def read(): Int = {
            val one = new Array[Byte](1)
            if (read(one, 0, 1) < 0) -1 else one(0) & 0xff
          }

          override def read(buffer: Array[Byte], offset: Int, requested: Int): Int = {
            if (requested == 0) return 0
            if (failAt >= 0 && position >= failAt)
              throw new IOException("fixture read failure")
            if (position == length) return -1
            val count = math.min(length - position, math.min(chunk, requested).toLong).toInt
            Arrays.fill(buffer, offset, offset + count, 0.toByte)
            if (changedByte >= position && changedByte < position + count)
              buffer(offset + (changedByte - position).toInt) = 1
            position += count
            bytesRead += count
            count
          }

          override def close(): Unit = closed += 1
        }
      })
  }

  private def merged(entries: Dependency*): JarEntry =
    NativeBundle.nativeMergeStrategy(entries.toVector) match {
      case Right(Vector(entry)) => entry
      case result => throw new AssertionError("Unexpected merge result: " + result)
    }

  def main(arguments: Array[String]): Unit = {
    assert(Runtime.getRuntime.maxMemory() <= 80L * 1024 * 1024,
      "Run the regression probe with -Xmx64m")

    val single = new Fixture(2L * 1024 * 1024 * 1024)
    val singleDependency = single.dependency("single")
    val singleEntry = merged(singleDependency)
    assert(single.opened == 0 && single.bytesRead == 0)
    assert(singleEntry.stream eq singleDependency.stream)
    val emitted = singleEntry.stream()
    try assert(emitted.read() == 0)
    finally emitted.close()
    assert(single.opened == 1 && single.closed == 1)

    // Larger than the real storage library and eight times the whole JVM heap.
    val largeSize = 512L * 1024 * 1024 + 17
    val first = new Fixture(largeSize)
    val second = new Fixture(largeSize, chunk = 8191)
    val firstDependency = first.dependency("first")
    val identical = merged(firstDependency, second.dependency("identical"))
    assert(identical.stream eq firstDependency.stream)
    assert(first.bytesRead == largeSize && second.bytesRead == largeSize)
    assert(first.opened == 1 && first.closed == 1 && second.opened == 1 && second.closed == 1)

    for (different <- Seq(new Fixture(100001, changedByte = 100000), new Fixture(100002))) {
      val original = new Fixture(100001)
      val result = NativeBundle.nativeMergeStrategy(
        Vector(original.dependency("original"), different.dependency("different")))
      assert(result.isLeft)
      assert(result.left.get.contains("original") && result.left.get.contains("different"))
      assert(original.closed == 1 && different.closed == 1)
    }

    val readable = new Fixture(100001)
    val failing = new Fixture(100001, failAt = 65536)
    try {
      merged(readable.dependency("readable"), failing.dependency("failing"))
      throw new AssertionError("Read failure was swallowed")
    } catch {
      case failure: IOException => assert(failure.getMessage == "fixture read failure")
    }
    assert(readable.closed == 1 && failing.closed == 1)
    println("PASS: native merge preserves streams, compares duplicates, rejects differences, closes failures, and uses bounded memory")
  }
}
