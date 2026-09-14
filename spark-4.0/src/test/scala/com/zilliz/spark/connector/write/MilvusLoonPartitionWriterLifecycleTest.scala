package com.zilliz.spark.connector.write

import java.nio.file.{Files, Path}
import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.jni.storage.StorageNative
import com.zilliz.spark.connector.options.MilvusOption

/** The writer's resource lifecycle against the real native writer.
  *
  * Spark's task path is `commit()` then `close()`, and the writer cleans up in
  * commit's `finally`, so cleanup runs twice on every successful write.
  * `loon_writer_destroy` is an unguarded `delete`, so a second call frees the
  * same C++ object again and takes the executor down with it. A JVM-level
  * assertion cannot observe a double free — the process dies — so the check is
  * that the sequence completes at all.
  */
class MilvusLoonPartitionWriterLifecycleTest
    extends AnyFunSuite
    with Matchers {

  private def skipWithoutLibrary(): Unit =
    try
      StorageNative.filesystemDestroy(
        StorageNative.filesystemGet(
          Map("fs.storage_type" -> "local").asJava,
          ""
        )
      )
    catch {
      case _: UnsatisfiedLinkError | _: NoClassDefFoundError =>
        cancel("libnative-storage-jni is not on this machine")
      case _: RuntimeException =>
        cancel("libnative-storage-jni is not on this machine")
    }

  private val schema = StructType(
    Seq(
      StructField("id", LongType, nullable = false),
      StructField("Timestamp", LongType, nullable = false)
    )
  )

  private def writerFor(dir: Path): MilvusLoonPartitionWriter = {
    val options = new java.util.HashMap[String, String]()
    options.put("fs.storage_type", "local")
    options.put("fs.root_path", dir.toAbsolutePath.toString)
    options.put(MilvusOption.WriterCustomPath, "segment-0")
    options.put(MilvusOption.WriterFieldIds, "id:100,Timestamp:1")
    new MilvusLoonPartitionWriter(
      partitionId = 0,
      taskId = 0L,
      sparkSchema = schema,
      milvusOption = MilvusOption(new CaseInsensitiveStringMap(options))
    )
  }

  private def withTempDir(body: Path => Unit): Unit = {
    val dir = Files.createTempDirectory("loon-writer-lifecycle")
    try body(dir)
    finally
      Files
        .walk(dir)
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(Files.deleteIfExists(_))
  }

  test("commit then close does not release the native writer twice") {
    skipWithoutLibrary()
    withTempDir { dir =>
      val writer = writerFor(dir)
      writer.write(InternalRow(1L, 100L))
      writer.write(InternalRow(2L, 101L))
      val message = writer.commit()
      message.asInstanceOf[MilvusLoonCommitMessage].recordCount shouldBe 2L
      // Spark calls close after commit. Before cleanup was made idempotent this
      // is where the process died.
      writer.close()
      // And a caller that closes twice is no different.
      writer.close()
    }
  }

  test("abort then close does not release the native writer twice") {
    skipWithoutLibrary()
    withTempDir { dir =>
      val writer = writerFor(dir)
      writer.write(InternalRow(1L, 100L))
      writer.abort()
      writer.close()
    }
  }

  test("close without a commit releases the writer once") {
    skipWithoutLibrary()
    withTempDir { dir =>
      val writer = writerFor(dir)
      writer.write(InternalRow(1L, 100L))
      writer.close()
      writer.close()
    }
  }
}
