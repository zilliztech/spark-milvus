package com.zilliz.spark.connector.apps.backfill

import java.nio.file.{Files, Path}
import java.util.Comparator

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.jni.storage.NativeStorageLibrary
import com.zilliz.spark.connector.options.MilvusOption
import com.zilliz.spark.connector.write.WriteSchema
import io.milvus.grpc.schema.{CollectionSchema, DataType, FieldSchema}

/** A backfill writes each segment's column groups into the segment's own
  * directory and records the run once in its job manifest. Each segment used to
  * open a job of its own as well, which left owner.json, the job manifest and
  * the marker under a random staging prefix per segment and task attempt, and
  * `cleanup_staging` never removes a backfill job.
  */
class BackfillSegmentWriteTest extends AnyFunSuite with Matchers {

  private def skipWithoutLibrary(): Unit =
    try NativeStorageLibrary.load()
    catch {
      case _: UnsatisfiedLinkError | _: NoClassDefFoundError =>
        cancel("libmilvus-storage-jni is not on this machine")
      case _: RuntimeException =>
        cancel("libmilvus-storage-jni is not on this machine")
    }

  private val schema = WriteSchema.resolve(
    StructType(
      Seq(
        StructField("id", LongType, nullable = false),
        StructField("Timestamp", LongType, nullable = false)
      )
    ),
    CollectionSchema(
      name = "backfill",
      fields = Seq(
        FieldSchema(fieldID = 100, name = "id", dataType = DataType.Int64),
        FieldSchema(fieldID = 1, name = "Timestamp", dataType = DataType.Int64)
      )
    ),
    WriteSchema.Mode.Columns
  )

  private def withTempDir(body: Path => Unit): Unit = {
    val dir = Files.createTempDirectory("backfill-segment-write")
    try body(dir)
    finally
      Files
        .walk(dir)
        .sorted(Comparator.reverseOrder())
        .forEach(Files.deleteIfExists(_))
  }

  test("a segment write leaves nothing under the staging prefix") {
    skipWithoutLibrary()
    withTempDir { dir =>
      val options = Map(
        "fs.storage_type" -> "local",
        "fs.root_path" -> dir.toAbsolutePath.toString,
        MilvusOption.WriterCustomPath -> "segment-0",
        MilvusOption.WriterCommitType -> "addfield"
      )
      val writer = MilvusBackfill.segmentWriter(schema, options, "run-job")
      try {
        writer.write(InternalRow(1L, 100L))
        writer.write(InternalRow(2L, 101L))
        writer.commit()
      } finally writer.close()

      Files.exists(dir.resolve("segment-0")) shouldBe true
      Files.exists(dir.resolve("staging")) shouldBe false
    }
  }
}
