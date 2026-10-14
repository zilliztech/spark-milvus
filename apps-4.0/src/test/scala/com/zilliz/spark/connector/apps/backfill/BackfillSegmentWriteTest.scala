package com.zilliz.spark.connector.apps.backfill

import java.nio.file.{Files, Path}
import java.util.Comparator
import scala.collection.mutable.ArrayBuffer

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
      val writer =
        BackfillSegmentWrite.segmentWriter(schema, options, "run-job")
      try {
        writer.write(InternalRow(1L, 100L))
        writer.write(InternalRow(2L, 101L))
        writer.commit()
      } finally writer.close()

      Files.exists(dir.resolve("segment-0")) shouldBe true
      Files.exists(dir.resolve("staging")) shouldBe false
    }
  }

  test("segment rows strip tracking columns and preserve provenance counts") {
    val target = StructType(
      Seq(
        StructField("a", LongType, nullable = true),
        StructField("b", LongType, nullable = true)
      )
    )
    val rows = Iterator(
      InternalRow(10L, 0L, 1L, 2L, true, true, false, false, true),
      InternalRow(10L, 1L, null, 3L, null, false, false, true, false),
      InternalRow(10L, 2L, 4L, null, true, true, false, null, false)
    )
    val written = ArrayBuffer.empty[InternalRow]
    val counts =
      BackfillSegmentWrite.writeRows(rows, target, readsSnapshotFields = true) {
        row =>
          written += row.copy()
      }
    written.map(_.numFields).toSeq shouldBe Seq(2, 2, 2)
    written
      .map(row => (row.get(0, LongType), row.get(1, LongType)))
      .toSeq shouldBe
      Seq((1L, 2L), (null, 3L), (4L, null))
    counts shouldBe BackfillSegmentWrite.RowCounts(
      3L,
      2L,
      Map("a" -> 2L, "b" -> 1L),
      Map("a" -> 0L, "b" -> 1L)
    )
  }

  test("replace rows require no provenance columns and preserve null values") {
    val target =
      StructType(Seq(StructField("value", LongType, nullable = true)))
    val written = ArrayBuffer.empty[Any]
    val counts = BackfillSegmentWrite.writeRows(
      Iterator(
        InternalRow(10L, 0L, null, null),
        InternalRow(10L, 1L, 7L, true)
      ),
      target,
      readsSnapshotFields = false
    ) { row => written += row.get(0, LongType) }
    written.toSeq shouldBe Seq(null, 7L)
    counts shouldBe BackfillSegmentWrite.RowCounts(2L, 1L, Map.empty, Map.empty)
  }

  test("a row write failure propagates without consuming later rows") {
    val target =
      StructType(Seq(StructField("value", LongType, nullable = false)))
    var consumed = 0
    val rows = Iterator(1L, 2L, 3L).map { value =>
      consumed += 1
      InternalRow(10L, value, value, true)
    }
    val failure = new IllegalStateException("writer failed")
    val error = intercept[IllegalStateException] {
      BackfillSegmentWrite.writeRows(
        rows,
        target,
        readsSnapshotFields = false
      ) { row =>
        if (row.getLong(0) == 2L) throw failure
      }
    }
    error should be theSameInstanceAs failure
    consumed shouldBe 2
  }

}
