package com.zilliz.milvus.storage.write.io

import java.nio.file.{Files, Path}
import java.util.Collections
import java.util.Comparator
import scala.jdk.CollectionConverters._

import org.apache.arrow.c.{ArrowArray, ArrowSchema, Data}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{BigIntVector, VarCharVector, VectorSchemaRoot}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.jni.storage.NativeStorageLibrary
import com.zilliz.milvus.storage.read.exec.SegmentReader
import com.zilliz.milvus.storage.read.plan.SegmentReadTask
import com.zilliz.milvus.storage.snapshot.SegmentLayout
import com.zilliz.milvus.storage.snapshot.V2ColumnGroup
import io.milvus.storage.{
  MilvusStorageColumnGroups,
  MilvusStorageProperties,
  MilvusStorageReader,
  MilvusStorageWriter
}

/** Writes a segment through milvus-storage's JNI and reads it back through
  * milvus-storage's JNI.
  *
  * This is the strongest check the write path has without a live Milvus: it
  * exercises both directions of the Arrow boundary and the column groups the
  * writer hands back, and the values have to survive the round trip.
  *
  * It is also the regression test for milvus-storage#493, which is why the row
  * count is what it is. `reader.record_batch_max_rows` defaults to 8192, so the
  * packed reader only starts handing back sliced batches
  * (`rb->Slice(min_rows)`) once more than 16384 rows are read. Below that the
  * offset materialization in milvus_storage_reader.cpp never runs, and a test
  * that stayed under the boundary would pass just as happily against the broken
  * ArrowArrayStream path.
  *
  * The check itself is that values stay strictly in step with the row offset.
  * If a sliced batch were imported without honouring `ArrowArray.offset`, every
  * batch after the first would restart from row 0 and the comparison fails.
  *
  * Needs libmilvus-storage-jni, so it skips where the library is absent, which
  * is the state of CI.
  */
class WriterRoundTripTest extends AnyFunSuite with Matchers {

  /** Over 2 * 8192, so the reader slices. See the class comment. */
  private val rows = 20480

  private def skipWithoutLibrary(): Unit =
    try
      NativeStorageLibrary.load()
    catch {
      case _: UnsatisfiedLinkError | _: NoClassDefFoundError =>
        cancel("libmilvus-storage-jni is not on this machine")
      case _: RuntimeException =>
        cancel("libmilvus-storage-jni is not on this machine")
    }

  private val schema = new Schema(
    Seq(
      new Field(
        "id",
        new FieldType(false, new ArrowType.Int(64, true), null),
        Collections.emptyList[Field]()
      ),
      new Field(
        "name",
        new FieldType(false, new ArrowType.Utf8(), null),
        Collections.emptyList[Field]()
      )
    ).asJava
  )

  private def fill(root: VectorSchemaRoot): Unit = {
    val id = root.getVector("id").asInstanceOf[BigIntVector]
    val name = root.getVector("name").asInstanceOf[VarCharVector]
    id.allocateNew(rows)
    name.allocateNew(rows)
    var i = 0
    while (i < rows) {
      id.setSafe(i, i.toLong * 7)
      name.setSafe(i, s"row-$i".getBytes("UTF-8"))
      i += 1
    }
    root.setRowCount(rows)
  }

  test(
    "a segment written through milvus-storage's JNI reads back with the same values"
  ) {
    skipWithoutLibrary()

    val dir: Path = Files.createTempDirectory("native-writer-roundtrip")
    val allocator = new RootAllocator(Long.MaxValue)
    // The local backend roots at fs.root_path and appends the key, so the
    // temp directory is the root and every path below is relative to it.
    val properties = Map(
      "fs.storage_type" -> "local",
      "fs.root_path" -> dir.toAbsolutePath.toString
    ).asJava

    // One exported ArrowSchema per consumer: loon_writer_new and loon_reader_new
    // each take ownership of the struct they are given, so handing the same one
    // to both fails with "Cannot import released ArrowSchema".
    var writeSchema: ArrowSchema = null
    var writer: MilvusStorageWriter = null
    var nativeProperties: MilvusStorageProperties = null
    var written = 0L
    var segmentReader: SegmentReader = null

    try {
      writeSchema = ArrowSchema.allocateNew(allocator)
      Data.exportSchema(allocator, schema, null, writeSchema)

      // --- write ---
      nativeProperties = new MilvusStorageProperties()
      nativeProperties.create(properties)
      writer = new MilvusStorageWriter()
      writer.create(
        "segment",
        writeSchema.memoryAddress(),
        nativeProperties
      )
      writer.isValid shouldBe true

      val source = VectorSchemaRoot.create(schema, allocator)
      try {
        fill(source)
        val array = ArrowArray.allocateNew(allocator)
        try {
          Data.exportVectorSchemaRoot(allocator, source, null, array)
          writer.write(array.memoryAddress())
          writer.flush()
        } finally array.close()
      } finally source.close()

      written = writer.close()
      written should not be 0L

      val groupCount = MilvusStorageColumnGroups.count(written)
      groupCount should be > 0
      val files = (0 until groupCount).map(
        MilvusStorageColumnGroups.files(written, _)
      )
      val counts = (0 until groupCount).map(
        MilvusStorageColumnGroups.fileRowCounts(written, _)
      )
      val columns = (0 until groupCount).map(
        MilvusStorageColumnGroups.columns(written, _)
      )
      info(
        s"wrote $groupCount column group(s): " +
          files
            .zip(counts)
            .map { case (f, c) => s"${f.toSeq} rows=${c.toSeq}" }
            .mkString("; ")
      )
      counts.map(_.sum).sum shouldBe rows.toLong

      // --- read back, through the registry the Spark readers use ---
      val task = SegmentReadTask(
        segmentId = 1L,
        partitionId = 1L,
        layout = SegmentLayout.ColumnGroups(
          Seq(
            V2ColumnGroup(
              fieldIds = Seq(0L, 1L),
              filePaths = files.head.toSeq,
              fileRowCounts = counts.head.toSeq
            )
          )
        ),
        schemaBytes = Array.emptyByteArray,
        properties = Map(
          "fs.storage_type" -> "local",
          "fs.root_path" -> dir.toAbsolutePath.toString
        )
      )
      // Field ids 0 and 1 stand for the two columns, in schema order.
      val nameFor = Map(0L -> "id", 1L -> "name")
      segmentReader = SegmentReader.open(
        task,
        schema,
        Seq("id", "name"),
        nameFor.get,
        allocator
      )

      var seen = 0
      var batches = 0
      var batch = segmentReader.next()
      while (batch.isDefined) {
        val root = batch.get
        try {
          batches += 1
          val id = root.getVector("id").asInstanceOf[BigIntVector]
          val name = root.getVector("name").asInstanceOf[VarCharVector]
          var i = 0
          while (i < root.getRowCount) {
            id.get(i) shouldBe seen.toLong * 7
            new String(name.get(i), "UTF-8") shouldBe s"row-$seen"
            seen += 1
            i += 1
          }
        } finally root.close()
        batch = segmentReader.next()
      }

      segmentReader.deliveredRows shouldBe rows.toLong
      seen shouldBe rows
      // Three batches at the 8192 default: 8192, 8192, 4096. More than one is
      // what makes this a slice regression rather than a single-batch read.
      batches should be >= 3

      // The crossing's own account of the same read (G5). Calls so far: open
      // properties allocation and creation, the column groups, the reader and
      // the batch reader, then one ReadNext per batch and the EOF call.
      val metrics = segmentReader.metrics
      metrics.batches shouldBe batches.toLong
      metrics.jniCalls shouldBe 5L + batches + 1L
      metrics.jniNanos should be > 0L
      metrics.arrowBytes should be > (rows.toLong * 8)
      metrics.allocatedMax should be > 0L
      // Over 16384 rows the packed reader slices, so the C side copied.
      metrics.copies should be > 0L
      metrics.copiedBytes should be > 0L
      segmentReader.close()
      // Closing adds three destroys and frees the properties. Native counters
      // remain available after close.
      val closed = segmentReader.metrics
      closed.jniCalls shouldBe metrics.jniCalls + 4L
      closed.copies shouldBe metrics.copies
    } finally {
      if (segmentReader != null) segmentReader.close()
      if (written != 0L) MilvusStorageColumnGroups.destroy(written)
      if (writer != null) writer.destroy()
      if (nativeProperties != null) nativeProperties.free()
      if (writeSchema != null) writeSchema.close()
      allocator.close()
      Files
        .walk(dir)
        .sorted(Comparator.reverseOrder())
        .forEach(Files.deleteIfExists(_))
    }
  }

}
