package com.zilliz.milvus.storage.io

import java.nio.file.{Files, Path}
import java.util.Collections
import java.util.Comparator
import scala.jdk.CollectionConverters._

import org.apache.arrow.c.{ArrowArray, ArrowSchema, Data}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{BigIntVector, VarCharVector}
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
  MilvusStorageReader
}

/** Reads frozen parquet through JNI, including sliced Arrow batches and
  * multiple-file column groups. Samples were generated once before the writer
  * migration; these regressions must work without any production writer.
  */
class NativeSegmentReadTest extends AnyFunSuite with Matchers {

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

  test("a frozen segment preserves values across sliced native Arrow batches") {
    skipWithoutLibrary()
    val dir = Files.createTempDirectory("native-fixed-segment")
    ReadFixtures.copyTo("segments/batch-20480", dir)
    val allocator = new RootAllocator(Long.MaxValue)
    var segmentReader: SegmentReader = null
    try {
      val task = SegmentReadTask(
        segmentId = 1L,
        partitionId = 1L,
        layout = SegmentLayout.ColumnGroups(
          Seq(
            V2ColumnGroup(
              fieldIds = Seq(0L, 1L),
              filePaths = Seq("segment.parquet"),
              fileRowCounts = Seq(rows.toLong)
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
      allocator.close()
      val paths = Files.walk(dir)
      try
        paths.sorted(Comparator.reverseOrder()).forEach(Files.deleteIfExists(_))
      finally paths.close()
    }
  }

  test("a column group whose file paths are short reads them all correctly") {
    skipWithoutLibrary()

    // Regression for the pointer lifetime in columnGroupsCreate: it used to
    // take `c_str()` on each path right after pushing it onto a vector that was
    // still growing. A path short enough for the small-string optimisation
    // keeps its characters inside the string object, so growing the vector
    // moves them and every pointer taken earlier dangles.
    //
    // This remains a reader regression even when no writer is available:
    // load the same fixed file bytes under names below the small-string limit.
    val dir: Path = Files.createTempDirectory("native-short-paths")
    val allocator = new RootAllocator(Long.MaxValue)
    val properties = Map(
      "fs.storage_type" -> "local",
      "fs.root_path" -> dir.toAbsolutePath.toString
    ).asJava

    val perFile = 1000
    val shortNames = Seq("a.pq", "b.pq", "c.pq")
    var readSchema: ArrowSchema = null
    var columnGroups = 0L
    var reader: MilvusStorageReader = null
    var nativeProperties: MilvusStorageProperties = null
    var batchReader = 0L

    try {
      ReadFixtures.copyTo("segments/short-paths", dir)
      val parts = (0 until 3).map(i => (s"part-$i.parquet", perFile.toLong))
      // Move each file to a name inside the small-string range. The path the C
      // layer is handed is relative to fs.root_path.
      val shortParts = parts.zip(shortNames).map { case ((path, count), name) =>
        Files.move(dir.resolve(path), dir.resolve(name))
        name.length should be < 16
        (name, count)
      }

      columnGroups = MilvusStorageColumnGroups.createFromGroups(
        Array(Array("id", "name")),
        Array(shortParts.map(_._1).toArray),
        Array(shortParts.map(_._2).toArray)
      )
      columnGroups should not be 0L

      readSchema = ArrowSchema.allocateNew(allocator)
      Data.exportSchema(allocator, schema, null, readSchema)
      nativeProperties = new MilvusStorageProperties()
      nativeProperties.create(properties)
      reader = new MilvusStorageReader()
      reader.create(
        columnGroups,
        readSchema.memoryAddress(),
        Array("id", "name"),
        nativeProperties
      )
      reader.isValid shouldBe true
      batchReader = reader.openRecordBatchReaderScala()

      var seen = 0
      var more = true
      while (more) {
        val array = ArrowArray.allocateNew(allocator)
        val batchSchema = ArrowSchema.allocateNew(allocator)
        try {
          more = reader.readNextBatchScala(
            batchReader,
            array.memoryAddress(),
            batchSchema.memoryAddress()
          )
          if (more) {
            val root =
              Data.importVectorSchemaRoot(allocator, array, batchSchema, null)
            try {
              val id = root.getVector("id").asInstanceOf[BigIntVector]
              var i = 0
              while (i < root.getRowCount) {
                id.get(i) shouldBe seen.toLong * 7
                seen += 1
                i += 1
              }
            } finally root.close()
          }
        } finally {
          array.close()
          batchSchema.close()
        }
      }

      seen shouldBe (3 * perFile)
    } finally {
      if (batchReader != 0L) reader.destroyRecordBatchReaderScala(batchReader)
      if (reader != null) reader.destroy()
      if (nativeProperties != null) nativeProperties.free()
      if (columnGroups != 0L) MilvusStorageColumnGroups.destroy(columnGroups)
      if (readSchema != null) readSchema.close()
      allocator.close()
      Files
        .walk(dir)
        .sorted(Comparator.reverseOrder())
        .forEach(Files.deleteIfExists(_))
    }
  }

  test("a column group holding several files reads every row of every file") {
    skipWithoutLibrary()

    // Regression for milvus-storage#657: a column group made of more than one
    // file must report the sum of its files' rows, not the first file's. It is
    // also the only check on the start/end accumulation columnGroupsCreate does
    // when it lays the files of one group end to end.
    val dir: Path = Files.createTempDirectory("native-multi-file")
    val allocator = new RootAllocator(Long.MaxValue)
    val properties = Map(
      "fs.storage_type" -> "local",
      "fs.root_path" -> dir.toAbsolutePath.toString
    ).asJava

    val perFile = 3000
    var readSchema: ArrowSchema = null
    var columnGroups = 0L
    var reader: MilvusStorageReader = null
    var nativeProperties: MilvusStorageProperties = null
    var batchReader = 0L

    try {
      ReadFixtures.copyTo("segments/multi-file", dir)
      val parts = (0 until 3).map(i => (s"part-$i.parquet", perFile.toLong))
      parts.map(_._2).sum shouldBe (3L * perFile)

      columnGroups = MilvusStorageColumnGroups.createFromGroups(
        Array(Array("id", "name")),
        Array(parts.map(_._1).toArray),
        Array(parts.map(_._2).toArray)
      )
      columnGroups should not be 0L

      readSchema = ArrowSchema.allocateNew(allocator)
      Data.exportSchema(allocator, schema, null, readSchema)
      nativeProperties = new MilvusStorageProperties()
      nativeProperties.create(properties)
      reader = new MilvusStorageReader()
      reader.create(
        columnGroups,
        readSchema.memoryAddress(),
        Array("id", "name"),
        nativeProperties
      )
      reader.isValid shouldBe true
      batchReader = reader.openRecordBatchReaderScala()

      var seen = 0
      var more = true
      while (more) {
        val array = ArrowArray.allocateNew(allocator)
        val batchSchema = ArrowSchema.allocateNew(allocator)
        try {
          more = reader.readNextBatchScala(
            batchReader,
            array.memoryAddress(),
            batchSchema.memoryAddress()
          )
          if (more) {
            val root =
              Data.importVectorSchemaRoot(allocator, array, batchSchema, null)
            try {
              val id = root.getVector("id").asInstanceOf[BigIntVector]
              var i = 0
              while (i < root.getRowCount) {
                id.get(i) shouldBe seen.toLong * 7
                seen += 1
                i += 1
              }
            } finally root.close()
          }
        } finally {
          array.close()
          batchSchema.close()
        }
      }

      // The whole point: every file's rows, not just the first file's.
      seen shouldBe (3 * perFile)
    } finally {
      if (batchReader != 0L) reader.destroyRecordBatchReaderScala(batchReader)
      if (reader != null) reader.destroy()
      if (nativeProperties != null) nativeProperties.free()
      if (columnGroups != 0L) MilvusStorageColumnGroups.destroy(columnGroups)
      if (readSchema != null) readSchema.close()
      allocator.close()
      Files
        .walk(dir)
        .sorted(Comparator.reverseOrder())
        .forEach(Files.deleteIfExists(_))
    }
  }
}
