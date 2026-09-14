package com.zilliz.milvus.storage.io

import java.nio.file.{Files, Path}
import scala.collection.JavaConverters._

import org.apache.arrow.c.{ArrowArray, ArrowSchema, Data}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{BigIntVector, VarCharVector, VectorSchemaRoot}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.jni.storage.StorageNative
import com.zilliz.milvus.storage.read.exec.{
  SegmentReader,
  SegmentReaderRegistry
}
import com.zilliz.milvus.storage.read.plan.InputSpec
import com.zilliz.milvus.storage.snapshot.V2ColumnGroup
import com.zilliz.milvus.storage.snapshot.SegmentLayout

/** Writes a segment through our JNI and reads it back through our JNI.
  *
  * This is the strongest check the write path has without a live Milvus: it
  * exercises both directions of the Arrow boundary and the column groups the
  * writer hands back, and the values have to survive the round trip.
  *
  * It is also the regression test for milvus-storage#493, which is why the row
  * count is what it is. `reader.record_batch_max_rows` defaults to 8192, so the
  * packed reader only starts handing back sliced batches
  * (`rb->Slice(min_rows)`) once more than 16384 rows are read. Below that the
  * offset materialization in segment_reader_jni.cpp never runs, and a test that
  * stayed under the boundary would pass just as happily against the broken
  * ArrowArrayStream path.
  *
  * The check itself is that values stay strictly in step with the row offset.
  * If a sliced batch were imported without honouring `ArrowArray.offset`, every
  * batch after the first would restart from row 0 and the comparison fails.
  *
  * Needs libnative-storage-jni, so it skips where the library is absent, which
  * is the state of CI.
  */
class WriterRoundTripTest extends AnyFunSuite with Matchers {

  /** Over 2 * 8192, so the reader slices. See the class comment. */
  private val rows = 20480

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

  private val schema = new Schema(
    Seq(
      new Field(
        "id",
        new FieldType(false, new ArrowType.Int(64, true), null),
        java.util.Collections.emptyList[Field]()
      ),
      new Field(
        "name",
        new FieldType(false, new ArrowType.Utf8(), null),
        java.util.Collections.emptyList[Field]()
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

  test("a segment written through our JNI reads back with the same values") {
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
    var writer = 0L
    var written = 0L
    var segmentReader: SegmentReader = null

    try {
      writeSchema = ArrowSchema.allocateNew(allocator)
      Data.exportSchema(allocator, schema, null, writeSchema)

      // --- write ---
      writer = StorageNative.writerNew(
        "segment",
        writeSchema.memoryAddress(),
        properties
      )
      writer should not be 0L

      val source = VectorSchemaRoot.create(schema, allocator)
      try {
        fill(source)
        val array = ArrowArray.allocateNew(allocator)
        try {
          Data.exportVectorSchemaRoot(allocator, source, null, array)
          StorageNative.writerWrite(writer, array.memoryAddress())
          StorageNative.writerFlush(writer)
        } finally array.close()
      } finally source.close()

      written = StorageNative.writerClose(writer, null, null)
      written should not be 0L

      val groupCount = StorageNative.nativeColumnGroupsCount(written)
      groupCount should be > 0
      val files = (0 until groupCount).map(
        StorageNative.nativeColumnGroupFiles(written, _)
      )
      val counts = (0 until groupCount).map(
        StorageNative.nativeColumnGroupRowCounts(written, _)
      )
      val columns = (0 until groupCount).map(
        StorageNative.nativeColumnGroupColumns(written, _)
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
      val spec = InputSpec(
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
      segmentReader = SegmentReaderRegistry.open(
        spec,
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
    } finally {
      if (segmentReader != null) segmentReader.close()
      if (written != 0L) StorageNative.nativeColumnGroupsDestroy(written)
      if (writer != 0L) StorageNative.writerDestroy(writer)
      if (writeSchema != null) writeSchema.close()
      allocator.close()
      Files
        .walk(dir)
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(Files.deleteIfExists(_))
    }
  }

  /** Writes `count` rows starting at `from` into its own segment under `dir`,
    * and returns that segment's single file path plus its row count.
    */
  private def writeSegment(
      allocator: RootAllocator,
      properties: java.util.Map[String, String],
      segment: String,
      from: Int,
      count: Int
  ): (String, Long) = {
    val schemaStruct = ArrowSchema.allocateNew(allocator)
    var handle = 0L
    var groups = 0L
    try {
      Data.exportSchema(allocator, schema, null, schemaStruct)
      handle = StorageNative.writerNew(
        segment,
        schemaStruct.memoryAddress(),
        properties
      )
      val source = VectorSchemaRoot.create(schema, allocator)
      try {
        val id = source.getVector("id").asInstanceOf[BigIntVector]
        val name = source.getVector("name").asInstanceOf[VarCharVector]
        id.allocateNew(count)
        name.allocateNew(count)
        var i = 0
        while (i < count) {
          id.setSafe(i, (from + i).toLong * 7)
          name.setSafe(i, s"row-${from + i}".getBytes("UTF-8"))
          i += 1
        }
        source.setRowCount(count)
        val array = ArrowArray.allocateNew(allocator)
        try {
          Data.exportVectorSchemaRoot(allocator, source, null, array)
          StorageNative.writerWrite(handle, array.memoryAddress())
          StorageNative.writerFlush(handle)
        } finally array.close()
      } finally source.close()

      groups = StorageNative.writerClose(handle, null, null)
      val files = StorageNative.nativeColumnGroupFiles(groups, 0)
      val counts = StorageNative.nativeColumnGroupRowCounts(groups, 0)
      require(
        files.length == 1,
        s"expected one file per segment, got ${files.toSeq}"
      )
      (files(0), counts(0))
    } finally {
      if (groups != 0L) StorageNative.nativeColumnGroupsDestroy(groups)
      if (handle != 0L) StorageNative.writerDestroy(handle)
      schemaStruct.close()
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
    // The test above cannot catch this: the writer names its files
    // `<segment>/_data/<uuid>.parquet`, which is far past the small-string
    // threshold, so the characters live on the heap and survive the move.
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
    var reader = 0L
    var batchReader = 0L

    try {
      val parts = (0 until 3).map { i =>
        writeSegment(allocator, properties, s"segment-$i", i * perFile, perFile)
      }
      // Move each file to a name inside the small-string range. The path the C
      // layer is handed is relative to fs.root_path.
      val shortParts = parts.zip(shortNames).map { case ((path, count), name) =>
        Files.move(dir.resolve(path), dir.resolve(name))
        name.length should be < 16
        (name, count)
      }

      columnGroups = StorageNative.columnGroupsCreate(
        Array(Array("id", "name")),
        Array(shortParts.map(_._1).toArray),
        Array(shortParts.map(_._2).toArray),
        "parquet"
      )
      columnGroups should not be 0L

      readSchema = ArrowSchema.allocateNew(allocator)
      Data.exportSchema(allocator, schema, null, readSchema)
      reader = StorageNative.readerNew(
        columnGroups,
        readSchema.memoryAddress(),
        Array("id", "name"),
        properties
      )
      reader should not be 0L
      batchReader = StorageNative.recordBatchReaderNew(reader, null)

      var seen = 0
      var more = true
      while (more) {
        val array = ArrowArray.allocateNew(allocator)
        val batchSchema = ArrowSchema.allocateNew(allocator)
        try {
          more = StorageNative.recordBatchReaderReadNext(
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
      if (batchReader != 0L) StorageNative.recordBatchReaderDestroy(batchReader)
      if (reader != 0L) StorageNative.readerDestroySegment(reader)
      if (columnGroups != 0L) StorageNative.columnGroupsDestroy(columnGroups)
      if (readSchema != null) readSchema.close()
      allocator.close()
      Files
        .walk(dir)
        .sorted(java.util.Comparator.reverseOrder())
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
    var reader = 0L
    var batchReader = 0L

    try {
      val parts = (0 until 3).map { i =>
        writeSegment(allocator, properties, s"segment-$i", i * perFile, perFile)
      }
      parts.map(_._2).sum shouldBe (3L * perFile)

      columnGroups = StorageNative.columnGroupsCreate(
        Array(Array("id", "name")),
        Array(parts.map(_._1).toArray),
        Array(parts.map(_._2).toArray),
        "parquet"
      )
      columnGroups should not be 0L

      readSchema = ArrowSchema.allocateNew(allocator)
      Data.exportSchema(allocator, schema, null, readSchema)
      reader = StorageNative.readerNew(
        columnGroups,
        readSchema.memoryAddress(),
        Array("id", "name"),
        properties
      )
      reader should not be 0L
      batchReader = StorageNative.recordBatchReaderNew(reader, null)

      var seen = 0
      var more = true
      while (more) {
        val array = ArrowArray.allocateNew(allocator)
        val batchSchema = ArrowSchema.allocateNew(allocator)
        try {
          more = StorageNative.recordBatchReaderReadNext(
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
      if (batchReader != 0L) StorageNative.recordBatchReaderDestroy(batchReader)
      if (reader != 0L) StorageNative.readerDestroySegment(reader)
      if (columnGroups != 0L) StorageNative.columnGroupsDestroy(columnGroups)
      if (readSchema != null) readSchema.close()
      allocator.close()
      Files
        .walk(dir)
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(Files.deleteIfExists(_))
    }
  }
}
