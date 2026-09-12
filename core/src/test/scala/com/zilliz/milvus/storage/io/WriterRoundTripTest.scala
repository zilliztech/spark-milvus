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

/** Writes a segment through our JNI and reads it back through our JNI.
  *
  * This is the strongest check the write path has without a live Milvus: it
  * exercises both directions of the Arrow boundary and the column groups the
  * writer hands back, and the values have to survive the round trip.
  *
  * Needs libnative-storage-jni, so it skips where the library is absent, which
  * is the state of CI.
  */
class WriterRoundTripTest extends AnyFunSuite with Matchers {

  private val rows = 5000

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
    var readSchema: ArrowSchema = null
    var writer = 0L
    var written = 0L
    var readColumnGroups = 0L
    var reader = 0L
    var batchReader = 0L

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

      // --- read back ---
      readColumnGroups = StorageNative.columnGroupsCreate(
        columns.map(_.toArray).toArray,
        files.map(_.toArray).toArray,
        counts.toArray,
        "parquet"
      )
      readColumnGroups should not be 0L

      readSchema = ArrowSchema.allocateNew(allocator)
      Data.exportSchema(allocator, schema, null, readSchema)
      reader = StorageNative.readerNew(
        readColumnGroups,
        readSchema.memoryAddress(),
        Array("id", "name"),
        properties
      )
      reader should not be 0L
      batchReader = StorageNative.recordBatchReaderNew(reader, null)
      batchReader should not be 0L

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
              val name = root.getVector("name").asInstanceOf[VarCharVector]
              var i = 0
              while (i < root.getRowCount) {
                id.get(i) shouldBe seen.toLong * 7
                new String(name.get(i), "UTF-8") shouldBe s"row-$seen"
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

      seen shouldBe rows
    } finally {
      if (batchReader != 0L) StorageNative.recordBatchReaderDestroy(batchReader)
      if (reader != 0L) StorageNative.readerDestroySegment(reader)
      if (readColumnGroups != 0L)
        StorageNative.columnGroupsDestroy(readColumnGroups)
      if (written != 0L) StorageNative.nativeColumnGroupsDestroy(written)
      if (writer != 0L) StorageNative.writerDestroy(writer)
      if (readSchema != null) readSchema.close()
      if (writeSchema != null) writeSchema.close()
      allocator.close()
      Files
        .walk(dir)
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(Files.deleteIfExists(_))
    }
  }
}
