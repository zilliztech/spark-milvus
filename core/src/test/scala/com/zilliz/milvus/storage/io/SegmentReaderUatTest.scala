package com.zilliz.milvus.storage.io

import java.nio.file.Paths
import java.util.Collections
import scala.collection.JavaConverters._

import org.apache.arrow.c.{ArrowArray, ArrowSchema, Data}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.milvus.storage.{
  MilvusStorageColumnGroups,
  MilvusStorageProperties,
  MilvusStorageReader,
  NativeLibraryLoader
}

/** Reads a real Milvus V2 column-group parquet through the upstream JNI, start
  * to finish: column groups, reader, per-batch Arrow, row count.
  *
  * Driven by an environment variable so it stays out of CI, which has neither
  * the native library nor the file:
  *
  * {{{
  * MILVUS_JNI_LOCAL_FILE=/abs/path/to/segment.parquet \
  *   sbt 'core/testOnly *SegmentReaderUatTest'
  * }}}
  */
class SegmentReaderUatTest extends AnyFunSuite with Matchers {

  private def uatFile: String =
    sys.env
      .get("MILVUS_JNI_LOCAL_FILE")
      .map(_.trim)
      .filter(_.nonEmpty)
      .getOrElse(cancel("set MILVUS_JNI_LOCAL_FILE to a real segment parquet"))

  /** Builds the Arrow schema from the parquet schema.
    *
    * A production caller passes the collection schema; here the file is the
    * only source of truth available, and milvus-storage does not write an
    * `ARROW:schema` key (its keys are storage_version, group_field_id_list and
    * row_group_metadata). Only the primitive types a Milvus column group can
    * hold are handled — an unknown one cancels rather than guessing.
    */
  private def arrowSchemaOf(path: String): Schema = {
    val reader = ParquetFileReader.open(
      HadoopInputFile.fromPath(
        new HadoopPath(path),
        new Configuration()
      )
    )
    try {
      val message = reader.getFooter.getFileMetaData.getSchema
      val fields = message.getFields.asScala.toSeq.map { column =>
        val primitive = column.asPrimitiveType().getPrimitiveTypeName
        val arrowType: ArrowType = primitive match {
          case PrimitiveTypeName.INT64   => new ArrowType.Int(64, true)
          case PrimitiveTypeName.INT32   => new ArrowType.Int(32, true)
          case PrimitiveTypeName.BOOLEAN => new ArrowType.Bool()
          case PrimitiveTypeName.FLOAT =>
            new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
          case PrimitiveTypeName.DOUBLE =>
            new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
          case PrimitiveTypeName.BINARY => new ArrowType.Binary()
          case PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY =>
            new ArrowType.FixedSizeBinary(
              column.asPrimitiveType().getTypeLength
            )
          case other =>
            cancel(
              s"unhandled parquet type $other for column ${column.getName}"
            )
        }
        val nullable = !column.isRepetition(Repetition.REQUIRED)
        new Field(
          column.getName,
          new FieldType(nullable, arrowType, null),
          Collections.emptyList[Field]()
        )
      }
      new Schema(fields.asJava)
    } finally reader.close()
  }

  private def rowCountOf(path: String): Long = {
    val reader = ParquetFileReader.open(
      HadoopInputFile.fromPath(
        new HadoopPath(path),
        new Configuration()
      )
    )
    try reader.getFooter.getBlocks.asScala.map(_.getRowCount).sum
    finally reader.close()
  }

  test("reads every row of a real segment through the upstream JNI") {
    val path = uatFile
    // Same gate the other native suites use: touch the library and skip when
    // it is absent, which is the state of CI.
    try
      NativeLibraryLoader.loadLibrary()
    catch {
      case _: UnsatisfiedLinkError | _: NoClassDefFoundError =>
        cancel("libmilvus-storage-jni is not on this machine")
      case _: RuntimeException =>
        cancel("libmilvus-storage-jni is not on this machine")
    }

    val schema = arrowSchemaOf(path)
    val expectedRows = rowCountOf(path)
    val columns: Seq[String] = schema.getFields.asScala.map(_.getName).toSeq
    info(s"schema has ${columns.size} columns, file has $expectedRows rows")

    val allocator = new RootAllocator(Long.MaxValue)
    var schemaStruct: ArrowSchema = null
    var columnGroups = 0L
    var reader: MilvusStorageReader = null
    var nativeProperties: MilvusStorageProperties = null
    var batchReader = 0L
    var delivered = 0L
    var batches = 0

    try {
      schemaStruct = ArrowSchema.allocateNew(allocator)
      Data.exportSchema(allocator, schema, null, schemaStruct)

      // The C layer roots the local backend at fs.root_path and appends the
      // key, so the directory goes in the properties and the column group
      // carries the file name.
      val file = Paths.get(path).toAbsolutePath
      columnGroups = MilvusStorageColumnGroups.createFromGroups(
        Array(columns.toArray),
        Array(Array(file.getFileName.toString)),
        Array(Array(expectedRows)),
        "parquet"
      )
      columnGroups should not be 0L

      val properties = Map(
        "fs.storage_type" -> "local",
        "fs.root_path" -> file.getParent.toString
      ).asJava
      nativeProperties = new MilvusStorageProperties()
      nativeProperties.create(properties)
      reader = new MilvusStorageReader()
      reader.create(
        columnGroups,
        schemaStruct.memoryAddress(),
        columns.toArray,
        nativeProperties
      )
      reader.isValid shouldBe true

      batchReader = reader.openRecordBatchReaderScala()
      batchReader should not be 0L

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
            val root: VectorSchemaRoot =
              Data.importVectorSchemaRoot(allocator, array, batchSchema, null)
            try {
              batches += 1
              delivered += root.getRowCount.toLong
              root.getFieldVectors.size() shouldBe columns.size
            } finally root.close()
          }
        } finally {
          array.close()
          batchSchema.close()
        }
      }

      info(s"read $delivered rows in $batches batches")
      delivered shouldBe expectedRows
      batches should be > 0
    } finally {
      if (batchReader != 0L) reader.destroyRecordBatchReaderScala(batchReader)
      if (reader != null) reader.destroy()
      if (nativeProperties != null) nativeProperties.free()
      if (columnGroups != 0L) MilvusStorageColumnGroups.destroy(columnGroups)
      if (schemaStruct != null) schemaStruct.close()
      // Closing the allocator asserts every buffer was released, so a handle
      // this test forgot fails it rather than passing quietly.
      allocator.close()
    }
  }
}
