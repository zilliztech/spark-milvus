package com.zilliz.milvus.storage.io

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.util.{Collections, Comparator}
import scala.jdk.CollectionConverters._

import org.apache.arrow.c.{ArrowArray, ArrowSchema, Data}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{BigIntVector, VarCharVector, VectorSchemaRoot}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.jni.storage.NativeStorageLibrary
import io.milvus.storage.{
  MilvusStorageColumnGroups,
  MilvusStorageProperties,
  MilvusStorageReader
}

/** Verifies the direct upstream JNI column-group and Arrow batch contracts
  * against a frozen segment, independently of the connector's SegmentReader.
  */
class NativeSegmentFixtureTest extends AnyFunSuite with Matchers {

  private val expectedRows = 20480
  private val columns = Array("id", "name")
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

  private def loadLibrary(): Unit =
    try NativeStorageLibrary.load()
    catch {
      case missing: UnsatisfiedLinkError
          if Option(missing.getMessage).exists(
            _.startsWith("no milvus-storage-jni in java.library.path")
          ) =>
        cancel("libmilvus-storage-jni is not on this machine")
    }

  private def closeSchema(schema: ArrowSchema): Unit =
    if (schema != null) {
      try if (schema.snapshot().release != 0L) schema.release()
      finally schema.close()
    }

  test("upstream JNI reads every frozen row and schema through Arrow EOF") {
    loadLibrary()
    val directory = Files.createTempDirectory("native-segment-fixture")
    try {
      ReadFixtures.copyTo("segments/batch-20480", directory)
      val allocator = new RootAllocator(Long.MaxValue)
      try {
        var schemaStruct: ArrowSchema = null
        var columnGroups = 0L
        var properties: MilvusStorageProperties = null
        var reader: MilvusStorageReader = null
        var batchReader = 0L
        try {
          schemaStruct = ArrowSchema.allocateNew(allocator)
          schemaStruct.save(new ArrowSchema.Snapshot())
          Data.exportSchema(allocator, schema, null, schemaStruct)
          columnGroups = MilvusStorageColumnGroups.createFromGroups(
            Array(columns),
            Array(Array("segment.parquet")),
            Array(Array(expectedRows.toLong))
          )
          columnGroups should not be 0L

          properties = new MilvusStorageProperties()
          properties.create(
            Map(
              "fs.storage_type" -> "local",
              "fs.root_path" -> directory.toAbsolutePath.toString
            ).asJava
          )
          reader = new MilvusStorageReader()
          reader.create(
            columnGroups,
            schemaStruct.memoryAddress(),
            columns,
            properties
          )
          reader.isValid shouldBe true
          batchReader = reader.openRecordBatchReaderScala()
          batchReader should not be 0L

          var delivered = 0
          var batches = 0
          var more = true
          while (more) {
            val array = ArrowArray.allocateNew(allocator)
            var batchSchema: ArrowSchema = null
            try {
              batchSchema = ArrowSchema.allocateNew(allocator)
              batchSchema.save(new ArrowSchema.Snapshot())
              more = reader.readNextBatchScala(
                batchReader,
                array.memoryAddress(),
                batchSchema.memoryAddress()
              )
              if (more) {
                // Import borrowed struct wrappers so this scope still owns
                // their allocations, including when import or assertions fail.
                val importedSchema = Data.importSchema(
                  allocator,
                  ArrowSchema.wrap(batchSchema.memoryAddress()),
                  null
                )
                val root = VectorSchemaRoot.create(importedSchema, allocator)
                try {
                  Data.importIntoVectorSchemaRoot(
                    allocator,
                    ArrowArray.wrap(array.memoryAddress()),
                    root,
                    null
                  )
                  val fields = root.getSchema.getFields.asScala
                  fields.map(_.getName).toSeq shouldBe columns.toSeq
                  fields.map(_.getType).toSeq shouldBe schema.getFields.asScala
                    .map(_.getType)
                    .toSeq
                  fields.map(_.isNullable).toSeq shouldBe Seq(false, false)
                  root.getFieldVectors.size() shouldBe columns.length
                  root.getRowCount should be > 0

                  val id = root.getVector("id").asInstanceOf[BigIntVector]
                  val name = root.getVector("name").asInstanceOf[VarCharVector]
                  id.getValueCount shouldBe root.getRowCount
                  name.getValueCount shouldBe root.getRowCount
                  var row = 0
                  while (row < root.getRowCount) {
                    id.isNull(row) shouldBe false
                    name.isNull(row) shouldBe false
                    id.get(row) shouldBe delivered.toLong * 7
                    new String(name.get(row), UTF_8) shouldBe s"row-$delivered"
                    delivered += 1
                    row += 1
                  }
                  batches += 1
                } finally root.close()
              } else {
                // JNI must signal actual EOF, with neither C struct owning a
                // batch. Reaching the expected count alone is not sufficient.
                array.snapshot().release shouldBe 0L
                batchSchema.snapshot().release shouldBe 0L
              }
            } finally {
              try {
                try if (array.snapshot().release != 0L) array.release()
                finally array.close()
              } finally closeSchema(batchSchema)
            }
          }
          delivered shouldBe expectedRows
          batches should be >= 3
        } finally {
          try {
            if (batchReader != 0L)
              reader.destroyRecordBatchReaderScala(batchReader)
          } finally {
            try if (reader != null) reader.destroy()
            finally {
              try if (properties != null) properties.free()
              finally {
                try
                  if (columnGroups != 0L)
                    MilvusStorageColumnGroups.destroy(columnGroups)
                finally closeSchema(schemaStruct)
              }
            }
          }
        }
        allocator.getAllocatedMemory shouldBe 0L
      } finally allocator.close()
    } finally {
      val paths = Files.walk(directory)
      try
        paths.sorted(Comparator.reverseOrder()).forEach(Files.deleteIfExists(_))
      finally paths.close()
    }
  }
}
