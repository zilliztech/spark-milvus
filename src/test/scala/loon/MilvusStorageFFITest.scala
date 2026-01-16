package com.zilliz.spark.connector.loon

import io.milvus.storage._
import org.apache.arrow.c.{ArrowArray, ArrowArrayStream, ArrowSchema => CArrowSchema, Data}
import org.apache.arrow.vector._
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.types.pojo.{ArrowType, Field => ArrowField, FieldType, Schema => ArrowSchema}
import java.util.{HashMap => JHashMap}
import java.io.File
import java.nio.file.{Files, Path}
import scala.collection.JavaConverters._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Milvus Storage FFI Test Suite (No Spark)
 *
 * This test demonstrates writing and reading Milvus storage data without Spark.
 * It uses MilvusStorageWriter to write data, then passes the column groups to reader.
 *
 */
class MilvusStorageFFITest extends AnyFunSuite with Matchers {

  test("Write and Read Milvus Storage data using FFI (No Spark)") {

    // Load native library
    info("Loading native library...")
    NativeLibraryLoader.loadLibrary()
    info("✓ Native library loaded successfully\n")

    // Create temporary directory for test data
    val tempDir = Files.createTempDirectory("milvus_ffi_test_")
    info(s"Test data directory: ${tempDir.toAbsolutePath}")

    val allocator = ArrowUtils.getAllocator

    try {
      // Create properties
      val properties = new MilvusStorageProperties()
      val props = new JHashMap[String, String]()
      props.put("fs.storage_type", "local")
      props.put("fs.root_path", "/")
      properties.create(props)

      properties.isValid shouldBe true
      info("✓ Properties created\n")

      // Create schema - keep the CArrowSchema object for proper cleanup
      val (writerSchema, writerSchemaPtr) = createTestSchema()
      info("✓ Schema created\n")

      // ===== WRITE PHASE =====
      info("Writing test data...")

      // Create writer
      val writer = new MilvusStorageWriter()
      writer.create(tempDir.toAbsolutePath.toString, writerSchemaPtr, properties)
      info("✓ Writer created\n")

      // Create and write test data
      val numRows = 100
      val vectorDim = 4
      val (root, arrowArray, arrowArrayPtr) = createTestData(allocator, numRows, vectorDim)

      try {
        writer.write(arrowArrayPtr)
        info(s"✓ Wrote $numRows rows\n")

        writer.flush()
        info("✓ Flushed writer\n")

        // Close writer and get column groups
        val columnGroupsPtr = writer.close()
        info(s"✓ Writer closed, column groups ptr: $columnGroupsPtr\n")

        // ===== READ PHASE =====
        info("Reading data back...")

        // Create a new schema for reader (schema was consumed by writer)
        val (readerSchema, readerSchemaPtr) = createTestSchema()

        // Columns to read
        val neededColumns = Array("id", "name", "value", "vector")
        info(s"Reading columns: ${neededColumns.mkString(", ")}\n")

        // Create reader with column groups from writer
        val reader = new MilvusStorageReader()
        reader.create(columnGroupsPtr, readerSchemaPtr, neededColumns, properties)
        info("✓ Reader created\n")

        // Read data using Arrow C Data Interface
        val recordBatchReaderPtr = reader.getRecordBatchReaderScala()
        val arrowArrayStream = ArrowArrayStream.wrap(recordBatchReaderPtr)

        try {
          val arrowReader = Data.importArrayStream(allocator, arrowArrayStream)

          try {
            var batchCount = 0
            var totalRows = 0

            while (arrowReader.loadNextBatch()) {
              batchCount += 1
              val readRoot = arrowReader.getVectorSchemaRoot
              val rows = displayBatchData(readRoot, batchCount)
              totalRows += rows
            }

            info(s"\nSuccessfully read $totalRows rows in $batchCount batch(es)\n")

            totalRows should be(numRows)
            batchCount should be > 0
          } finally {
            arrowReader.close()
          }
        } finally {
          arrowArrayStream.close()
        }

        // Clean up reader - use close() on Java objects, not native free()
        reader.destroy()
        readerSchema.close()

      } finally {
        root.close()
        arrowArray.close()
      }

      // Clean up - use close() on Java objects, not native free()
      writer.destroy()
      properties.free()
      writerSchema.close()

    } finally {
      // Clean up temp directory
      deleteDirectory(tempDir.toFile)
    }
  }

  /**
  * Recursively delete a directory
  */
  private def deleteDirectory(dir: File): Unit = {
    if (dir.exists()) {
      if (dir.isDirectory) {
        dir.listFiles().foreach(deleteDirectory)
      }
      dir.delete()
    }
  }

  /**
   * Create test data and return VectorSchemaRoot, ArrowArray object and its pointer
   * Returns the ArrowArray object so it can be properly closed later
   */
  private def createTestData(allocator: org.apache.arrow.memory.BufferAllocator, numRows: Int, vectorDim: Int): (VectorSchemaRoot, ArrowArray, Long) = {
    val metadata0 = Map("PARQUET:field_id" -> "100").asJava
    val metadata1 = Map("PARQUET:field_id" -> "101").asJava
    val metadata2 = Map("PARQUET:field_id" -> "102").asJava
    val metadata3 = Map("PARQUET:field_id" -> "103").asJava

    val fields = List(
      new ArrowField("id", new FieldType(false, new ArrowType.Int(64, true), null, metadata0), null),
      new ArrowField("name", new FieldType(false, new ArrowType.Utf8(), null, metadata1), null),
      new ArrowField("value", new FieldType(false, new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE), null, metadata2), null),
      new ArrowField("vector",
        new FieldType(false, new ArrowType.List(), null, metadata3),
        List(new ArrowField("element", new FieldType(false, new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE), null), null)).asJava)
    ).asJava

    val schema = new ArrowSchema(fields)
    val root = VectorSchemaRoot.create(schema, allocator)

    // Get vectors
    val idVector = root.getVector("id").asInstanceOf[BigIntVector]
    val nameVector = root.getVector("name").asInstanceOf[VarCharVector]
    val valueVector = root.getVector("value").asInstanceOf[Float8Vector]
    val vectorListVector = root.getVector("vector").asInstanceOf[ListVector]

    // Allocate space
    idVector.allocateNew(numRows)
    nameVector.allocateNew(numRows)
    valueVector.allocateNew(numRows)

    // Fill data
    for (i <- 0 until numRows) {
      idVector.set(i, i.toLong)
      nameVector.setSafe(i, s"name_$i".getBytes("UTF-8"))
      valueVector.set(i, i * 1.5)

      // Write vector data
      val writer = vectorListVector.getWriter
      writer.setPosition(i)
      writer.startList()
      for (j <- 0 until vectorDim) {
        writer.float4().writeFloat4(i * 0.1f + j)
      }
      writer.endList()
    }

    // Set row count
    root.setRowCount(numRows)

    // Export to C Data Interface - keep the ArrowArray object for proper cleanup
    val arrowArray = ArrowArray.allocateNew(allocator)
    Data.exportVectorSchemaRoot(allocator, root, null, arrowArray)

    (root, arrowArray, arrowArray.memoryAddress())
  }

  /**
   * Create test schema matching the test data
   * Returns both the CArrowSchema object and its memory address for proper cleanup
   */
  private def createTestSchema(): (CArrowSchema, Long) = {
    val allocator = ArrowUtils.getAllocator

    val metadata0 = Map("PARQUET:field_id" -> "100").asJava
    val metadata1 = Map("PARQUET:field_id" -> "101").asJava
    val metadata2 = Map("PARQUET:field_id" -> "102").asJava
    val metadata3 = Map("PARQUET:field_id" -> "103").asJava

    // Define fields matching the test data
    // Schema from C++ test: id (int64), name (utf8), value (double), vector (list<float>)
    val fields = List(
      new ArrowField("id", new FieldType(false, new ArrowType.Int(64, true), null, metadata0), null),
      new ArrowField("name", new FieldType(false, new ArrowType.Utf8(), null, metadata1), null),
      new ArrowField("value", new FieldType(false, new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE), null, metadata2), null),
      new ArrowField("vector",
        new FieldType(false,
          new ArrowType.List(), // Variable-length list
          null,
          metadata3),
        List(new ArrowField("element", new FieldType(false, new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE), null), null)).asJava)
    ).asJava

    val schema = new ArrowSchema(fields)
    val arrowSchema = CArrowSchema.allocateNew(allocator)
    Data.exportSchema(allocator, schema, null, arrowSchema)

    (arrowSchema, arrowSchema.memoryAddress())
  }

  /**
   * Display data from VectorSchemaRoot
   */
  private def displayBatchData(root: VectorSchemaRoot, batchNum: Int): Int = {
    try {
      val rowCount = root.getRowCount
      info(s"\n--- Batch $batchNum: $rowCount rows ---")

      // Extract columns (matching C++ test schema: id, name, value, vector)
      val idVector = root.getVector("id").asInstanceOf[BigIntVector]
      val nameVector = root.getVector("name").asInstanceOf[VarCharVector]
      val valueVector = root.getVector("value").asInstanceOf[Float8Vector]
      val vectorListVector = root.getVector("vector").asInstanceOf[org.apache.arrow.vector.complex.ListVector]

      // Display first few rows
      val displayCount = Math.min(10, rowCount)
      info(f"\n${"ID"}%-10s ${"Name"}%-20s ${"Value"}%-15s ${"Vector"}")
      info("-" * 80)

      for (i <- 0 until displayCount) {
        val id = if (!idVector.isNull(i)) idVector.get(i).toString else "null"
        val name = if (!nameVector.isNull(i)) new String(nameVector.get(i), "UTF-8") else "null"
        val value = if (!valueVector.isNull(i)) f"${valueVector.get(i)}%.1f" else "null"

        // Extract vector elements (variable-length list)
        val vectorStr = if (!vectorListVector.isNull(i)) {
          val vectorSlice = vectorListVector.getObject(i).asInstanceOf[java.util.List[Float]]
          vectorSlice.asScala.map(v => f"$v%.1f").mkString("[", ", ", "]")
        } else {
          "null"
        }

        info(f"$id%-10s $name%-20s $value%-15s $vectorStr")
      }

      if (rowCount > displayCount) {
        info(s"... and ${rowCount - displayCount} more rows")
      }

      rowCount
    } catch {
      case e: Exception =>
        info(s"Error displaying data: ${e.getMessage}")
        e.printStackTrace()
        0
    }
  }

}
