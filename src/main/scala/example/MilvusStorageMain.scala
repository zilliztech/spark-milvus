package example

import io.milvus.storage._
import org.apache.arrow.c.{ArrowArrayStream, Data}
import org.apache.arrow.vector._
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.arrow.vector.types.pojo.{ArrowType, Field => ArrowField, FieldType, Schema => ArrowSchema}
import java.util.{HashMap => JHashMap}
import java.io.File
import scala.io.Source
import scala.collection.JavaConverters._

/**
 * Milvus Storage Main Application
 *
 * This application demonstrates reading Milvus storage data.
 * It reads test data from src/test/data directory by default.
 *
 * Usage:
 *   spark-submit --class example.MilvusStorageMain target/scala-2.13/spark-connector-assembly-0.1.14-SNAPSHOT.jar [test_data_directory]
 */
object MilvusStorageMain {

  def main(args: Array[String]): Unit = {
    try {
      // Load native library
      NativeLibraryLoader.loadLibrary()

      // Determine test data directory
      val testDataDir = if (args.length > 0) {
        new File(args(0))
      } else {
        // Default to src/test/data
        new File("src/test/data")
      }

      if (!testDataDir.exists()) {
        throw new RuntimeException(s"Test data directory not found: ${testDataDir.getAbsolutePath}")
      }

      println(s"Test data directory: ${testDataDir.getAbsolutePath}")

      // Read and update manifest
      val manifestFile = new File(testDataDir, "manifest.json")
      if (!manifestFile.exists()) {
        throw new RuntimeException(s"Manifest file not found: ${manifestFile.getAbsolutePath}")
      }

      println(s"Reading manifest: ${manifestFile.getAbsolutePath}")
      val originalManifest = Source.fromFile(manifestFile).mkString

      // Update manifest paths to point to actual test data location
      val updatedManifest = updateManifestPaths(originalManifest, testDataDir.getAbsolutePath)
      println("Manifest loaded and paths updated\n")
      println("Updated manifest:")
      println(updatedManifest)
      println()

      // Create reader properties
      val readerProperties = new MilvusStorageProperties()
      val readerProps = new JHashMap[String, String]()
      readerProps.put("fs.storage_type", "local")
      readerProps.put("fs.root_path", "/")
      readerProperties.create(readerProps)

      if (!readerProperties.isValid) {
        throw new RuntimeException("Failed to create valid reader properties")
      }

      // Define schema - matching the test data structure
      val schema = createTestSchema()

      // Columns to read (matching C++ test schema order)
      val neededColumns = Array("id", "name", "value", "vector")

      // Create reader
      val reader = new MilvusStorageReader()
      reader.create(updatedManifest, schema, neededColumns, readerProperties)

      // Read data using Arrow C Data Interface
      val recordBatchReaderPtr = reader.getRecordBatchReaderScala(null, 1024, 8 * 1024 * 1024)

      // Wrap the C++ ArrowArrayStream pointer with Arrow Java's ArrowArrayStream
      val allocator = ArrowUtils.getAllocator
      val arrowArrayStream = ArrowArrayStream.wrap(recordBatchReaderPtr)

      try {
        // Import the stream to get an ArrowReader
        val arrowReader = Data.importArrayStream(allocator, arrowArrayStream)

        try {
          // Read all batches using Arrow's standard iteration
          var batchCount = 0
          var totalRows = 0

          while (arrowReader.loadNextBatch()) {
            batchCount += 1
            val root = arrowReader.getVectorSchemaRoot
            val rows = displayBatchData(root, batchCount)
            totalRows += rows
          }

          println(s"Successfully read $totalRows rows in $batchCount batch(es)\n")
        } finally {
          arrowReader.close()
        }
      } finally {
        arrowArrayStream.close()
      }

      // Clean up
      reader.destroy()
      readerProperties.free()
      ArrowUtils.releaseArrowSchema(schema)
    } catch {
      case e: Exception =>
        println(s"\n✗ Error: ${e.getMessage}")
        e.printStackTrace()
        System.exit(1)
    }
  }

  /**
   * Create test schema matching the test data
   */
  private def createTestSchema(): Long = {
    val allocator = ArrowUtils.getAllocator

    val metadata0 = new JHashMap[String, String]()
    metadata0.put("PARQUET:field_id", "100")
    val metadata1 = new JHashMap[String, String]()
    metadata1.put("PARQUET:field_id", "101")
    val metadata2 = new JHashMap[String, String]()
    metadata2.put("PARQUET:field_id", "102")
    val metadata3 = new JHashMap[String, String]()
    metadata3.put("PARQUET:field_id", "103")

    // Define fields matching the test data
    // Schema from C++ test: id (int64), name (utf8), value (double), vector (list<float>)
    val elementChildren = new java.util.ArrayList[ArrowField]()
    elementChildren.add(new ArrowField("element", new FieldType(false, new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE), null), null))

    val fields = new java.util.ArrayList[ArrowField]()
    fields.add(new ArrowField("id", new FieldType(false, new ArrowType.Int(64, true), null, metadata0), null))
    fields.add(new ArrowField("name", new FieldType(false, new ArrowType.Utf8(), null, metadata1), null))
    fields.add(new ArrowField("value", new FieldType(false, new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE), null, metadata2), null))
    fields.add(new ArrowField("vector", new FieldType(false, new ArrowType.List(), null, metadata3), elementChildren))

    val schema = new ArrowSchema(fields)
    val arrowSchema = org.apache.arrow.c.ArrowSchema.allocateNew(allocator)
    org.apache.arrow.c.Data.exportSchema(allocator, schema, null, arrowSchema)

    arrowSchema.memoryAddress()
  }

  /**
   * Display data from VectorSchemaRoot
   */
  private def displayBatchData(root: VectorSchemaRoot, batchNum: Int): Int = {
    try {
      val rowCount = root.getRowCount
      println(s"\n--- Batch $batchNum: $rowCount rows ---")

      // Extract columns (matching C++ test schema: id, name, value, vector)
      val idVector = root.getVector("id").asInstanceOf[BigIntVector]
      val nameVector = root.getVector("name").asInstanceOf[VarCharVector]
      val valueVector = root.getVector("value").asInstanceOf[Float8Vector]
      val vectorListVector = root.getVector("vector").asInstanceOf[org.apache.arrow.vector.complex.ListVector]

      // Display first few rows
      val displayCount = Math.min(10, rowCount)
      println(String.format("\n%-10s %-20s %-15s %s", "ID", "Name", "Value", "Vector"))
      println("-" * 80)

      var i = 0
      while (i < displayCount) {
        val id = if (!idVector.isNull(i)) idVector.get(i).toString else "null"
        val name = if (!nameVector.isNull(i)) new String(nameVector.get(i), "UTF-8") else "null"
        val value = if (!valueVector.isNull(i)) String.format("%.1f", valueVector.get(i).asInstanceOf[AnyRef]) else "null"

        // Extract vector elements (variable-length list)
        val vectorStr = if (!vectorListVector.isNull(i)) {
          val vectorSlice = vectorListVector.getObject(i).asInstanceOf[java.util.List[Float]]
          val sb = new StringBuilder("[")
          var j = 0
          while (j < vectorSlice.size()) {
            if (j > 0) sb.append(", ")
            sb.append(String.format("%.1f", vectorSlice.get(j).asInstanceOf[AnyRef]))
            j += 1
          }
          sb.append("]")
          sb.toString
        } else {
          "null"
        }

        println(String.format("%-10s %-20s %-15s %s", id, name, value, vectorStr))
        i += 1
      }

      if (rowCount > displayCount) {
        println(s"... and ${rowCount - displayCount} more rows")
      }

      rowCount
    } catch {
      case e: Exception =>
        println(s"Error displaying data: ${e.getMessage}")
        e.printStackTrace()
        0
    }
  }

  /**
   * Update manifest paths to point to actual test data location
   */
  private def updateManifestPaths(manifest: String, testDataDir: String): String = {
    // Simple path replacement - replace the temp paths with actual paths
    var updated = manifest

    var i = 0
    while (i < 3) {
      // Replace any path to column_group_i.parquet with the actual path
      val pattern = s""""paths":\\s*\\[\\s*"[^"]*column_group_${i}\\.parquet"\\s*\\]"""
      val replacement = s""""paths": ["${testDataDir}/column_group_${i}.parquet"]"""
      updated = updated.replaceAll(pattern, replacement)
      i += 1
    }

    updated
  }
}
