package com.zilliz.spark.connector.loon

import io.milvus.storage._
import org.apache.arrow.c.{ArrowArrayStream, Data}
import org.apache.arrow.vector._
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.arrow.vector.types.pojo.{ArrowType, Field => ArrowField, FieldType, Schema => ArrowSchema}
import java.util.{HashMap => JHashMap}
import java.io.File
import scala.io.Source
import scala.collection.JavaConverters._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Milvus Storage FFI Test Suite (No Spark)
 *
 * This test demonstrates reading Milvus storage data without Spark.
 * It reads test data from src/test/data directory which uses Milvus Storage FFI writer to write data.
 *
 */
class MilvusStorageFFITest extends AnyFunSuite with Matchers {

  test("Read Milvus Storage data using FFI (No Spark)") {

    // Load native library
    info("Loading native library...")
    NativeLibraryLoader.loadLibrary()
    info("✓ Native library loaded successfully\n")

    // Default to src/test/data
    val testDataDir = new File("src/test/data")

    if (!testDataDir.exists()) {
      fail(s"Test data directory not found: ${testDataDir.getAbsolutePath}")
    }

    info(s"Test data directory: ${testDataDir.getAbsolutePath}")

    // Read and update manifest
    val manifestFile = new File(testDataDir, "manifest.json")
    if (!manifestFile.exists()) {
      fail(s"Manifest file not found: ${manifestFile.getAbsolutePath}")
    }

    info(s"Reading manifest: ${manifestFile.getAbsolutePath}")
    val originalManifest = Source.fromFile(manifestFile).mkString

    // Update manifest paths to point to actual test data location
    val updatedManifest = updateManifestPaths(originalManifest, testDataDir.getAbsolutePath)
    info("✓ Manifest loaded and paths updated\n")
    info("Updated manifest:")
    info(updatedManifest)

    // Create reader properties
    val readerProperties = new MilvusStorageProperties()
    val readerProps = new JHashMap[String, String]()
    readerProps.put("fs.storage_type", "local")
    readerProps.put("fs.root_path", "/")
    readerProperties.create(readerProps)

    readerProperties.isValid shouldBe true

    info("✓ Reader properties created\n")

    // Define schema - matching the test data structure
    val schema = createTestSchema()
    info("✓ Schema created\n")

    // Columns to read (matching C++ test schema order)
    val neededColumns = Array("id", "name", "value", "vector")
    info(s"Reading columns: ${neededColumns.mkString(", ")}\n")

    // Create reader
    val reader = new MilvusStorageReader()
    reader.create(updatedManifest, schema, neededColumns, readerProperties)
    info("✓ Reader created\n")

    // Read data using Arrow C Data Interface
    info("Reading data from Milvus storage...")
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

        info(s"\nSuccessfully read $totalRows rows in $batchCount batch(es)\n")

        totalRows should be > 0
        batchCount should be > 0
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
  }

  /**
   * Create test schema matching the test data
   */
  private def createTestSchema(): Long = {
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

  /**
   * Update manifest paths to point to actual test data location using JSON parser
   */
  private def updateManifestPaths(manifest: String, testDataDir: String): String = {
    try {
      // Create Jackson ObjectMapper with Scala module
      val mapper = new ObjectMapper()
      mapper.registerModule(DefaultScalaModule)
      
      // Parse the JSON manifest
      val rootNode = mapper.readTree(manifest)
      
      // Navigate to column_groups array
      val columnGroupsNode = rootNode.get("column_groups")
      if (columnGroupsNode != null && columnGroupsNode.isArray) {
        val columnGroupsArray = columnGroupsNode.asInstanceOf[ArrayNode]
        
        // Update paths for each column group
        for (i <- 0 until columnGroupsArray.size()) {
          val columnGroup = columnGroupsArray.get(i).asInstanceOf[ObjectNode]
          val pathsNode = columnGroup.get("paths")
          
          if (pathsNode != null && pathsNode.isArray) {
            val pathsArray = pathsNode.asInstanceOf[ArrayNode]
            
            // Update each path in the paths array
            for (j <- 0 until pathsArray.size()) {
              val currentPath = pathsArray.get(j).asText()
              if (currentPath.contains(s"column_group_${i}.parquet")) {
                val newPath = s"${testDataDir}/column_group_${i}.parquet"
                pathsArray.set(j, mapper.valueToTree(newPath).asInstanceOf[JsonNode])
              }
            }
          }
        }
      }
      
      // Convert back to JSON string
      mapper.writeValueAsString(rootNode)
      
    } catch {
      case e: Exception =>
        info(s"Warning: Failed to parse JSON manifest, ${e.getMessage}")
        manifest
    }
  }
}
