package com.zilliz.spark.connector.operations.backfill

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.util.Random
import scala.sys.process._
import scala.util.{Try, Success, Failure}

import com.zilliz.spark.connector.{MilvusClient, MilvusConnectionParams, MilvusFieldData, MilvusOption}
import com.zilliz.spark.connector.loon.Properties
import io.milvus.grpc.schema.DataType
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import java.io.File

/**
 * Integration test for MilvusBackfill operation
 *
 * Prerequisites:
 * - Milvus 2.6+ running at localhost:19530
 * - Minio running at localhost:9000
 */
class MilvusBackfillTest extends AnyFunSuite with BeforeAndAfterAll {

  var spark: SparkSession = _
  var milvusClient: MilvusClient = _

  val collectionName = s"backfilltestcollection_${System.currentTimeMillis()}"
  val snapshotName = s"backfill_snapshot_${System.currentTimeMillis()}"
  val dim = 128
  val batchSize = 10000
  val batchCount = 10
  val s3Bucket = "a-bucket"

  // Jackson mapper for JSON parsing
  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  override def beforeAll(): Unit = {
    // Initialize Spark
    spark = SparkSession.builder()
      .appName("MilvusDataSourceTest")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // Initialize Milvus client
    milvusClient = MilvusClient(
      MilvusConnectionParams(
        uri = "http://localhost:19530",
        token = "root:Milvus",
        databaseName = "default"
      )
    )

    // Prepare test data
    prepareTestCollection()
  }

  override def afterAll(): Unit = {
    try {
      // Clean up snapshot first
      dropSnapshotViaPython(snapshotName)
      // Then drop collection
      milvusClient.dropCollection("", collectionName)
    } finally {
      if (milvusClient != null) milvusClient.close()
      if (spark != null) spark.stop()
    }
  }

  /**
   * Create a snapshot using Python script (pymilvus has snapshot API, milvus-proto doesn't yet)
   * Returns the S3 location path
   */
  private def createSnapshotViaPython(collectionName: String, snapshotName: String, description: String = ""): String = {
    val scriptPath = new File("scripts/create_snapshot.py").getAbsolutePath
    val cmd = Seq("python3", scriptPath, collectionName, snapshotName, description)

    info(s"Creating snapshot via Python: $cmd")

    val output = cmd.!!
    info(s"Python output: $output")

    val result = mapper.readValue(output, classOf[Map[String, Any]])

    if (result.contains("error")) {
      throw new RuntimeException(s"Failed to create snapshot: ${result("error")}")
    }

    val s3Location = result("s3_location").toString
    info(s"Snapshot created at S3 location: $s3Location")

    // Return as s3a:// path for Spark/Hadoop
    s"s3a://$s3Bucket/$s3Location"
  }

  /**
   * Drop snapshot using Python script
   */
  private def dropSnapshotViaPython(snapshotName: String): Unit = {
    Try {
      val cmd = Seq(
        "python3", "-c",
        s"""
from pymilvus import MilvusClient
client = MilvusClient(uri='http://localhost:19530', token='root:Milvus')
client.drop_snapshot('$snapshotName')
print('Snapshot dropped')
"""
      )
      cmd.!!
    } match {
      case Success(_) => info(s"Snapshot $snapshotName dropped")
      case Failure(e) => info(s"Failed to drop snapshot $snapshotName: ${e.getMessage}")
    }
  }

  test("Use MilvusBackfill API for backfill operation") {
    // Create backfill data with NULL values in the MIDDLE to test edge cases
    val totalRecords = batchSize * batchCount

    // Distribution:
    // - First 30%: Has backfill data
    // - Middle 40%: NULL (un-joined) ← Key test: NULL in the middle
    // - Last 30%: Has backfill data
    val firstRangeEnd = (totalRecords * 0.3).toInt
    val nullRangeStart = firstRangeEnd
    val nullRangeEnd = (totalRecords * 0.7).toInt
    val lastRangeStart = nullRangeEnd

    val recordsWithData = firstRangeEnd + (totalRecords - lastRangeStart)
    val expectedNullRecords = nullRangeEnd - nullRangeStart

    info(s"\n=== Test Setup ===")
    info(s"Total records in collection: $totalRecords")
    info(s"Backfill data distribution:")
    info(s"  Records 0-${firstRangeEnd-1}: Has backfill data ($firstRangeEnd rows)")
    info(s"  Records $nullRangeStart-${nullRangeEnd-1}: NO backfill data - will be NULL ($expectedNullRecords rows)")
    info(s"  Records $lastRangeStart-${totalRecords-1}: Has backfill data (${totalRecords - lastRangeStart} rows)")
    info(s"Total records with data: $recordsWithData")
    info(s"Total records with NULL: $expectedNullRecords")

    // Create backfill data: skip the middle range to create NULL gap
    val addFieldData = (0 until firstRangeEnd).map { i =>
      (i.toLong, s"api_new_value_$i", i * 2) // pk, new_field1, new_field2
    } ++ (lastRangeStart until totalRecords).map { i =>
      (i.toLong, s"api_new_value_$i", i * 2)
    }

    val addFieldDF = spark.createDataFrame(addFieldData).toDF("pk", "new_field1", "new_field2")

    val tempDir = new File(System.getProperty("java.io.tmpdir"))
    val parquetPath = new File(tempDir, s"new_field_data_${System.currentTimeMillis()}.parquet").getAbsolutePath
    addFieldDF.write.mode("overwrite").parquet(parquetPath)
    addFieldDF.show(10, truncate = false)

    try {
      // Create backfill configuration
      val config = BackfillConfig(
        milvusUri = "http://localhost:19530",
        milvusToken = "root:Milvus",
        databaseName = "default",
        collectionName = collectionName,
        s3Endpoint = "localhost:9000",
        s3BucketName = "a-bucket",
        s3AccessKey = "minioadmin",
        s3SecretKey = "minioadmin",
        s3UseSSL = false,
        s3RootPath = "files",
        s3Region = "us-east-1",
        batchSize = 512
      )

      // Create snapshot via Python (since milvus-proto doesn't have snapshot RPC yet)
      val snapshotPath = createSnapshotViaPython(
        collectionName,
        snapshotName,
        "add field backfill snapshot for test"
      )
      info(s"Using snapshot path: $snapshotPath")

      // Execute MilvusBackfill API
      val result = MilvusBackfill.run(
        spark,
        parquetPath,
        snapshotPath,
        config
      )

      // Verify result
      result match {
        case Right(success) =>
          info("\n=== Backfill API Result ===")
          info(s"  Collection ID: ${success.collectionId}")
          info(s"  Partition ID: ${success.partitionId}")
          info(s"  Segments processed: ${success.segmentResults.size}")
          info(s"  New fields: ${success.newFieldNames.mkString(", ")}")
          info(s"  Total execution time: ${success.executionTimeMs}ms")

          info("\n  Per-Segment Results ===")
          var totalRowsProcessed = 0L
          success.segmentResults.toSeq.sortBy(_._1).foreach { case (segmentId, segResult) =>
            info(s"  Segment $segmentId:")
            info(s"    Rows: ${segResult.rowCount}")
            info(s"    Execution time: ${segResult.executionTimeMs}ms")
            info(s"    Output path: ${segResult.outputPath}")
            info(s"    Manifest paths: ${segResult.manifestPaths.mkString(", ")}")
            totalRowsProcessed += segResult.rowCount
          }

          // Assertions
          assert(success.segmentResults.nonEmpty, "Should process at least one segment")
          assert(success.newFieldNames.contains("new_field1"), "Should contain new_field1")
          assert(success.newFieldNames.contains("new_field2"), "Should contain new_field2")

          // Verify all rows were processed (including those with nulls)
          info(s"\n=== Null Handling Verification ===")
          info(s"  Total rows in collection: $totalRecords")
          info(s"  Total rows processed: $totalRowsProcessed")
          info(s"  Rows with backfill data (joined): $recordsWithData")
          info(s"  Expected rows with nulls (un-joined): $expectedNullRecords")

          assert(totalRowsProcessed == totalRecords,
            s"Should process ALL rows including un-joined ones. Expected: $totalRecords, Got: $totalRowsProcessed")

          info(s"All $totalRecords rows were processed")
          info(s"This includes $recordsWithData rows with backfill data and $expectedNullRecords rows with NULL values")
          info(s"NULL gap in middle range ($nullRangeStart-${nullRangeEnd-1}) handled correctly")

        case Left(error) =>
          fail(s"Backfill API failed: ${error.message}")
      }

    } finally {
      // Delete temporary Parquet file
      val parquetFile = new File(parquetPath)
      if (parquetFile.exists()) {
        def deleteRecursively(file: File): Unit = {
          if (file.isDirectory) {
            file.listFiles().foreach(deleteRecursively)
          }
          file.delete()
        }
        deleteRecursively(parquetFile)
      }
    }
  }

  // Helper method to prepare test collection
  private def prepareTestCollection(): Unit = {
    milvusClient.dropCollection("", collectionName)

    val fields = List(
      milvusClient.createCollectionField("id", isPrimary = true, dataType = DataType.Int64, autoID = false),
      milvusClient.createCollectionField("int64", dataType = DataType.Int64, isClusteringKey = true),
      milvusClient.createCollectionField("float", dataType = DataType.Float),
      milvusClient.createCollectionField("varchar", dataType = DataType.VarChar, typeParams = Map("max_length" -> "1024")),
      milvusClient.createCollectionField("vector", dataType = DataType.FloatVector, typeParams = Map("dim" -> dim.toString))
    )

    val schema = milvusClient.createCollectionSchema(
      name = collectionName,
      fields = fields,
      description = "Test collection for MilvusBackfill",
      enableAutoID = false,
      enableDynamicSchema = false
    )

    milvusClient.createCollection("", collectionName, schema, shardsNum = 1)

    // Generate and insert test data
    val random = new Random(42)
    for (i <- 0 until batchCount) {
      val idData = (0 until batchSize).map(j => (i * batchSize + j).toLong)
      val int64Data = (0 until batchSize).map(j => j.toLong)
      val floatData = (0 until batchSize).map(_ => random.nextFloat())
      val varcharData = (0 until batchSize).map(j =>
        s"test_string_${i * batchSize + j}")
      val vectorData = (0 until batchSize).map(_ =>
        (0 until dim).map(_ => random.nextFloat()).toSeq)

      val fieldsData = Seq(
        MilvusFieldData.packInt64FieldData("id", idData),
        MilvusFieldData.packInt64FieldData("int64", int64Data),
        MilvusFieldData.packFloatFieldData("float", floatData),
        MilvusFieldData.packStringFieldData("varchar", varcharData),
        MilvusFieldData.packFloatVectorFieldData("vector", vectorData, dim)
      )

      milvusClient.insert("", collectionName, fieldsData = fieldsData, numRows = batchSize)
    }

    // Flush to ensure data is persisted
    milvusClient.flush("", Seq(collectionName))
    Thread.sleep(10000)
  }
}
