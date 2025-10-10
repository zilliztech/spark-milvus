package com.zilliz.spark.connector

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.SparkSession
import scala.util.Random
import io.milvus.grpc.schema.DataType
import com.zilliz.spark.connector.loon.Properties

/**
 * Integration test for MilvusDataReader with Storage V2 segments
 *
 * Prerequisites:
 * - Milvus 2.6+ running at localhost:19530
 * - Minio running at localhost:9000
 * - Native library libmilvus-storage.so loaded via LD_PRELOAD
 */
class MilvusDataReaderTest extends AnyFunSuite with BeforeAndAfterAll {

  var spark: SparkSession = _
  var milvusClient: MilvusClient = _

  val collectionName = s"test_datareader_v2_${System.currentTimeMillis()}"
  val dim = 128
  val batchSize = 10000
  val batchCount = 10

  override def beforeAll(): Unit = {
    // Initialize Spark
    spark = SparkSession.builder()
      .appName("MilvusDataReaderTest")
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
      // Clean up
      milvusClient.dropCollection("", collectionName)
    } finally {
      if (milvusClient != null) milvusClient.close()
      if (spark != null) spark.stop()
    }
  }

  test("Read Storage V2 collection using MilvusDataReader") {
    val config = MilvusDataReaderConfig(
      uri = "http://localhost:19530",
      token = "root:Milvus",
      collectionName = collectionName,
      options = Map(
        MilvusOption.MilvusDatabaseName -> "default",
        Properties.FsConfig.FsAddress -> "localhost:9000",
        Properties.FsConfig.FsBucketName -> "a-bucket",
        Properties.FsConfig.FsRootPath -> "files",
        Properties.FsConfig.FsAccessKeyId -> "minioadmin",
        Properties.FsConfig.FsAccessKeyValue -> "minioadmin",
        Properties.FsConfig.FsUseSSL -> "false"
      )
    )

    val df = MilvusDataReader.read(spark, config)

    println("\n=== Schema ===")
    df.printSchema()

    println("\n=== Data Sample ===")
    df.show(10, truncate = false)

    // Verify row count (should equal inserted data since no deletes)
    val actualCount = df.count()
    val expectedCount = batchSize * batchCount
    assert(actualCount == expectedCount,
      s"Expected $expectedCount rows but got $actualCount")

    // Verify schema - should not have row_id and timestamp columns (they are dropped)
    val fieldNames = df.schema.fieldNames.toSet
    assert(!fieldNames.contains("row_id"), "row_id should be dropped")
    assert(!fieldNames.contains("timestamp"), "timestamp should be dropped")
    assert(fieldNames.contains("id"), "id field should be present")
    assert(fieldNames.contains("int64"), "int64 field should be present")
    assert(fieldNames.contains("float"), "float field should be present")
    assert(fieldNames.contains("varchar"), "varchar field should be present")
    assert(fieldNames.contains("vector"), "vector field should be present")
  }

  test("Query Storage V2 data with Spark SQL") {
    val config = MilvusDataReaderConfig(
      uri = "http://localhost:19530",
      token = "root:Milvus",
      collectionName = collectionName,
      options = Map(
        MilvusOption.MilvusDatabaseName -> "default",
        Properties.FsConfig.FsAddress -> "localhost:9000",
        Properties.FsConfig.FsBucketName -> "a-bucket",
        Properties.FsConfig.FsRootPath -> "files",
        Properties.FsConfig.FsAccessKeyId -> "minioadmin",
        Properties.FsConfig.FsAccessKeyValue -> "minioadmin",
        Properties.FsConfig.FsUseSSL -> "false"
      )
    )

    val df = MilvusDataReader.read(spark, config)

    // Register as temp view
    df.createOrReplaceTempView("milvus_data")

    // Run SQL queries
    println("\n=== SQL Query: Filter by int64 < 5 ===")
    val filtered = spark.sql("SELECT id, int64, float FROM milvus_data WHERE int64 < 5")
    filtered.show()

    assert(filtered.count() > 0, "Filtered query should return results")

    println("\n=== SQL Query: Aggregation ===")
    val agg = spark.sql("SELECT COUNT(*) as total, AVG(float) as avg_float FROM milvus_data")
    agg.show()

    println("\nSuccessfully executed Spark SQL queries")
  }

  test("Verify automatic V2 format detection") {
    // Create a new client for this test to avoid gRPC timeout issues
    val testClient = MilvusClient(
      MilvusConnectionParams(
        uri = "http://localhost:19530",
        token = "root:Milvus",
        databaseName = "default"
      )
    )

    try {
      // Verify segments are V2
      val segments = testClient.getSegments("default", collectionName).get
      assert(segments.nonEmpty, "Collection should have segments")
      assert(segments.forall(_.storageVersion >= 2),
        "All segments should be V2 (storageVersion >= 2)")

      val config = MilvusDataReaderConfig(
        uri = "http://localhost:19530",
        token = "root:Milvus",
        collectionName = collectionName,
        options = Map(
          MilvusOption.MilvusDatabaseName -> "default",
          Properties.FsConfig.FsAddress -> "localhost:9000",
          Properties.FsConfig.FsBucketName -> "a-bucket",
          Properties.FsConfig.FsRootPath -> "files",
          Properties.FsConfig.FsAccessKeyId -> "minioadmin",
          Properties.FsConfig.FsAccessKeyValue -> "minioadmin",
          Properties.FsConfig.FsUseSSL -> "false"
        )
      )

      // Read data - should automatically detect and use storagev2 format
      val df = MilvusDataReader.read(spark, config)

      // Verify we can read the data successfully
      val count = df.count()
      assert(count == batchSize * batchCount,
        s"Should read all ${batchSize * batchCount} rows")
    } finally {
      testClient.close()
    }
  }

  // Helper method to prepare test collection
  private def prepareTestCollection(): Unit = {
    // Drop and recreate collection
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
      description = "Test collection for MilvusDataReader with Storage V2",
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

    // Wait a bit for segments to be sealed and storage version to be set
    Thread.sleep(2000)
  }
}
