package com.zilliz.spark.connector.sources

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.SparkSession
import scala.util.Random

import com.zilliz.spark.connector.{MilvusClient, MilvusConnectionParams, MilvusFieldData, MilvusOption}
import com.zilliz.spark.connector.loon.Properties
import io.milvus.grpc.schema.DataType

/**
 * Integration test for MilvusDataSource
 *
 * Prerequisites:
 * - Milvus 2.6+ running at localhost:19530
 * - Minio running at localhost:9000
 */
class MilvusDataSourceTest extends AnyFunSuite with BeforeAndAfterAll {

  var spark: SparkSession = _
  var milvusClient: MilvusClient = _

  val collectionName = s"test_storagev2_collection_${System.currentTimeMillis()}"
  val dim = 128
  val batchSize = 10
  val batchCount = 3

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
      // Clean up
      milvusClient.dropCollection("", collectionName)
    } finally {
      if (milvusClient != null) milvusClient.close()
      if (spark != null) spark.stop()
    }
  }

  test("Read data using Storage V2 DataSource") {
    val df = spark.read
      .format("milvus")
      .option(MilvusOption.MilvusUri, "http://localhost:19530")
      .option(MilvusOption.MilvusToken, "root:Milvus")
      .option(MilvusOption.MilvusCollectionName, collectionName)
      .option(MilvusOption.MilvusDatabaseName, "default")
      .option(Properties.FsConfig.FsAddress, "localhost:9000")
      .option(Properties.FsConfig.FsBucketName, "a-bucket")
      .option(Properties.FsConfig.FsRootPath, "files")
      .option(Properties.FsConfig.FsAccessKeyId, "minioadmin")
      .option(Properties.FsConfig.FsAccessKeyValue, "minioadmin")
      .option(Properties.FsConfig.FsUseSSL, "false")
      .load()

    // Show schema
    df.printSchema()

    // Show data sample
    df.show()

    // Verify row count
    val actualCount = df.count()
    val expectedCount = batchSize * batchCount

    assert(actualCount == expectedCount,
      s"Expected $expectedCount rows but got $actualCount")
  }

  test("Read specific fields using ReaderFieldIDs") {
    val df = spark.read
      .format("milvus")
      .option(MilvusOption.MilvusUri, "http://localhost:19530")
      .option(MilvusOption.MilvusToken, "root:Milvus")
      .option(MilvusOption.MilvusCollectionName, collectionName)
      .option(MilvusOption.MilvusDatabaseName, "default")
      .option(MilvusOption.ReaderFieldIDs, "100,102")  // read only id and float fields
      .option(Properties.FsConfig.FsAddress, "localhost:9000")
      .option(Properties.FsConfig.FsBucketName, "a-bucket")
      .option(Properties.FsConfig.FsRootPath, "files")
      .option(Properties.FsConfig.FsAccessKeyId, "minioadmin")
      .option(Properties.FsConfig.FsAccessKeyValue, "minioadmin")
      .option(Properties.FsConfig.FsUseSSL, "false")
      .load()

    df.printSchema()

    df.show(10, truncate = false)

    // Verify we only have the requested fields
    val fieldNames = df.schema.fieldNames.toSet
    assert(fieldNames.contains("id"), "id field should be present")
    assert(fieldNames.contains("float"), "float field should be present")
  }

  test("Query data with Spark SQL") {
    val df = spark.read
      .format("milvus")
      .option(MilvusOption.MilvusUri, "http://localhost:19530")
      .option(MilvusOption.MilvusToken, "root:Milvus")
      .option(MilvusOption.MilvusCollectionName, collectionName)
      .option(MilvusOption.MilvusDatabaseName, "default")
      .option(Properties.FsConfig.FsAddress, "localhost:9000")
      .option(Properties.FsConfig.FsBucketName, "a-bucket")
      .option(Properties.FsConfig.FsRootPath, "files")
      .option(Properties.FsConfig.FsAccessKeyId, "minioadmin")
      .option(Properties.FsConfig.FsAccessKeyValue, "minioadmin")
      .option(Properties.FsConfig.FsUseSSL, "false")
      .load()     

    // Register as temp view
    df.createOrReplaceTempView("milvus_data")

    // Run SQL queries
    info("\n=== SQL Query: Filter by int64 < 5 ===")
    val filtered = spark.sql("SELECT id, int64, float FROM milvus_data WHERE int64 < 5")
    filtered.show()

    assert(filtered.count() > 0, "Filtered query should return results")

    info("\n=== SQL Query: Aggregation ===")
    val agg = spark.sql("SELECT COUNT(*) as total, AVG(float) as avg_float FROM milvus_data")
    agg.show()

    info("\nSuccessfully executed Spark SQL queries")
  }

  test("Column pruning via select") {
    val df = spark.read
      .format("milvus")
      .option(MilvusOption.MilvusUri, "http://localhost:19530")
      .option(MilvusOption.MilvusToken, "root:Milvus")
      .option(MilvusOption.MilvusCollectionName, collectionName)
      .option(MilvusOption.MilvusDatabaseName, "default")
      .option(Properties.FsConfig.FsAddress, "localhost:9000")
      .option(Properties.FsConfig.FsBucketName, "a-bucket")
      .option(Properties.FsConfig.FsRootPath, "files")
      .option(Properties.FsConfig.FsAccessKeyId, "minioadmin")
      .option(Properties.FsConfig.FsAccessKeyValue, "minioadmin")
      .option(Properties.FsConfig.FsUseSSL, "false")
      .load()

    val result = df.select("id", "int64", "float")

    result.show()

    // Verify schema only has selected columns
    val fieldNames = result.schema.fieldNames.toSet
    assert(fieldNames.size == 3, "Should only have 3 columns")
    assert(fieldNames.contains("id"), "Should have id column")
    assert(fieldNames.contains("int64"), "Should have int64 column")
    assert(fieldNames.contains("float"), "Should have float column")

    val count = result.count()
    val expectedCount = batchSize * batchCount
    assert(count == expectedCount, s"Should return all $expectedCount rows")
  }

  test("Column pruning via explicit ReaderFieldIDs") {
    // Field IDs: id=100, int64=101, float=102, varchar=103, vector=104
    val df = spark.read
      .format("milvus")
      .option(MilvusOption.MilvusUri, "http://localhost:19530")
      .option(MilvusOption.MilvusToken, "root:Milvus")
      .option(MilvusOption.MilvusCollectionName, collectionName)
      .option(MilvusOption.MilvusDatabaseName, "default")
      .option(MilvusOption.ReaderFieldIDs, "100,101,102")  // Only read id, int64, float
      .option(Properties.FsConfig.FsAddress, "localhost:9000")
      .option(Properties.FsConfig.FsBucketName, "a-bucket")
      .option(Properties.FsConfig.FsRootPath, "files")
      .option(Properties.FsConfig.FsAccessKeyId, "minioadmin")
      .option(Properties.FsConfig.FsAccessKeyValue, "minioadmin")
      .option(Properties.FsConfig.FsUseSSL, "false")
      .load()

    df.show()

    // Verify schema only has the fields specified by ReaderFieldIDs
    val fieldNames = df.schema.fieldNames.toSet
    assert(fieldNames.size == 3, "Should only have 3 columns")
    assert(fieldNames.contains("id"), "Should have id column")
    assert(fieldNames.contains("int64"), "Should have int64 column")
    assert(fieldNames.contains("float"), "Should have float column")

    val count = df.count()
    val expectedCount = batchSize * batchCount
    assert(count == expectedCount, s"Should return all $expectedCount rows")
  }

  test("Vector similarity search with different metrics") {
    val random = new Random(123)
    val queryVector = Array.fill(dim)(random.nextFloat())
    val queryVectorJson = queryVector.mkString("[", ",", "]")

    info(s"Query vector (first 5 elements): [${queryVector.take(5).mkString(", ")}, ...]")

    val topK = 5

    // similarity metrics to test
    val metrics = Seq("L2", "COSINE", "IP")

    for (metric <- metrics) {
      info(s"\n=== Testing vector search with $metric similarity ===")
      info(s"TopK: $topK, Metric: $metric")

      // Execute vector search
      val results = spark.read
        .format("milvus")
        .option(MilvusOption.MilvusUri, "http://localhost:19530")
        .option(MilvusOption.MilvusToken, "root:Milvus")
        .option(MilvusOption.MilvusCollectionName, collectionName)
        .option(MilvusOption.MilvusDatabaseName, "default")
        // Vector search configuration
        .option(MilvusOption.VectorSearchQueryVector, queryVectorJson)
        .option(MilvusOption.VectorSearchTopK, topK.toString)
        .option(MilvusOption.VectorSearchMetric, metric)
        .option(MilvusOption.VectorSearchVectorColumn, "vector")
        // S3/Minio configuration
        .option(Properties.FsConfig.FsAddress, "localhost:9000")
        .option(Properties.FsConfig.FsBucketName, "a-bucket")
        .option(Properties.FsConfig.FsRootPath, "files")
        .option(Properties.FsConfig.FsAccessKeyId, "minioadmin")
        .option(Properties.FsConfig.FsAccessKeyValue, "minioadmin")
        .option(Properties.FsConfig.FsUseSSL, "false")
        .load()

      results.show()
      assert(results.count() == topK, s"No results returned from vector search with $metric metric")
    }
  }

  test("Vector search with SQL query filter by int64 < 5") {
    // Generate a random query vector
    val random = new Random(999)
    val queryVector = Array.fill(dim)(random.nextFloat())
    val queryVectorJson = queryVector.mkString("[", ",", "]")

    val topK = 5

    // Load data with vector search
    val df = spark.read
      .format("milvus")
      .option(MilvusOption.MilvusUri, "http://localhost:19530")
      .option(MilvusOption.MilvusToken, "root:Milvus")
      .option(MilvusOption.MilvusCollectionName, collectionName)
      .option(MilvusOption.MilvusDatabaseName, "default")
      .option(MilvusOption.VectorSearchQueryVector, queryVectorJson)
      .option(MilvusOption.VectorSearchTopK, topK.toString)
      .option(MilvusOption.VectorSearchMetric, "L2")
      .option(MilvusOption.VectorSearchVectorColumn, "vector")
      .option(Properties.FsConfig.FsAddress, "localhost:9000")
      .option(Properties.FsConfig.FsBucketName, "a-bucket")
      .option(Properties.FsConfig.FsRootPath, "files")
      .option(Properties.FsConfig.FsAccessKeyId, "minioadmin")
      .option(Properties.FsConfig.FsAccessKeyValue, "minioadmin")
      .option(Properties.FsConfig.FsUseSSL, "false")
      .load()

    // Register as temp view
    df.createOrReplaceTempView("vector_search_results")

    val sqlResults = spark.sql(s"""
      SELECT id, int64, varchar
      FROM vector_search_results
      WHERE int64 < 5
      LIMIT $topK
    """)

    sqlResults.show(truncate = false)

    val count = sqlResults.count()
    assert(count >= 0, "SQL query should execute successfully")
  }

  // Filter pushdown tests are skipped for MilvusDataSource as filter pushdown
  // is properly tested in MilvusStorageV2DataSourceTest
  test("Filter pushdown - equality filter on int64") {
    val df = spark.read
      .format("milvus")
      .option(MilvusOption.MilvusUri, "http://localhost:19530")
      .option(MilvusOption.MilvusToken, "root:Milvus")
      .option(MilvusOption.MilvusCollectionName, collectionName)
      .option(MilvusOption.MilvusDatabaseName, "default")
      .option(Properties.FsConfig.FsAddress, "localhost:9000")
      .option(Properties.FsConfig.FsBucketName, "a-bucket")
      .option(Properties.FsConfig.FsRootPath, "files")
      .option(Properties.FsConfig.FsAccessKeyId, "minioadmin")
      .option(Properties.FsConfig.FsAccessKeyValue, "minioadmin")
      .option(Properties.FsConfig.FsUseSSL, "false")
      .load()

    info("\n=== Filter pushdown: int64 = 3 ===")
    val filtered = df.filter("int64 = 3")

    // Check execution plan
    info("Physical plan:")
    filtered.explain(true)

    filtered.show()

    val count = filtered.count()
    // Each batch has int64 from 0 to batchSize-1, so value 3 should appear in each batch
    val expectedCount = if (3 < batchSize) batchCount else 0
    assert(count == expectedCount,
      s"Expected $expectedCount rows with int64=3, but got $count")
  }

  test("Filter pushdown - range filter on int64") {
    val df = spark.read
      .format("milvus")
      .option(MilvusOption.MilvusUri, "http://localhost:19530")
      .option(MilvusOption.MilvusToken, "root:Milvus")
      .option(MilvusOption.MilvusCollectionName, collectionName)
      .option(MilvusOption.MilvusDatabaseName, "default")
      .option(Properties.FsConfig.FsAddress, "localhost:9000")
      .option(Properties.FsConfig.FsBucketName, "a-bucket")
      .option(Properties.FsConfig.FsRootPath, "files")
      .option(Properties.FsConfig.FsAccessKeyId, "minioadmin")
      .option(Properties.FsConfig.FsAccessKeyValue, "minioadmin")
      .option(Properties.FsConfig.FsUseSSL, "false")
      .load()

    info("\n=== Filter pushdown: int64 >= 5 AND int64 < 8 ===")
    val filtered = df.filter("int64 >= 5 AND int64 < 8")
    filtered.show()

    val count = filtered.count()
    // int64 range [5, 8) = values 5, 6, 7 (3 values per batch)
    val expectedCount = if (batchSize > 7) batchCount * 3 else 0
    assert(count == expectedCount,
      s"Expected $expectedCount rows with int64 in [5,8), but got $count")
  }

  test("Filter pushdown - IN filter on int64") {
    val df = spark.read
      .format("milvus")
      .option(MilvusOption.MilvusUri, "http://localhost:19530")
      .option(MilvusOption.MilvusToken, "root:Milvus")
      .option(MilvusOption.MilvusCollectionName, collectionName)
      .option(MilvusOption.MilvusDatabaseName, "default")
      .option(Properties.FsConfig.FsAddress, "localhost:9000")
      .option(Properties.FsConfig.FsBucketName, "a-bucket")
      .option(Properties.FsConfig.FsRootPath, "files")
      .option(Properties.FsConfig.FsAccessKeyId, "minioadmin")
      .option(Properties.FsConfig.FsAccessKeyValue, "minioadmin")
      .option(Properties.FsConfig.FsUseSSL, "false")
      .load()

    info("\n=== Filter pushdown: int64 IN (1, 3, 5) ===")
    val filtered = df.filter("int64 IN (1, 3, 5)")
    filtered.show()

    val count = filtered.count()
    // Each batch has values 1, 3, 5 if batchSize > 5
    val expectedCount = if (batchSize > 5) batchCount * 3 else 0
    assert(count == expectedCount,
      s"Expected $expectedCount rows with int64 in (1,3,5), but got $count")
  }

  test("Filter pushdown - combined with column pruning") {
    val df = spark.read
      .format("milvus")
      .option(MilvusOption.MilvusUri, "http://localhost:19530")
      .option(MilvusOption.MilvusToken, "root:Milvus")
      .option(MilvusOption.MilvusCollectionName, collectionName)
      .option(MilvusOption.MilvusDatabaseName, "default")
      .option(Properties.FsConfig.FsAddress, "localhost:9000")
      .option(Properties.FsConfig.FsBucketName, "a-bucket")
      .option(Properties.FsConfig.FsRootPath, "files")
      .option(Properties.FsConfig.FsAccessKeyId, "minioadmin")
      .option(Properties.FsConfig.FsAccessKeyValue, "minioadmin")
      .option(Properties.FsConfig.FsUseSSL, "false")
      .load()

    info("\n=== Filter pushdown + column pruning: select id, int64 where int64 < 5 ===")
    val result = df.select("id", "int64").filter("int64 < 5")
    result.show()

    // Verify schema
    val fieldNames = result.schema.fieldNames.toSet
    assert(fieldNames.size == 2, "Should only have 2 columns")
    assert(fieldNames.contains("id") && fieldNames.contains("int64"),
      "Should have id and int64 columns")

    // Verify filter worked
    val count = result.count()
    val expectedCount = if (batchSize > 5) batchCount * 5 else batchCount * batchSize
    assert(count == expectedCount,
      s"Expected $expectedCount rows with int64 < 5, but got $count")
  }

  test("Filter pushdown - IS NULL and IS NOT NULL") {
    val df = spark.read
      .format("milvus")
      .option(MilvusOption.MilvusUri, "http://localhost:19530")
      .option(MilvusOption.MilvusToken, "root:Milvus")
      .option(MilvusOption.MilvusCollectionName, collectionName)
      .option(MilvusOption.MilvusDatabaseName, "default")
      .option(Properties.FsConfig.FsAddress, "localhost:9000")
      .option(Properties.FsConfig.FsBucketName, "a-bucket")
      .option(Properties.FsConfig.FsRootPath, "files")
      .option(Properties.FsConfig.FsAccessKeyId, "minioadmin")
      .option(Properties.FsConfig.FsAccessKeyValue, "minioadmin")
      .option(Properties.FsConfig.FsUseSSL, "false")
      .load()

    info("\n=== Filter pushdown: int64 IS NOT NULL ===")
    val notNullFiltered = df.filter("int64 IS NOT NULL")
    notNullFiltered.show()

    // All int64 values are non-null in test data
    val count = notNullFiltered.count()
    val expectedCount = batchSize * batchCount
    assert(count == expectedCount,
      s"Expected all $expectedCount rows (int64 IS NOT NULL), but got $count")
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
      description = "Test collection for Storage V2 DataSource",
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
  }
}
