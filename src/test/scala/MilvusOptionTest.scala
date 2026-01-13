package com.zilliz.spark.connector

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for MilvusOption parsing and validation
 */
class MilvusOptionTest extends AnyFunSuite with Matchers {

  test("Parse basic options with default values") {
    val options = Map(
      MilvusOption.MilvusUri -> "http://localhost:19530",
      MilvusOption.MilvusCollectionName -> "test_collection"
    )

    val milvusOption = MilvusOption(options)

    milvusOption.uri shouldBe "http://localhost:19530"
    milvusOption.collectionName shouldBe "test_collection"
    milvusOption.token shouldBe ""
    milvusOption.databaseName shouldBe ""
    milvusOption.retryCount shouldBe 3
    milvusOption.retryInterval shouldBe 1000
    milvusOption.insertMaxBatchSize shouldBe 5000
  }

  test("Parse all connection options") {
    val options = Map(
      MilvusOption.MilvusUri -> "https://milvus.example.com:19530",
      MilvusOption.MilvusToken -> "root:password",
      MilvusOption.MilvusDatabaseName -> "my_database",
      MilvusOption.MilvusCollectionName -> "my_collection",
      MilvusOption.MilvusPartitionName -> "partition_1",
      MilvusOption.MilvusRetryCount -> "5",
      MilvusOption.MilvusRetryInterval -> "2000",
      MilvusOption.MilvusInsertMaxBatchSize -> "10000"
    )

    val milvusOption = MilvusOption(options)

    milvusOption.uri shouldBe "https://milvus.example.com:19530"
    milvusOption.token shouldBe "root:password"
    milvusOption.databaseName shouldBe "my_database"
    milvusOption.collectionName shouldBe "my_collection"
    milvusOption.partitionName shouldBe "partition_1"
    milvusOption.retryCount shouldBe 5
    milvusOption.retryInterval shouldBe 2000
    milvusOption.insertMaxBatchSize shouldBe 10000
  }

  test("Parse segment and partition IDs") {
    val options = Map(
      MilvusOption.MilvusUri -> "http://localhost:19530",
      MilvusOption.MilvusCollectionID -> "123456789",
      MilvusOption.MilvusPartitionID -> "987654321",
      MilvusOption.MilvusSegmentID -> "111222333"
    )

    val milvusOption = MilvusOption(options)

    milvusOption.collectionID shouldBe "123456789"
    milvusOption.partitionID shouldBe "987654321"
    milvusOption.segmentID shouldBe "111222333"
  }

  test("Parse extra columns configuration") {
    val options = Map(
      MilvusOption.MilvusUri -> "http://localhost:19530",
      MilvusOption.MilvusExtraColumns -> "partition, segment_id, row_offset"
    )

    val milvusOption = MilvusOption(options)

    milvusOption.extraColumns should contain allOf ("partition", "segment_id", "row_offset")
    milvusOption.extraColumns.size shouldBe 3
  }

  test("Parse empty extra columns") {
    val options = Map(
      MilvusOption.MilvusUri -> "http://localhost:19530",
      MilvusOption.MilvusExtraColumns -> ""
    )

    val milvusOption = MilvusOption(options)

    milvusOption.extraColumns shouldBe empty
  }

  test("Parse fieldIDs configuration") {
    val options = Map(
      MilvusOption.MilvusUri -> "http://localhost:19530",
      MilvusOption.ReaderFieldIDs -> "100,101,102"
    )

    val milvusOption = MilvusOption(options)

    milvusOption.fieldIDs shouldBe "100,101,102"
  }

  test("Parse vector search configuration") {
    val options = Map(
      MilvusOption.MilvusUri -> "http://localhost:19530",
      MilvusOption.VectorSearchQueryVector -> "[0.1, 0.2, 0.3, 0.4]",
      MilvusOption.VectorSearchTopK -> "10",
      MilvusOption.VectorSearchMetric -> "L2",
      MilvusOption.VectorSearchVectorColumn -> "embedding"
    )

    val milvusOption = MilvusOption(options)

    milvusOption.vectorSearchConfig shouldBe defined
    val config = milvusOption.vectorSearchConfig.get
    config.queryVector shouldBe Array(0.1f, 0.2f, 0.3f, 0.4f)
    config.topK shouldBe 10
    config.metricType shouldBe "L2"
    config.vectorColumn shouldBe "embedding"
  }

  test("Parse vector search with COSINE metric") {
    val options = Map(
      MilvusOption.MilvusUri -> "http://localhost:19530",
      MilvusOption.VectorSearchQueryVector -> "[1.0, 2.0, 3.0]",
      MilvusOption.VectorSearchTopK -> "5",
      MilvusOption.VectorSearchMetric -> "cosine"
    )

    val milvusOption = MilvusOption(options)

    milvusOption.vectorSearchConfig shouldBe defined
    val config = milvusOption.vectorSearchConfig.get
    config.metricType shouldBe "COSINE"  // Should be uppercase
  }

  test("Vector search config is None when query vector is missing") {
    val options = Map(
      MilvusOption.MilvusUri -> "http://localhost:19530",
      MilvusOption.VectorSearchTopK -> "10"
    )

    val milvusOption = MilvusOption(options)

    milvusOption.vectorSearchConfig shouldBe None
  }

  test("Vector search config is None when topK is missing") {
    val options = Map(
      MilvusOption.MilvusUri -> "http://localhost:19530",
      MilvusOption.VectorSearchQueryVector -> "[0.1, 0.2, 0.3]"
    )

    val milvusOption = MilvusOption(options)

    milvusOption.vectorSearchConfig shouldBe None
  }

  test("Vector search uses default values for optional fields") {
    val options = Map(
      MilvusOption.MilvusUri -> "http://localhost:19530",
      MilvusOption.VectorSearchQueryVector -> "[0.5, 0.5]",
      MilvusOption.VectorSearchTopK -> "3"
    )

    val milvusOption = MilvusOption(options)

    milvusOption.vectorSearchConfig shouldBe defined
    val config = milvusOption.vectorSearchConfig.get
    config.metricType shouldBe "L2"  // Default metric
    config.vectorColumn shouldBe "vector"  // Default column name
  }

  test("isInt64PK returns true for int64 type") {
    MilvusOption.isInt64PK("int64") shouldBe true
    MilvusOption.isInt64PK("Int64") shouldBe true
    MilvusOption.isInt64PK("INT64") shouldBe true
  }

  test("isInt64PK returns false for non-int64 types") {
    MilvusOption.isInt64PK("string") shouldBe false
    MilvusOption.isInt64PK("varchar") shouldBe false
    MilvusOption.isInt64PK("") shouldBe false
  }

  test("vectorDimKey generates correct key format") {
    MilvusOption.vectorDimKey("embedding") shouldBe "vector.embedding.dim"
    MilvusOption.vectorDimKey("my_vector") shouldBe "vector.my_vector.dim"
  }

  test("Parse TLS/SSL configuration") {
    val options = Map(
      MilvusOption.MilvusUri -> "https://milvus.example.com:19530",
      MilvusOption.MilvusServerPemPath -> "/path/to/server.pem",
      MilvusOption.MilvusClientKeyPath -> "/path/to/client.key",
      MilvusOption.MilvusClientPemPath -> "/path/to/client.pem",
      MilvusOption.MilvusCaPemPath -> "/path/to/ca.pem"
    )

    val milvusOption = MilvusOption(options)

    milvusOption.serverPemPath shouldBe "/path/to/server.pem"
    milvusOption.clientKeyPath shouldBe "/path/to/client.key"
    milvusOption.clientPemPath shouldBe "/path/to/client.pem"
    milvusOption.caPemPath shouldBe "/path/to/ca.pem"
  }

  test("Options map is preserved") {
    val options = Map(
      MilvusOption.MilvusUri -> "http://localhost:19530",
      MilvusOption.MilvusCollectionName -> "test",
      "custom.option" -> "custom_value"
    )

    val milvusOption = MilvusOption(options)

    milvusOption.options should contain key "custom.option"
    milvusOption.options("custom.option") shouldBe "custom_value"
  }
}

/**
 * Unit tests for MilvusS3Option
 */
class MilvusS3OptionTest extends AnyFunSuite with Matchers {

  test("Parse S3 options with default values") {
    import scala.collection.JavaConverters._
    import org.apache.spark.sql.util.CaseInsensitiveStringMap

    val options = new CaseInsensitiveStringMap(Map(
      MilvusOption.ReaderType -> "insert"
    ).asJava)

    val s3Option = MilvusS3Option(options)

    s3Option.readerType shouldBe "insert"
    s3Option.s3BucketName shouldBe "a-bucket"
    s3Option.s3RootPath shouldBe "files"
    s3Option.s3Endpoint shouldBe "localhost:9000"
    s3Option.s3AccessKey shouldBe "minioadmin"
    s3Option.s3SecretKey shouldBe "minioadmin"
    s3Option.s3UseSSL shouldBe false
    s3Option.s3PathStyleAccess shouldBe true
    s3Option.s3MaxConnections shouldBe 32
    s3Option.s3PreloadPoolSize shouldBe 4
  }

  test("Parse custom S3 options") {
    import scala.collection.JavaConverters._
    import org.apache.spark.sql.util.CaseInsensitiveStringMap

    val options = new CaseInsensitiveStringMap(Map(
      MilvusOption.ReaderType -> "insert",
      MilvusOption.S3FileSystemTypeName -> "s3a://",
      MilvusOption.S3BucketName -> "my-bucket",
      MilvusOption.S3RootPath -> "data/milvus",
      MilvusOption.S3Endpoint -> "s3.amazonaws.com",
      MilvusOption.S3AccessKey -> "AKIAIOSFODNN7EXAMPLE",
      MilvusOption.S3SecretKey -> "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
      MilvusOption.S3UseSSL -> "true",
      MilvusOption.S3PathStyleAccess -> "false",
      MilvusOption.S3MaxConnections -> "64",
      MilvusOption.S3PreloadPoolSize -> "8"
    ).asJava)

    val s3Option = MilvusS3Option(options)

    s3Option.s3FileSystemType shouldBe "s3a://"
    s3Option.s3BucketName shouldBe "my-bucket"
    s3Option.s3RootPath shouldBe "data/milvus"
    s3Option.s3Endpoint shouldBe "s3.amazonaws.com"
    s3Option.s3AccessKey shouldBe "AKIAIOSFODNN7EXAMPLE"
    s3Option.s3SecretKey shouldBe "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
    s3Option.s3UseSSL shouldBe true
    s3Option.s3PathStyleAccess shouldBe false
    s3Option.s3MaxConnections shouldBe 64
    s3Option.s3PreloadPoolSize shouldBe 8
  }

  test("notEmpty helper function") {
    import scala.collection.JavaConverters._
    import org.apache.spark.sql.util.CaseInsensitiveStringMap

    val options = new CaseInsensitiveStringMap(Map(
      MilvusOption.ReaderType -> "insert"
    ).asJava)

    val s3Option = MilvusS3Option(options)

    s3Option.notEmpty("test") shouldBe true
    s3Option.notEmpty("  test  ") shouldBe true
    s3Option.notEmpty("") shouldBe false
    s3Option.notEmpty("   ") shouldBe false
    s3Option.notEmpty(null) shouldBe false
  }

  test("getFilePath generates correct S3 path") {
    import scala.collection.JavaConverters._
    import org.apache.spark.sql.util.CaseInsensitiveStringMap

    val options = new CaseInsensitiveStringMap(Map(
      MilvusOption.ReaderType -> "insert",
      MilvusOption.S3FileSystemTypeName -> "s3a://",
      MilvusOption.S3BucketName -> "test-bucket",
      MilvusOption.S3RootPath -> "files"
    ).asJava)

    val s3Option = MilvusS3Option(options)

    // Test relative path
    val path1 = s3Option.getFilePath("insert_log/123/456")
    path1.toString should include("s3a://test-bucket/files/insert_log/123/456")

    // Test absolute S3 path (should not be modified)
    val path2 = s3Option.getFilePath("s3a://other-bucket/other-path")
    path2.toString shouldBe "s3a://other-bucket/other-path"
  }

  test("getConf generates correct S3 configuration") {
    import scala.collection.JavaConverters._
    import org.apache.spark.sql.util.CaseInsensitiveStringMap

    val options = new CaseInsensitiveStringMap(Map(
      MilvusOption.ReaderType -> "insert",
      MilvusOption.S3FileSystemTypeName -> "s3a://",
      MilvusOption.S3BucketName -> "test-bucket",
      MilvusOption.S3Endpoint -> "localhost:9000",
      MilvusOption.S3AccessKey -> "test-key",
      MilvusOption.S3SecretKey -> "test-secret",
      MilvusOption.S3UseSSL -> "false",
      MilvusOption.S3PathStyleAccess -> "true",
      MilvusOption.S3MaxConnections -> "16"
    ).asJava)

    val s3Option = MilvusS3Option(options)
    val conf = s3Option.getConf()

    conf.get("fs.s3a.endpoint") shouldBe "localhost:9000"
    conf.get("fs.s3a.access.key") shouldBe "test-key"
    conf.get("fs.s3a.secret.key") shouldBe "test-secret"
    conf.get("fs.s3a.connection.ssl.enabled") shouldBe "false"
    conf.get("fs.s3a.path.style.access") shouldBe "true"
    conf.get("fs.s3a.threads.max") shouldBe "16"
    conf.get("fs.s3a.impl") shouldBe "org.apache.hadoop.fs.s3a.S3AFileSystem"
  }
}
