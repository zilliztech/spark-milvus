package com.zilliz.spark.connector.options

import java.io.{
  ByteArrayInputStream,
  ByteArrayOutputStream,
  ObjectInputStream,
  ObjectOutputStream
}

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.milvus.storage.expr.PlanParser
import com.zilliz.milvus.storage.read.plan.ReadLimits

/** Unit tests for MilvusOption parsing and validation
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
    milvusOption.readLimits shouldBe ReadLimits.Default
    milvusOption.writeFileRollingBytes shouldBe
      MilvusOption.DefaultWriteFileRollingBytes
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

  test("Parse extra columns configuration with canonical names") {
    val options = Map(
      MilvusOption.MilvusUri -> "http://localhost:19530",
      MilvusOption.MilvusExtraColumns ->
        "_segment_id, _row_offset, _timestamp"
    )

    val milvusOption = MilvusOption(options)

    milvusOption.extraColumns should contain allOf (
      MilvusOption.MilvusExtraColumnSegmentID,
      MilvusOption.MilvusExtraColumnRowOffset,
      MilvusOption.MilvusExtraColumnTimestamp
    )
    milvusOption.extraColumns.size shouldBe 3
  }

  test("Parse legacy extra column aliases as canonical requests") {
    val options = Map(
      MilvusOption.MilvusUri -> "http://localhost:19530",
      MilvusOption.MilvusExtraColumns ->
        "$segment_id, segment_id, $row_offset, row_offset, _timestamp"
    )

    val milvusOption = MilvusOption(options)

    milvusOption.extraColumns should contain allOf (
      "_segment_id",
      "_row_offset",
      "_timestamp"
    )
    milvusOption.extraColumns.size shouldBe 3
    milvusOption.extraColumns should not contain "segment_id"
    milvusOption.extraColumns should not contain "row_offset"
  }

  test("extra columns reject partition, unknown names and empty entries") {
    Seq("partition", "unknown", "_segment_id,,_row_offset").foreach { value =>
      val error = intercept[IllegalArgumentException] {
        MilvusOption(
          Map(MilvusOption.MilvusExtraColumns -> value)
        )
      }
      error.getMessage should include(MilvusOption.MilvusExtraColumns)
      error.getMessage should include(value)
    }
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

    milvusOption.vectorSearch shouldBe defined
    val config = milvusOption.vectorSearch.get
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

    milvusOption.vectorSearch shouldBe defined
    val config = milvusOption.vectorSearch.get
    config.metricType shouldBe "COSINE" // Should be uppercase
  }

  test("Vector search fails when query vector is missing") {
    val options = Map(
      MilvusOption.MilvusUri -> "http://localhost:19530",
      MilvusOption.VectorSearchTopK -> "10"
    )

    val error = intercept[IllegalArgumentException](MilvusOption(options))
    error.getMessage should include(MilvusOption.VectorSearchQueryVector)
    error.getMessage should include(MilvusOption.VectorSearchTopK)
  }

  test("Vector search fails when topK is missing") {
    val options = Map(
      MilvusOption.MilvusUri -> "http://localhost:19530",
      MilvusOption.VectorSearchQueryVector -> "[0.1, 0.2, 0.3]"
    )

    val error = intercept[IllegalArgumentException](MilvusOption(options))
    error.getMessage should include(MilvusOption.VectorSearchQueryVector)
    error.getMessage should include(MilvusOption.VectorSearchTopK)
  }

  test("Vector search fails on malformed values") {
    Seq(
      Map(
        MilvusOption.VectorSearchMetric -> "COSINE"
      ) -> MilvusOption.VectorSearchQueryVector,
      Map(
        MilvusOption.VectorSearchVectorColumn -> "embedding"
      ) -> MilvusOption.VectorSearchQueryVector,
      Map(
        MilvusOption.VectorSearchQueryVector -> "   ",
        MilvusOption.VectorSearchTopK -> "   "
      ) -> MilvusOption.VectorSearchQueryVector,
      Map(
        MilvusOption.VectorSearchQueryVector -> "   ",
        MilvusOption.VectorSearchTopK -> "10"
      ) -> MilvusOption.VectorSearchQueryVector,
      Map(
        MilvusOption.VectorSearchQueryVector -> "[0.1, 0.2]",
        MilvusOption.VectorSearchTopK -> "   "
      ) -> MilvusOption.VectorSearchTopK,
      Map(
        MilvusOption.VectorSearchQueryVector -> "0.1,0.2",
        MilvusOption.VectorSearchTopK -> "10"
      ) -> MilvusOption.VectorSearchQueryVector,
      Map(
        MilvusOption.VectorSearchQueryVector -> "[]",
        MilvusOption.VectorSearchTopK -> "10"
      ) -> MilvusOption.VectorSearchQueryVector,
      Map(
        MilvusOption.VectorSearchQueryVector -> "[0.1, nope]",
        MilvusOption.VectorSearchTopK -> "10"
      ) -> MilvusOption.VectorSearchQueryVector,
      Map(
        MilvusOption.VectorSearchQueryVector -> "[0.1, NaN]",
        MilvusOption.VectorSearchTopK -> "10"
      ) -> MilvusOption.VectorSearchQueryVector,
      Map(
        MilvusOption.VectorSearchQueryVector -> "[0.1,]",
        MilvusOption.VectorSearchTopK -> "10"
      ) -> MilvusOption.VectorSearchQueryVector,
      Map(
        MilvusOption.VectorSearchQueryVector -> "[0.1, 0.2]",
        MilvusOption.VectorSearchTopK -> "0"
      ) -> MilvusOption.VectorSearchTopK,
      Map(
        MilvusOption.VectorSearchQueryVector -> "[0.1, 0.2]",
        MilvusOption.VectorSearchTopK -> "10",
        MilvusOption.VectorSearchMetric -> "unknown"
      ) -> MilvusOption.VectorSearchMetric,
      Map(
        MilvusOption.VectorSearchQueryVector -> "[0.1, 0.2]",
        MilvusOption.VectorSearchTopK -> "10",
        MilvusOption.VectorSearchMetric -> "   "
      ) -> MilvusOption.VectorSearchMetric,
      Map(
        MilvusOption.VectorSearchQueryVector -> "[0.1, 0.2]",
        MilvusOption.VectorSearchTopK -> "10",
        MilvusOption.VectorSearchVectorColumn -> "   "
      ) -> MilvusOption.VectorSearchVectorColumn
    ).foreach { case (options, expectedMessage) =>
      val error = intercept[IllegalArgumentException](MilvusOption(options))
      error.getMessage should include(expectedMessage)
    }
  }

  test("Vector search uses default values for optional fields") {
    val options = Map(
      MilvusOption.MilvusUri -> "http://localhost:19530",
      MilvusOption.VectorSearchQueryVector -> "[0.5, 0.5]",
      MilvusOption.VectorSearchTopK -> "3"
    )

    val milvusOption = MilvusOption(options)

    milvusOption.vectorSearch shouldBe defined
    val config = milvusOption.vectorSearch.get
    config.metricType shouldBe "L2" // Default metric
    config.vectorColumn shouldBe "vector" // Default column name
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

  test("milvus.filter parses a trimmed expression through the Map entry") {
    val parsed = MilvusOption(
      Map(MilvusOption.MilvusFilter -> "  price >= 10 and active == true  ")
    )

    parsed.milvusFilter shouldBe Some(
      PlanParser.parse("price >= 10 and active == true")
    )
  }

  test("milvus.filter is absent when the option is not set") {
    MilvusOption(Map.empty[String, String]).milvusFilter shouldBe None
  }

  test("milvus.filter key is case insensitive") {
    val values = new java.util.HashMap[String, String]()
    values.put(MilvusOption.MilvusFilter.toUpperCase, "id in [1, 2]")

    val parsed = MilvusOption(new CaseInsensitiveStringMap(values))

    parsed.milvusFilter shouldBe Some(PlanParser.parse("id in [1, 2]"))
  }

  test("milvus.filter rejects an explicitly blank value") {
    Seq("", " \t ").foreach { value =>
      val error = intercept[IllegalArgumentException] {
        MilvusOption(Map(MilvusOption.MilvusFilter -> value))
      }
      error.getMessage should include(MilvusOption.MilvusFilter)
      error.getMessage should include("empty")
    }
  }

  test("milvus.filter reports the parser failure with the option key") {
    val error = intercept[IllegalArgumentException] {
      MilvusOption(Map(MilvusOption.MilvusFilter -> "id = 1"))
    }

    error.getMessage should include(MilvusOption.MilvusFilter)
    error.getMessage should include("Unsupported comparison")
    error.getCause shouldBe a[IllegalArgumentException]
  }

  test("milvus.filter rejects every vector.search option namespace") {
    Seq(
      MilvusOption.VectorSearchQueryVector,
      MilvusOption.VectorSearchFilter,
      "VECTOR.SEARCH.FUTURE"
    ).foreach { vectorKey =>
      val error = intercept[IllegalArgumentException] {
        MilvusOption(
          Map(
            MilvusOption.MilvusFilter -> "id > 0",
            vectorKey -> "configured"
          )
        )
      }
      error.getMessage should include(MilvusOption.MilvusFilter)
      error.getMessage should include(MilvusOption.VectorSearchFilter)
    }
  }

  test("milvus.filter expression survives Java serialization") {
    val expected = MilvusOption(
      Map(MilvusOption.MilvusFilter -> "id >= 7 or id is null")
    )
    val bytes = new ByteArrayOutputStream()
    val output = new ObjectOutputStream(bytes)
    try output.writeObject(expected)
    finally output.close()

    val input = new ObjectInputStream(
      new ByteArrayInputStream(bytes.toByteArray)
    )
    val restored =
      try input.readObject().asInstanceOf[MilvusOption]
      finally input.close()

    restored.milvusFilter shouldBe expected.milvusFilter
  }

  test("isSnapshotMode accepts explicit snapshot mode") {
    MilvusOption.isSnapshotMode(
      Map(MilvusOption.SnapshotMode -> "true")
    ) shouldBe true
  }

  test(
    "validateSnapshotModeOptions rejects explicit mode without segment hints"
  ) {
    Seq(
      Map(MilvusOption.SnapshotMode -> "true"),
      Map(
        MilvusOption.SnapshotMode -> "true",
        MilvusOption.SnapshotManifests -> " "
      ),
      Map(
        MilvusOption.SnapshotMode -> "true",
        MilvusOption.SnapshotV2Segments -> " "
      )
    ).foreach { options =>
      val err = intercept[IllegalArgumentException] {
        MilvusOption.validateSnapshotModeOptions(options)
      }
      err.getMessage should include(MilvusOption.SnapshotManifests)
      err.getMessage should include(MilvusOption.SnapshotV2Segments)
    }
  }

  test("validateSnapshotModeOptions accepts explicit mode with segment hints") {
    MilvusOption.validateSnapshotModeOptions(
      Map(
        MilvusOption.SnapshotMode -> "true",
        MilvusOption.SnapshotManifests -> "[]"
      )
    )
    MilvusOption.validateSnapshotModeOptions(
      Map(
        MilvusOption.SnapshotMode -> "true",
        MilvusOption.SnapshotV2Segments -> "[]"
      )
    )
  }

  test("isSnapshotMode accepts snapshot metadata options") {
    MilvusOption.isSnapshotMode(
      Map(MilvusOption.SnapshotManifests -> "[]")
    ) shouldBe true
    MilvusOption.isSnapshotMode(
      Map(MilvusOption.SnapshotV2Segments -> "[]")
    ) shouldBe true
  }

  test(
    "isSnapshotMode respects explicit false over snapshot metadata options"
  ) {
    val mapOptions = Map(
      MilvusOption.SnapshotMode -> "false",
      MilvusOption.SnapshotManifests -> "[]"
    )
    MilvusOption.isSnapshotMode(mapOptions) shouldBe false

    val javaOptions = new java.util.HashMap[String, String]()
    mapOptions.foreach { case (key, value) => javaOptions.put(key, value) }
    MilvusOption.isSnapshotMode(
      new CaseInsensitiveStringMap(javaOptions)
    ) shouldBe false
  }

  test("isSnapshotMode is consistent across option map types") {
    Seq(
      Map(MilvusOption.SnapshotMode.toUpperCase -> "true"),
      Map(MilvusOption.SnapshotManifests.toUpperCase -> "[]"),
      Map(MilvusOption.SnapshotV2Segments.toUpperCase -> "[]"),
      Map(MilvusOption.MilvusCollectionName -> "c")
    ).foreach { options =>
      val javaOptions = new java.util.HashMap[String, String]()
      options.foreach { case (key, value) => javaOptions.put(key, value) }
      MilvusOption.isSnapshotMode(options) shouldBe MilvusOption.isSnapshotMode(
        new CaseInsensitiveStringMap(javaOptions)
      )
    }
  }

  test("isSnapshotMode is false for normal client options") {
    MilvusOption.isSnapshotMode(
      Map(MilvusOption.MilvusCollectionName -> "c")
    ) shouldBe false
  }

  test("read booleans reject invalid values") {
    Seq(
      MilvusOption.SnapshotMode -> ((options: Map[String, String]) =>
        MilvusOption.isSnapshotMode(options)
      ),
      MilvusOption.ReadApplyDeletes -> ((options: Map[String, String]) =>
        MilvusOption.readApplyDeletes(options)
      ),
      MilvusOption.ReadVectorRaw -> ((options: Map[String, String]) =>
        MilvusOption.readVectorRaw(options)
      ),
      MilvusOption.ReadColumnar -> ((options: Map[String, String]) =>
        MilvusOption.readColumnar(options)
      )
    ).foreach { case (key, parse) =>
      Seq("truthy", "", "   ").foreach { value =>
        val error = intercept[IllegalArgumentException](
          parse(Map(key -> value))
        )
        error.getMessage should include(key)
      }
    }
  }

  test("partition and segment selectors parse strict distinct ids") {
    MilvusOption.selectedPartitionIds(
      Map(MilvusOption.MilvusPartitions -> "20, 21,20")
    ) shouldBe Seq(20L, 21L)
    MilvusOption.selectedSegmentIds(
      Map(MilvusOption.MilvusSegments -> "30,31")
    ) shouldBe Seq(30L, 31L)

    Seq(
      MilvusOption.MilvusPartitions -> "20,,21",
      MilvusOption.MilvusPartitions -> "   ",
      MilvusOption.MilvusSegments -> "",
      MilvusOption.MilvusSegments -> "30,nope",
      MilvusOption.MilvusSegments -> "30,-1"
    ).foreach { case (key, value) =>
      val error = intercept[IllegalArgumentException] {
        if (key == MilvusOption.MilvusPartitions)
          MilvusOption.selectedPartitionIds(Map(key -> value))
        else MilvusOption.selectedSegmentIds(Map(key -> value))
      }
      error.getMessage should include(key)
      if (value.trim.isEmpty) error.getMessage should include("empty")
      else error.getMessage should include(value)
    }
  }

  test("reader field ids are strict and case insensitive") {
    MilvusOption.readerFieldIds(
      Map(MilvusOption.ReaderFieldIDs.toUpperCase -> "101, 300")
    ) shouldBe Seq(101L, 300L)

    val error = intercept[IllegalArgumentException] {
      MilvusOption.readerFieldIds(
        Map(MilvusOption.ReaderFieldIDs -> "101,field")
      )
    }
    error.getMessage should include(MilvusOption.ReaderFieldIDs)
    error.getMessage should include("field")

    val blank = intercept[IllegalArgumentException] {
      MilvusOption.readerFieldIds(
        Map(MilvusOption.ReaderFieldIDs -> "   ")
      )
    }
    blank.getMessage should include(MilvusOption.ReaderFieldIDs)
  }

  test("resource limits parse once with strict case-insensitive keys") {
    val parsed = MilvusOption(
      Map(
        MilvusOption.ReadBatchMaxRows.toUpperCase -> " 2048 ",
        MilvusOption.ReadBatchMaxBytes.toUpperCase -> "16777216",
        MilvusOption.ReadArrowMaxBytes.toUpperCase -> "67108864",
        MilvusOption.WriteFileRollingBytes.toUpperCase -> "1073741824",
        StorageProperties.StorageType -> StorageProperties.StorageTypeLocal
      )
    )

    parsed.readLimits shouldBe ReadLimits(2048, 16777216L, 67108864L)
    parsed.writeFileRollingBytes shouldBe 1073741824L
    MilvusOption.writerProperties(parsed)(
      MilvusOption.NativeWriterFileRollingSize
    ) shouldBe "1073741824"
  }

  test("integer and long options reject blank, non-positive and overflow") {
    val invalid = Seq(
      MilvusOption.MilvusInsertMaxBatchSize -> "",
      MilvusOption.MilvusRetryCount -> "0",
      MilvusOption.MilvusRetryInterval -> "-1",
      MilvusOption.ReadBatchMaxRows -> "2147483648",
      MilvusOption.ReadBatchMaxRows -> "1.5",
      MilvusOption.ReadBatchMaxBytes -> "0",
      MilvusOption.ReadBatchMaxBytes ->
        (ReadLimits.MaxBatchBytes + 1L).toString,
      MilvusOption.ReadArrowMaxBytes -> "9223372036854775808",
      MilvusOption.WriteFileRollingBytes -> "-10"
    )

    invalid.foreach { case (key, value) =>
      val error = intercept[IllegalArgumentException] {
        MilvusOption(Map(key -> value))
      }
      error.getMessage should include(key)
      error.getMessage should include(s"'$value'")
    }
  }
}

/** Unit tests for MilvusS3Option
  */
class MilvusS3OptionTest extends AnyFunSuite with Matchers {

  test("Parse S3 options with default values") {
    import scala.collection.JavaConverters._
    import org.apache.spark.sql.util.CaseInsensitiveStringMap

    val options = new CaseInsensitiveStringMap(
      Map(
        MilvusOption.ReaderType -> "insert"
      ).asJava
    )

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

    val options = new CaseInsensitiveStringMap(
      Map(
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
      ).asJava
    )

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

    val options = new CaseInsensitiveStringMap(
      Map(
        MilvusOption.ReaderType -> "insert"
      ).asJava
    )

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

    val options = new CaseInsensitiveStringMap(
      Map(
        MilvusOption.ReaderType -> "insert",
        MilvusOption.S3FileSystemTypeName -> "s3a://",
        MilvusOption.S3BucketName -> "test-bucket",
        MilvusOption.S3RootPath -> "files"
      ).asJava
    )

    val s3Option = MilvusS3Option(options)

    // Test relative path
    val path1 = s3Option.getFilePath("insert_log/123/456")
    path1.toString should include("s3a://test-bucket/files/insert_log/123/456")

    // Test absolute S3 path (should not be modified)
    val path2 = s3Option.getFilePath("s3a://other-bucket/other-path")
    path2.toString shouldBe "s3a://other-bucket/other-path"
  }

  test("S3 booleans and positive integers use strict parsing") {
    import scala.collection.JavaConverters._

    Seq(
      MilvusOption.S3UseSSL -> "yes",
      MilvusOption.S3PathStyleAccess -> "",
      MilvusOption.S3MaxConnections -> "0",
      MilvusOption.S3PreloadPoolSize -> "2147483648"
    ).foreach { case (key, value) =>
      val error = intercept[IllegalArgumentException] {
        MilvusS3Option(
          new CaseInsensitiveStringMap(Map(key.toUpperCase -> value).asJava)
        )
      }
      error.getMessage should include(key)
      error.getMessage should include(s"'$value'")
    }
  }

  test("isBackupMode is true only when milvus.backup.dir is set") {
    MilvusOption.isBackupMode(Map.empty[String, String]) shouldBe false
    MilvusOption.isBackupMode(
      Map(MilvusOption.MilvusUri -> "http://localhost:19530")
    ) shouldBe false
    MilvusOption.isBackupMode(
      Map(MilvusOption.BackupDir -> "s3a://bucket/backup/b1")
    ) shouldBe true
    MilvusOption.isBackupMode(
      Map(MilvusOption.BackupDir -> "   ")
    ) shouldBe false
  }

  test("backupDir normalizes the s3:// alias to s3a://") {
    MilvusOption.backupDir(
      Map(MilvusOption.BackupDir -> "s3://bucket/backup/b1")
    ) shouldBe Some("s3a://bucket/backup/b1")
    MilvusOption.backupDir(
      Map(MilvusOption.BackupDir -> "s3a://bucket/backup/b1")
    ) shouldBe Some("s3a://bucket/backup/b1")
    MilvusOption.backupDir(
      Map(MilvusOption.BackupDir -> "/data/backup/b1")
    ) shouldBe Some("/data/backup/b1")
  }

  test("validateBackupModeOptions rejects backup combined with snapshot mode") {
    val both = Map(
      MilvusOption.BackupDir -> "s3a://bucket/backup/b1",
      MilvusOption.SnapshotV2Segments -> "[]"
    )
    intercept[IllegalArgumentException] {
      MilvusOption.validateBackupModeOptions(both)
    }
    // Backup mode alone is fine.
    MilvusOption.validateBackupModeOptions(
      Map(MilvusOption.BackupDir -> "s3a://bucket/backup/b1")
    )
  }

  test("empty-valued snapshot hint keys do not enable snapshot mode") {
    // Config templates keep optional keys with empty values; they must not trip
    // the snapshot/backup mutual-exclusion check.
    MilvusOption.isSnapshotMode(
      Map(MilvusOption.SnapshotManifests -> "")
    ) shouldBe false
    MilvusOption.isSnapshotMode(
      Map(MilvusOption.SnapshotV2Segments -> "   ")
    ) shouldBe false
    MilvusOption.validateBackupModeOptions(
      Map(
        MilvusOption.BackupDir -> "s3a://bucket/backup/b1",
        MilvusOption.SnapshotManifests -> ""
      )
    )
    MilvusOption.validateBackupModeOptions(
      Map(
        MilvusOption.BackupDir -> "s3a://bucket/backup/b1",
        MilvusOption.SnapshotV2Segments -> ""
      )
    )
  }

  // Validation itself lives in core.credential and is tested there
  // (StoragePropertiesTest). These check that MilvusOption's map reaches it,
  // which is what every reader and writer does at construction.

  test("a MilvusOption without a bucket name is rejected") {
    val err = intercept[IllegalArgumentException] {
      StorageProperties.from(
        MilvusOption(
          Map(MilvusOption.MilvusUri -> "http://localhost:19530")
        ).options
      )
    }
    assert(err.getMessage.contains(StorageProperties.BucketName))
  }

  test("a blank value counts as missing") {
    val err = intercept[IllegalArgumentException] {
      StorageProperties.from(
        MilvusOption(
          Map(
            MilvusOption.MilvusUri -> "http://localhost:19530",
            StorageProperties.BucketName -> "   "
          )
        ).options
      )
    }
    assert(err.getMessage.contains(StorageProperties.BucketName))
  }

  test("a missing endpoint is reported once the bucket is given") {
    val err = intercept[IllegalArgumentException] {
      StorageProperties.from(
        MilvusOption(
          Map(
            MilvusOption.MilvusUri -> "http://localhost:19530",
            StorageProperties.BucketName -> "b"
          )
        ).options
      )
    }
    assert(err.getMessage.contains(StorageProperties.Address))
  }
}
