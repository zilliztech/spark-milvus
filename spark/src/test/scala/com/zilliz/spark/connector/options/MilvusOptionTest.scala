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

  test("Parse extra columns configuration with canonical names") {
    val options = Map(
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
      MilvusOption.MilvusExtraColumns -> ""
    )

    val milvusOption = MilvusOption(options)

    milvusOption.extraColumns shouldBe empty
  }

  test("Parse fieldIDs configuration") {
    val options = Map(
      MilvusOption.ReaderFieldIDs -> "100,101,102"
    )

    val milvusOption = MilvusOption(options)

    milvusOption.fieldIDs shouldBe "100,101,102"
  }

  test("Options map is preserved") {
    val options = Map(
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
        MilvusOption.SnapshotPath -> " "
      )
    ).foreach { options =>
      val err = intercept[IllegalArgumentException] {
        MilvusOption.validateSnapshotModeOptions(options)
      }
      err.getMessage should include(MilvusOption.SnapshotManifests)
      err.getMessage should include(MilvusOption.SnapshotPath)
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
        MilvusOption.SnapshotPath -> "[]"
      )
    )
  }

  test("isSnapshotMode accepts snapshot metadata options") {
    MilvusOption.isSnapshotMode(
      Map(MilvusOption.SnapshotManifests -> "[]")
    ) shouldBe true
    MilvusOption.isSnapshotMode(
      Map(MilvusOption.SnapshotPath -> "[]")
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
      Map(MilvusOption.SnapshotPath.toUpperCase -> "[]"),
      Map.empty[String, String]
    ).foreach { options =>
      val javaOptions = new java.util.HashMap[String, String]()
      options.foreach { case (key, value) => javaOptions.put(key, value) }
      MilvusOption.isSnapshotMode(options) shouldBe MilvusOption.isSnapshotMode(
        new CaseInsensitiveStringMap(javaOptions)
      )
    }
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
        StorageProperties.StorageType -> StorageProperties.StorageTypeLocal
      )
    )

    parsed.readLimits shouldBe ReadLimits(2048, 16777216L, 67108864L)
  }

  test("integer and long options reject blank, non-positive and overflow") {
    val invalid = Seq(
      MilvusOption.ReadBatchMaxRows -> "2147483648",
      MilvusOption.ReadBatchMaxRows -> "1.5",
      MilvusOption.ReadBatchMaxBytes -> "0",
      MilvusOption.ReadBatchMaxBytes ->
        (ReadLimits.MaxBatchBytes + 1L).toString,
      MilvusOption.ReadArrowMaxBytes -> "9223372036854775808"
    )

    invalid.foreach { case (key, value) =>
      val error = intercept[IllegalArgumentException] {
        MilvusOption(Map(key -> value))
      }
      error.getMessage should include(key)
      error.getMessage should include(s"'$value'")
    }
  }

  test(
    "public options reject retired service, write and compatibility keys before I/O"
  ) {
    Seq(
      "milvus.uri",
      "milvus.database.name",
      "milvus.collection.name",
      "milvus.backup.dir",
      "milvus.snapshot.v2.segments",
      "milvus.writer.customPath",
      "milvus.write.file.rolling.bytes",
      "milvus.collection.id",
      "milvus.partition.id",
      "milvus.segment.id",
      "path",
      "type",
      "s3.accessKey",
      "fs.accessKeyId"
    ).foreach { key =>
      val error = intercept[IllegalArgumentException] {
        MilvusOption(
          Map(key.toUpperCase -> "", MilvusOption.SnapshotPath -> "fixed.json")
        )
      }
      error.getMessage.toLowerCase should include(key.toLowerCase)
      error.getMessage should include("V3")
    }
  }

  test(
    "read options retain effective storage aliases and canonical storage keys"
  ) {
    val values = Map(
      MilvusOption.S3Endpoint -> "store:9000",
      MilvusOption.S3BucketName -> "data",
      MilvusOption.S3PathStyleAccess -> "true",
      MilvusOption.FsBucketName -> "data",
      StorageProperties.AccessKeyId -> "example",
      StorageProperties.AccessKeyValue -> "example-secret"
    )
    val parsed = MilvusOption(values)
    values.foreach { case (key, value) =>
      OptionParsing.value(parsed.options, key) shouldBe Some(value)
    }
  }
}
