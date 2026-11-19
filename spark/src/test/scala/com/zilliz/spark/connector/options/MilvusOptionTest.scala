package com.zilliz.spark.connector.options

import java.util.Collections

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.milvus.storage.read.plan.ReadLimits

/** Unit tests for MilvusOption parsing and validation
  */
class MilvusOptionTest extends AnyFunSuite with Matchers {

  test("Options map is preserved") {
    val options = Map(
      "custom.option" -> "custom_value"
    )

    val milvusOption = MilvusOption(options)

    milvusOption.options should contain key "custom.option"
    milvusOption.options("custom.option") shouldBe "custom_value"
  }

  // Spark passes unknown options through. A read that still sets the removed
  // option would otherwise return unfiltered rows without a warning.
  test("the removed milvus.filter option fails instead of being ignored") {
    for {
      key <- Seq("milvus.filter", "MILVUS.FILTER")
      value <- Seq("category == 1", "", " \t ")
    } {
      val error = intercept[IllegalArgumentException] {
        MilvusOption(Map(key -> value))
      }
      error.getMessage should include("'milvus.filter' was removed")
      error.getMessage should include("DataFrame where/filter or SQL WHERE")
      val direct = intercept[IllegalArgumentException] {
        MilvusOption.rejectRemovedOptions(
          new CaseInsensitiveStringMap(Collections.singletonMap(key, value))
        )
      }
      direct.getMessage shouldBe error.getMessage
    }
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
      MilvusOption.ReadVectorRaw -> ((options: Map[String, String]) =>
        MilvusOption.readVectorRaw(options)
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

  test("physical and live reads reject an explicit delete flag") {
    for {
      base <- Seq(
        Map.empty[String, String],
        Map(MilvusOption.ReadMode -> "physical")
      )
      value <- Seq("true", "false", "")
    } {
      val error = intercept[IllegalArgumentException](
        MilvusOption(base + ("MILVUS.READ.APPLY.DELETES" -> value))
      )
      error.getMessage shouldBe "Unsupported option 'milvus.read.apply.deletes'"
    }
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
