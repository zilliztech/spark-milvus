package com.zilliz.milvus.storage.stats

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.io.ReadFixtures
import io.milvus.grpc.schema.DataType

class PrimaryKeyStatsTest extends AnyFunSuite with Matchers {
  private val fixtures = Files.createTempDirectory("primary-key-read-fixtures")
  ReadFixtures.copyTo("stats/primary-key", fixtures)

  private def bytes(name: String): Array[Byte] =
    Files.readAllBytes(fixtures.resolve(s"$name.json"))
  private def json(name: String): String = new String(bytes(name), UTF_8)

  test("Milvus's file reads back and answers for its keys") {
    val stats = PrimaryKeyStats
      .fromBytes(
        Files.readAllBytes(
          Paths.get(
            "core/src/test/data/v3-bloom-filter-469093182140267755.json"
          )
        )
      )
      .head
    stats.fieldId shouldBe 100L
    stats.pkType shouldBe DataType.Int64
    stats.minPk shouldBe 0L
    stats.maxPk shouldBe 2999L
    (0L until 3000L).forall(stats.mightContainLong) shouldBe true
    (100000L until 110000L).count(stats.mightContainLong) should be < 50
  }

  test("stored VarChar keys hash as UTF-8 and keep string bounds") {
    val stats = PrimaryKeyStats.fromJson(json("varchar"))
    stats.pkType shouldBe DataType.VarChar
    stats.minPk shouldBe "row-0"
    stats.maxPk shouldBe "中文"
    stats.mightContainString("row-99") shouldBe true
    stats.mightContainString("row-100") shouldBe false
    stats.mightContainString("中文") shouldBe true
  }

  test("stored VarChar bounds follow Milvus's UTF-8 byte order") {
    val stats = PrimaryKeyStats.fromJson(json("varchar-unicode"))
    val emoji = new String(Character.toChars(0x1f600))
    stats.minPk shouldBe ""
    stats.maxPk shouldBe emoji
    Seq("row-0", "中文", "Ａ", "\ue000", emoji, "").foreach { key =>
      stats.mightContainString(key) shouldBe true
      PrimaryKeyStats.compareVarChar(stats.minPk.toString, key) should be <= 0
      PrimaryKeyStats.compareVarChar(key, stats.maxPk.toString) should be <= 0
    }
  }

  test("reader accepts a fixed single object and a compound array") {
    PrimaryKeyStats.fromBytes(bytes("long-7")) should have size 1
    val parsed = PrimaryKeyStats.fromBytes(bytes("compound"))
    parsed should have size 2
    parsed.head.mightContainLong(7L) shouldBe true
    parsed(1).mightContainLong(9L) shouldBe true
  }

  test("damaged or weakly typed JSON is rejected instead of defaulted") {
    val valid = json("long-7")
    an[IllegalArgumentException] should be thrownBy PrimaryKeyStats.fromJson(
      valid.replace("\"fieldID\":100", "\"fieldID\":\"100\"")
    )
    an[IllegalArgumentException] should be thrownBy PrimaryKeyStats.fromJson(
      valid.replace("\"minPk\":7", "\"minPk\":\"7\"")
    )
    an[IllegalArgumentException] should be thrownBy PrimaryKeyStats.fromJson(
      valid.replace("\"maxPk\":7,", "")
    )
    an[IllegalArgumentException] should be thrownBy PrimaryKeyStats.fromBytes(
      "[]".getBytes(UTF_8)
    )
  }
}
