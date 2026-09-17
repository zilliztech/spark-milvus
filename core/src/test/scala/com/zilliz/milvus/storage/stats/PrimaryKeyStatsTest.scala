package com.zilliz.milvus.storage.stats

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.milvus.grpc.schema.DataType

/** The stats a written segment carries, checked against the file Milvus wrote
  * on UAT for the same keys.
  */
class PrimaryKeyStatsTest extends AnyFunSuite with Matchers {

  // _stats/bloom_filter.100/469093182140267755 of segment 469093182140257752
  // in spark_uat_v3: Milvus 3.0.1 flushed ids 0..2999 into it.
  private val milvusFile = new String(
    Files.readAllBytes(
      Paths.get("core/src/test/data/v3-bloom-filter-469093182140267755.json")
    ),
    StandardCharsets.UTF_8
  )

  test("the same 3000 keys produce the byte-identical file Milvus wrote") {
    val builder = new PrimaryKeyStats.Builder(100L, DataType.Int64)
    (0L until 3000L).foreach(builder.addLong)
    val stats = builder.build()
    stats.filter.k shouldBe 12
    stats.filter.numBits shouldBe 51200L
    stats.toJson shouldBe milvusFile
  }

  test("Milvus's file reads back and answers for its keys") {
    val stats = PrimaryKeyStats.fromJson(milvusFile)
    stats.fieldId shouldBe 100L
    stats.pkType shouldBe DataType.Int64
    stats.minPk shouldBe 0L
    stats.maxPk shouldBe 2999L
    (0L until 3000L).forall(stats.mightContainLong) shouldBe true
    // at 0.001 false positives, 10000 absent keys give a few hits at most
    (100000L until 110000L).count(stats.mightContainLong) should be < 50
  }

  test("VarChar keys hash as UTF-8 and keep string bounds") {
    val builder = new PrimaryKeyStats.Builder(101L, DataType.VarChar)
    Seq("row-7", "row-0", "row-99", "中文").foreach(builder.addString)
    val stats = builder.build()
    stats.minPk shouldBe "row-0"
    stats.maxPk shouldBe "中文"
    stats.mightContainString("row-99") shouldBe true
    stats.mightContainString("row-100") shouldBe false
    val back = PrimaryKeyStats.fromJson(stats.toJson)
    back.pkType shouldBe DataType.VarChar
    back.minPk shouldBe "row-0"
    back.mightContainString("中文") shouldBe true
    back.toJson shouldBe stats.toJson
  }

  // Review 749178e #12: Milvus compares VarChar keys as Go strings, i.e.
  // unsigned UTF-8 bytes; Java's String order is UTF-16 and disagrees above
  // the BMP.
  test("VarChar bounds follow Milvus's UTF-8 byte order") {
    val pua = new String(Character.toChars(0xe000))
    val emoji = new String(Character.toChars(0x1f600))
    val b = new PrimaryKeyStats.Builder(101L, DataType.VarChar)
    Seq(emoji, pua).foreach(b.addString)
    val s = b.build()
    s.minPk shouldBe pua
    s.maxPk shouldBe emoji

    def utf8(x: String) = x.getBytes(StandardCharsets.UTF_8)
    val keys = Seq("row-0", "中文", "Ａ", pua, emoji, "")
    val mixed = new PrimaryKeyStats.Builder(101L, DataType.VarChar)
    keys.foreach(mixed.addString)
    val m = mixed.build()
    keys.foreach { k =>
      withClue(s"key ${k.codePoints().toArray.mkString(",")}: ") {
        java.util.Arrays.compareUnsigned(
          utf8(m.minPk.toString),
          utf8(k)
        ) should be <= 0
        java.util.Arrays.compareUnsigned(
          utf8(k),
          utf8(m.maxPk.toString)
        ) should be <= 0
      }
    }
    m.maxPk shouldBe emoji
    m.minPk shouldBe ""
    val back = PrimaryKeyStats.fromJson(m.toJson)
    back.minPk shouldBe m.minPk
    back.maxPk shouldBe m.maxPk
  }

  test("reader accepts both a generated object and a compound array") {
    val first = new PrimaryKeyStats.Builder(100L, DataType.Int64)
    first.addLong(7L)
    val second = new PrimaryKeyStats.Builder(100L, DataType.Int64)
    second.addLong(9L)

    PrimaryKeyStats.fromBytes(first.build().toBytes) should have size 1
    val compound =
      s"[${first.build().toJson},${second.build().toJson}]"
        .getBytes(StandardCharsets.UTF_8)
    val parsed = PrimaryKeyStats.fromBytes(compound)
    parsed should have size 2
    parsed.head.mightContainLong(7L) shouldBe true
    parsed(1).mightContainLong(9L) shouldBe true
  }

  test("damaged or weakly typed JSON is rejected instead of defaulted") {
    val builder = new PrimaryKeyStats.Builder(100L, DataType.Int64)
    builder.addLong(7L)
    val valid = builder.build().toJson

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
      "[]".getBytes(StandardCharsets.UTF_8)
    )
  }

  test("a key of the wrong type and an empty segment are refused") {
    val builder = new PrimaryKeyStats.Builder(100L, DataType.Int64)
    an[IllegalArgumentException] should be thrownBy builder.addString("x")
    an[IllegalArgumentException] should be thrownBy builder.build()
    an[IllegalArgumentException] should be thrownBy
      new PrimaryKeyStats.Builder(100L, DataType.Float)
  }
}
