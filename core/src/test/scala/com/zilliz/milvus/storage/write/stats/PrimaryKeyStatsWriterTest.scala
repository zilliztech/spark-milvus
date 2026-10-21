package com.zilliz.milvus.storage.write.stats

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.stats.PrimaryKeyStats
import io.milvus.grpc.schema.DataType

/** The stats a written segment carries, checked against the file Milvus wrote
  * on UAT for the same keys.
  */
class PrimaryKeyStatsWriterTest extends AnyFunSuite with Matchers {

  // _stats/bloom_filter.100/469093182140267755 of segment 469093182140257752
  // in spark_uat_v3: Milvus 3.0.1 flushed ids 0..2999 into it.
  private val milvusFile = new String(
    Files.readAllBytes(
      Paths.get("core/src/test/data/v3-bloom-filter-469093182140267755.json")
    ),
    StandardCharsets.UTF_8
  )

  test("the same 3000 keys produce the byte-identical file Milvus wrote") {
    val builder = new PrimaryKeyStatsBuilder(100L, DataType.Int64)
    (0L until 3000L).foreach(builder.addLong)
    val stats = builder.build()
    stats.filter.k shouldBe 12
    stats.filter.numBits shouldBe 51200L
    PrimaryKeyStatsEncoder.toJson(stats) shouldBe milvusFile
  }

  test("VarChar keys hash as UTF-8 and keep string bounds") {
    val builder = new PrimaryKeyStatsBuilder(101L, DataType.VarChar)
    Seq("row-7", "row-0", "row-99", "中文").foreach(builder.addString)
    val stats = builder.build()
    stats.minPk shouldBe "row-0"
    stats.maxPk shouldBe "中文"
    stats.mightContainString("row-99") shouldBe true
    stats.mightContainString("row-100") shouldBe false
    val back = PrimaryKeyStats.fromJson(PrimaryKeyStatsEncoder.toJson(stats))
    back.pkType shouldBe DataType.VarChar
    back.minPk shouldBe "row-0"
    back.mightContainString("中文") shouldBe true
    PrimaryKeyStatsEncoder.toJson(back) shouldBe PrimaryKeyStatsEncoder.toJson(
      stats
    )
  }

  // Milvus compares VarChar keys as Go strings, i.e. unsigned UTF-8 bytes;
  // Java's String order is UTF-16 and disagrees above the BMP.
  test("VarChar bounds follow Milvus's UTF-8 byte order") {
    val pua = new String(Character.toChars(0xe000))
    val emoji = new String(Character.toChars(0x1f600))
    val b = new PrimaryKeyStatsBuilder(101L, DataType.VarChar)
    Seq(emoji, pua).foreach(b.addString)
    val s = b.build()
    s.minPk shouldBe pua
    s.maxPk shouldBe emoji

    def utf8(x: String) = x.getBytes(StandardCharsets.UTF_8)
    val keys = Seq("row-0", "中文", "Ａ", pua, emoji, "")
    val mixed = new PrimaryKeyStatsBuilder(101L, DataType.VarChar)
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
    val back = PrimaryKeyStats.fromJson(PrimaryKeyStatsEncoder.toJson(m))
    back.minPk shouldBe m.minPk
    back.maxPk shouldBe m.maxPk
  }

  test("a key of the wrong type and an empty segment are refused") {
    val builder = new PrimaryKeyStatsBuilder(100L, DataType.Int64)
    an[IllegalArgumentException] should be thrownBy builder.addString("x")
    an[IllegalArgumentException] should be thrownBy builder.build()
    an[IllegalArgumentException] should be thrownBy
      new PrimaryKeyStatsBuilder(100L, DataType.Float)
  }
}
