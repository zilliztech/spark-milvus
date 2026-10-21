package com.zilliz.milvus.storage.write.stats

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.stats.{BlockedBloomFilter, PrimaryKeyStats}
import io.milvus.grpc.schema.DataType

/** The write side consumes only public read-format APIs and never hands a
  * reader storage that its next add operation can mutate.
  */
class StatsIsolationTest extends AnyFunSuite with Matchers {
  test("a decoded filter owns its words and exposes copied block bytes") {
    val words = new Array[Int](BlockedBloomFilter.WordsPerBlock)
    val filter = BlockedBloomFilter.fromWords(2, words)
    val hash = 0x0000000100000000L
    filter.mightContain(hash) shouldBe false
    java.util.Arrays.fill(words, -1)
    filter.mightContain(hash) shouldBe false
    val exposed = filter.blockBytes(0)
    java.util.Arrays.fill(exposed, (-1).toByte)
    filter.mightContain(hash) shouldBe false
    filter.blockBytes(0).forall(_ == 0.toByte) shouldBe true
  }

  test("building again cannot mutate a filter already handed to a reader") {
    val builder = BlockedBloomFilterBuilder.sized(100L)
    val empty = builder.build()
    val hash = 0x0000000100000000L
    builder.add(hash)
    val populated = builder.build()
    empty.mightContain(hash) shouldBe false
    populated.mightContain(hash) shouldBe true
    val bytes = BlockedBloomFilterEncoder.toJson(populated)
    (1L to 1000L).foreach(builder.add)
    BlockedBloomFilterEncoder.toJson(populated) shouldBe bytes
  }

  test(
    "primary-key stats retain their bounds and bytes when the builder grows"
  ) {
    val builder = new PrimaryKeyStatsBuilder(100L, DataType.Int64)
    builder.addLong(7L)
    val first = builder.build()
    val serialized = PrimaryKeyStatsEncoder.toBytes(first)
    builder.addLong(9L)
    val second = builder.build()
    first.keys shouldBe 1L
    first.minPk shouldBe 7L
    first.maxPk shouldBe 7L
    PrimaryKeyStatsEncoder.toBytes(first).toSeq shouldBe serialized.toSeq
    second.keys shouldBe 2L
    second.maxPk shouldBe 9L
    PrimaryKeyStats
      .fromBytes(serialized)
      .head
      .mightContainLong(7L) shouldBe true
  }
}
