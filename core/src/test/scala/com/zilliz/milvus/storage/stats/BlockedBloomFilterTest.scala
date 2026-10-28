package com.zilliz.milvus.storage.stats

import java.util.Base64

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class BlockedBloomFilterTest extends AnyFunSuite with Matchers {
  test("an empty fixed block proves a miss") {
    val empty =
      Base64.getUrlEncoder.withoutPadding().encodeToString(new Array[Byte](64))
    val filter = BlockedBloomFilter.fromJson(2, Seq(empty))
    filter.numBits shouldBe 512L
    filter.mightContain(42L) shouldBe false
  }

  test("a stored block decodes little-endian words and answers its set bit") {
    val bytes = new Array[Byte](64)
    bytes(0) = 2 // hash h1=1, h2=0 with k=2 tests bit 1 of the first word.
    val encoded = Base64.getUrlEncoder.withoutPadding().encodeToString(bytes)
    val filter = BlockedBloomFilter.fromJson(2, Seq(encoded))
    filter.blockBytes(0) shouldBe bytes
    filter.mightContain(0x0000000100000000L) shouldBe true
    filter.mightContain(0x0000000200000000L) shouldBe false
  }

  test("missing and truncated encoded blocks are rejected") {
    an[IllegalArgumentException] should be thrownBy BlockedBloomFilter.fromJson(
      2,
      Seq.empty
    )
    an[IllegalArgumentException] should be thrownBy BlockedBloomFilter.fromJson(
      2,
      Seq("AA")
    )
  }
}
