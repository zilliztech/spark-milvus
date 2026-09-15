package com.zilliz.milvus.storage.stats

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class BlockedBloomFilterTest extends AnyFunSuite with Matchers {

  test(
    "sizing follows blobloom: 3000 keys at 0.001 is 100 blocks and 12 hashes"
  ) {
    BlockedBloomFilter.optimize(3000L, 0.001) shouldBe ((51200L, 12))
    // a small capacity still gets one whole block; 17 bits per key at 0.001
    val (bits, k) = BlockedBloomFilter.optimize(1L, 0.001)
    bits shouldBe 512L
    k should be >= 2
    BlockedBloomFilter.optimize(100000L, 0.001)._1 shouldBe 1700352L
  }

  test("what was added is found; the empty filter finds nothing") {
    val filter = BlockedBloomFilter.sized(1000L)
    filter.mightContain(42L) shouldBe false
    (1L to 1000L).foreach(i => filter.add(i * 0x9e3779b97f4a7c15L))
    (1L to 1000L).forall(i =>
      filter.mightContain(i * 0x9e3779b97f4a7c15L)
    ) shouldBe true
  }

  test(
    "the JSON form round-trips and encodes a block as 64 little-endian bytes"
  ) {
    val filter = BlockedBloomFilter.sized(10L)
    filter.add(0x0000000100000000L) // h1 = 1, h2 = 0: block 0
    val json = filter.toJson
    json should startWith(s"{\"k\":${filter.k},\"b\":[\"")
    val back = BlockedBloomFilter.fromJson(filter.k, Seq(json.split("\"")(5)))
    back shouldBe filter
    back.mightContain(0x0000000100000000L) shouldBe true
  }
}
