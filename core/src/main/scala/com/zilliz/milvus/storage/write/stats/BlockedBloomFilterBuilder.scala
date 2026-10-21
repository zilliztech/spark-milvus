package com.zilliz.milvus.storage.write.stats

import com.zilliz.milvus.storage.stats.BlockedBloomFilter
import com.zilliz.milvus.storage.stats.BlockedBloomFilter.{
  BlockBits,
  WordsPerBlock
}

/** Mutable construction state; each build returns an independent read model. */
final class BlockedBloomFilterBuilder private (
    val k: Int,
    private val blocks: Array[Int]
) {
  def add(hash: Long): Unit = {
    BlockedBloomFilter.allBits(hash, k, blocks.length / WordsPerBlock) {
      (word, mask) =>
        blocks(word) |= mask
        true
    }
    ()
  }

  def build(): BlockedBloomFilter = BlockedBloomFilter.fromWords(k, blocks)
}

object BlockedBloomFilterBuilder {

  /** Milvus's `common.maxBloomFalsePositive` default. */
  val MilvusFalsePositiveRate: Double = 0.001

  /** An empty filter sized for `capacity` keys at `fpRate`, as blobloom's
    * `NewOptimized` sizes it.
    */
  def sized(
      capacity: Long,
      fpRate: Double = MilvusFalsePositiveRate
  ): BlockedBloomFilterBuilder = {
    val (nbits, k) = optimize(capacity, fpRate)
    new BlockedBloomFilterBuilder(
      k,
      new Array[Int]((nbits / BlockBits).toInt * WordsPerBlock)
    )
  }

  // blobloom's table of bits-per-key that reach a false-positive rate with
  // blocked filters, indexed by ceil(-log2(p) / ln 2).
  private val correctC: Array[Int] = Array(
    1, 1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16, 17, 18, 20, 21, 23, 25,
    26, 28, 30, 32, 35, 38, 40, 44, 48, 51, 58, 64, 74, 90
  )

  /** blobloom's `Optimize`: bits and hash count for a capacity and rate. */
  def optimize(capacity: Long, fpRate: Double): (Long, Int) = {
    require(
      fpRate > 0 && fpRate <= 1,
      s"false positive rate $fpRate must be in (0, 1]"
    )
    val n = if (capacity == 0) 1.0 else capacity.toDouble
    var c = math.ceil(-log2(fpRate) / math.log(2))
    c = if (c < correctC.length) correctC(c.toInt).toDouble else c * 3
    var nbits = (c * n).toLong
    if (nbits % BlockBits != 0) nbits += BlockBits - nbits % BlockBits
    c = nbits.toDouble / n
    val kf = c * math.log(2)
    if (kf < 1) return (nbits, 2) // New() raises fewer than 2 hashes to 2
    val lower = math.floor(kf)
    val upper = math.ceil(kf)
    if (lower == upper) return (nbits, math.max(lower.toInt, 2))
    // blobloom (optimize.go) names Floor(k) `ceilK` and Ceil(k) `floorK`
    // and then takes `floorK` when the floor's rate is the lower one, so
    // the hash count it picks is the ceiling in that case. Ported as it
    // behaves, which the byte-identical check against Milvus's own file
    // confirms.
    val fprUpper = fpRateOf(c, upper)
    val fprLower = fpRateOf(c, lower)
    val k = if (fprLower < fprUpper) upper else lower
    (nbits, math.max(k.toInt, 2))
  }

  private def log2(x: Double): Double = math.log(x) / math.log(2)

  /** blobloom's `fpRate`: the rate of a blocked filter with `c` bits per key
    * and `k` hashes, summing over the Poisson-distributed keys per block.
    */
  private def fpRateOf(c: Double, k: Double): Double = {
    val eps = 1e-9
    val mean = BlockBits / c
    val i = math.ceil(mean)
    var p = math.exp(logPoisson(mean, i) + logFprBlock(BlockBits / i, k))
    var j = i - 1
    var stop = false
    while (j > 0 && !stop) {
      val add = math.exp(logPoisson(mean, j) + logFprBlock(BlockBits / j, k))
      p += add
      if (add / p < eps) stop = true
      j -= 1
    }
    j = i + 1
    stop = false
    while (!stop) {
      val add = math.exp(logPoisson(mean, j) + logFprBlock(BlockBits / j, k))
      p += add
      if (add / p < eps) stop = true
      j += 1
    }
    p
  }

  private def logFprBlock(c: Double, k: Double): Double =
    k * math.log1p(-math.exp(-k / c))

  private def logPoisson(lambda: Double, k: Double): Double =
    k * math.log(lambda) - lambda - logGamma(k + 1)

  /** lgamma for the positive integers this needs: log((k-1)!) summed. */
  private def logGamma(x: Double): Double = {
    var sum = 0.0
    var j = 2
    while (j < x) { sum += math.log(j); j += 1 }
    sum
  }
}
