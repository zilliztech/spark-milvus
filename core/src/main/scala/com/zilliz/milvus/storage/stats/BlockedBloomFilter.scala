package com.zilliz.milvus.storage.stats

import java.nio.{ByteBuffer, ByteOrder}
import java.util.Base64

/** The blocked bloom filter Milvus keeps for a segment's primary keys: a port
  * of `github.com/greatroar/blobloom` (the `milvus-io/blobloom` fork adds only
  * the JSON form), bit for bit, so a filter built here from a segment's keys is
  * byte-identical to the one Milvus would have written.
  *
  * The filter is `nblocks` blocks of 512 bits. A key contributes one 64-bit
  * hash (xxh3, computed by the caller): the low 32 bits pick the block, then `k
  * \- 1` rounds of enhanced double hashing on the two halves set one bit each
  * inside that block. Sizing follows blobloom's `Optimize` for a capacity and a
  * false-positive rate, Milvus's default rate being 0.001.
  *
  * JSON: `{"k":K,"b":["<base64url of 64 bytes>", ...]}`, each block its 16
  * little-endian 32-bit words.
  */
final class BlockedBloomFilter private (
    val k: Int,
    private val blocks: Array[Int]
) {
  import BlockedBloomFilter._

  def numBlocks: Int = blocks.length / WordsPerBlock

  def numBits: Long = numBlocks.toLong * BlockBits

  def add(hash: Long): Unit = {
    var h1 = (hash >>> 32).toInt
    var h2 = hash.toInt
    val base = blockIndex(h2) * WordsPerBlock
    var i = 1
    while (i < k) {
      h1 = h1 + h2
      h2 = h2 + i
      val word = base + ((h1 >>> 5) & (WordsPerBlock - 1))
      blocks(word) |= 1 << (h1 & 31)
      i += 1
    }
  }

  def mightContain(hash: Long): Boolean = {
    var h1 = (hash >>> 32).toInt
    var h2 = hash.toInt
    val base = blockIndex(h2) * WordsPerBlock
    var i = 1
    while (i < k) {
      h1 = h1 + h2
      h2 = h2 + i
      val word = base + ((h1 >>> 5) & (WordsPerBlock - 1))
      if ((blocks(word) & (1 << (h1 & 31))) == 0) return false
      i += 1
    }
    true
  }

  /** blobloom's `reducerange`: a 32-bit value scaled into `[0, nblocks)`. */
  private def blockIndex(h2: Int): Int =
    (((h2 & 0xffffffffL) * numBlocks) >>> 32).toInt

  /** The block, as the 64 bytes the JSON form encodes. */
  def blockBytes(index: Int): Array[Byte] = {
    val buf = ByteBuffer.allocate(BlockBits / 8).order(ByteOrder.LITTLE_ENDIAN)
    var w = 0
    while (w < WordsPerBlock) {
      buf.putInt(blocks(index * WordsPerBlock + w))
      w += 1
    }
    buf.array()
  }

  def toJson: String = {
    val sb = new StringBuilder
    sb.append("{\"k\":").append(k).append(",\"b\":[")
    var b = 0
    while (b < numBlocks) {
      if (b > 0) sb.append(',')
      sb.append('"')
        .append(Base64.getUrlEncoder.encodeToString(blockBytes(b)))
        .append('"')
      b += 1
    }
    sb.append("]}")
    sb.toString
  }

  override def equals(other: Any): Boolean = other match {
    case that: BlockedBloomFilter =>
      k == that.k && java.util.Arrays.equals(blocks, that.blocks)
    case _ => false
  }

  override def hashCode(): Int = 31 * k + java.util.Arrays.hashCode(blocks)
}

object BlockedBloomFilter {
  val BlockBits: Int = 512
  val WordsPerBlock: Int = BlockBits / 32

  /** Milvus's `common.maxBloomFalsePositive` default. */
  val MilvusFalsePositiveRate: Double = 0.001

  /** An empty filter sized for `capacity` keys at `fpRate`, as blobloom's
    * `NewOptimized` sizes it.
    */
  def sized(
      capacity: Long,
      fpRate: Double = MilvusFalsePositiveRate
  ): BlockedBloomFilter = {
    val (nbits, k) = optimize(capacity, fpRate)
    new BlockedBloomFilter(
      k,
      new Array[Int]((nbits / BlockBits).toInt * WordsPerBlock)
    )
  }

  /** A filter from its JSON form. */
  def fromJson(k: Int, blocksBase64: Seq[String]): BlockedBloomFilter = {
    require(
      k >= 2,
      s"blocked bloom filter hash count must be at least 2, got $k"
    )
    require(blocksBase64.nonEmpty, "blocked bloom filter must contain a block")
    val words = new Array[Int](blocksBase64.size * WordsPerBlock)
    blocksBase64.zipWithIndex.foreach { case (encoded, b) =>
      val bytes = Base64.getUrlDecoder.decode(encoded)
      require(
        bytes.length == BlockBits / 8,
        s"block $b is ${bytes.length} bytes, not ${BlockBits / 8}"
      )
      val buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
      var w = 0
      while (w < WordsPerBlock) {
        words(b * WordsPerBlock + w) = buf.getInt
        w += 1
      }
    }
    new BlockedBloomFilter(k, words)
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
