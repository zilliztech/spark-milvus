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

  def mightContain(hash: Long): Boolean =
    BlockedBloomFilter.allBits(hash, k, numBlocks) { (word, mask) =>
      (blocks(word) & mask) != 0
    }

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

  /** Materializes decoded words without retaining caller-owned mutable memory.
    */
  def fromWords(k: Int, words: Array[Int]): BlockedBloomFilter = {
    require(
      k >= 2,
      s"blocked bloom filter hash count must be at least 2, got $k"
    )
    require(
      words.nonEmpty && words.length % WordsPerBlock == 0,
      "blocked bloom filter must contain complete 512-bit blocks"
    )
    new BlockedBloomFilter(k, words.clone())
  }

  /** Visits the bit locations for one hash, stopping when a bit is rejected.
    * The query path tests these bits; a builder sets the same locations.
    */
  def allBits(hash: Long, k: Int, numBlocks: Int)(
      accept: (Int, Int) => Boolean
  ): Boolean = {
    var h1 = (hash >>> 32).toInt
    var h2 = hash.toInt
    val base = (((h2 & 0xffffffffL) * numBlocks) >>> 32).toInt * WordsPerBlock
    var i = 1
    while (i < k) {
      h1 = h1 + h2
      h2 = h2 + i
      val word = base + ((h1 >>> 5) & (WordsPerBlock - 1))
      if (!accept(word, 1 << (h1 & 31))) return false
      i += 1
    }
    true
  }
}
