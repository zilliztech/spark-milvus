package com.zilliz.milvus.storage.vector

import java.nio.ByteBuffer
import scala.collection.IndexedSeq

import com.zilliz.milvus.storage.schema.MetricType

/** One query's candidates as bytes: what a first-stage task sends on.
  *
  * A candidate is a place and a score — a global segment ordinal, an unsigned
  * row offset and a Float score — and a task sends at most k of them for each
  * of its queries, best first under [[Candidate.ranking]]. Packing them into
  * one array per query is what keeps the first stage's output at "queries × k ×
  * 12" bytes rather than that many JVM objects. The merge stage walks two
  * answers directly, or selects the best heads of a larger batch (design
  * document architecture/candidate-merge.html). Inputs and returned arrays are
  * read-only; an empty side or a single run may return an input array itself.
  *
  * All runs in one merge use the same global dictionary, whose ordinal order
  * preserves signed Long segment-id order without decoding during comparisons.
  * The bytes are big-endian, so what one executor writes another reads.
  */
object CandidateBytes {

  val Width: Int = 12
  val MaxRows: Long = 1L << 32
  val Empty: Array[Byte] = Array.emptyByteArray

  def count(packed: Array[Byte]): Int = {
    require(
      packed.length % Width == 0,
      s"Packed candidates come in $Width bytes each, not ${packed.length}"
    )
    packed.length / Width
  }

  def segmentId(
      packed: Array[Byte],
      at: Int,
      dictionary: CandidateDictionary
  ): Long = {
    val encoded =
      ByteBuffer.wrap(packed).getInt(at * Width).toLong & 0xffffffffL
    dictionary.segmentId(encoded)
  }

  def rowOffset(packed: Array[Byte], at: Int): Long =
    ByteBuffer.wrap(packed).getInt(at * Width + 4).toLong & 0xffffffffL

  def score(packed: Array[Byte], at: Int): Float =
    ByteBuffer.wrap(packed).getFloat(at * Width + 8)

  /** The candidates in the order they were packed, best first. */
  def foreach(
      packed: Array[Byte],
      dictionary: CandidateDictionary
  )(
      each: (Long, Long, Float) => Unit
  ): Unit = {
    val buffer = ByteBuffer.wrap(packed)
    val size = count(packed)
    var at = 0
    while (at < size) {
      val offset = at * Width
      val encoded = buffer.getInt(offset).toLong & 0xffffffffL
      each(
        dictionary.segmentId(encoded),
        buffer.getInt(offset + 4).toLong & 0xffffffffL,
        buffer.getFloat(offset + 8)
      )
      at += 1
    }
  }

  /** One query's run straight out of [[TopKMerger]]'s columns, which already
    * hold it best first, so this writes `size` candidates from `base` without
    * building a `Candidate` for any of them.
    */
  private[vector] def writeColumns(
      segments: Array[Long],
      offsets: Array[Long],
      scores: Array[Float],
      base: Int,
      size: Int,
      encoding: CandidateEncoding
  ): Array[Byte] = {
    require(size >= 0, s"$size candidates")
    if (size == 0) return Empty
    val packed =
      new Array[Byte](checkedLength(size.toLong * Width))
    val buffer = ByteBuffer.wrap(packed)
    var at = 0
    while (at < size) {
      val from = base + at
      encoding.write(
        buffer,
        at * Width,
        segments(from),
        offsets(from),
        scores(from)
      )
      at += 1
    }
    packed
  }

  /** The best k of two packed answers to the same query, best first.
    *
    * Both sides are sorted, so this walks each once. Merging in either order,
    * or merging three answers two at a time in any order, gives the same bytes:
    * the ranking is total, which is what the two tie-breakers in
    * [[Candidate.ranking]] are for.
    */
  def merge(
      left: Array[Byte],
      right: Array[Byte],
      k: Int,
      metric: MetricType
  ): Array[Byte] = {
    require(k > 0, s"topK must be positive: $k")
    val sign = if (metric.smallerIsBetter) 1 else -1
    val width = Width
    val leftSize = count(left)
    val rightSize = count(right)
    if (rightSize == 0) return if (leftSize <= k) left else left.take(k * width)
    if (leftSize == 0)
      return if (rightSize <= k) right else right.take(k * width)
    val size = math.min(k.toLong, leftSize.toLong + rightSize).toInt
    val packed = new Array[Byte](checkedLength(size.toLong * width))
    val leftBuffer = ByteBuffer.wrap(left)
    val rightBuffer = ByteBuffer.wrap(right)
    var leftAt = 0
    var rightAt = 0
    var at = 0
    while (at < size) {
      val takeLeft =
        if (leftAt >= leftSize) false
        else if (rightAt >= rightSize) true
        else
          CandidateHeads.compare(
            sign,
            leftBuffer,
            leftAt * width,
            rightBuffer,
            rightAt * width
          ) <= 0
      val source = if (takeLeft) left else right
      val from = (if (takeLeft) leftAt else rightAt) * width
      val to = at * width
      System.arraycopy(source, from, packed, to, width)
      if (takeLeft) leftAt += 1 else rightAt += 1
      at += 1
    }
    packed
  }

  /** The best k across sorted runs, without allocating intermediate answers. */
  def merge(
      inputs: IndexedSeq[Array[Byte]],
      k: Int,
      metric: MetricType
  ): Array[Byte] = {
    require(k > 0, s"topK must be positive: $k")
    if (inputs.isEmpty) return Empty
    if (inputs.size == 1) return merge(inputs.head, Empty, k, metric)
    if (inputs.size == 2) return merge(inputs(0), inputs(1), k, metric)
    val total = inputs.foldLeft(0L)((sum, run) => sum + count(run))
    val runs = inputs.filter(_.nonEmpty)
    if (runs.isEmpty) return Empty
    if (runs.size == 1) return merge(runs.head, Empty, k, metric)
    if (runs.size == 2) return merge(runs(0), runs(1), k, metric)

    val size = math.min(k.toLong, total).toInt
    val width = Width
    val packed = new Array[Byte](checkedLength(size.toLong * width))
    val buffers = runs.iterator.map(ByteBuffer.wrap).toArray
    val heads = new CandidateHeads(buffers, metric, (_, _) => width)
    var at = 0
    while (at < size) {
      System.arraycopy(runs(heads.run), heads.offset, packed, at * width, width)
      at += 1
      if (at < size) heads.advance()
    }
    packed
  }

  private[vector] def checkedLength(bytes: Long): Int = {
    require(
      bytes >= 0L && bytes <= Int.MaxValue.toLong,
      s"Packed candidate output does not fit one array: $bytes bytes"
    )
    bytes.toInt
  }
}
