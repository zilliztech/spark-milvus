package com.zilliz.milvus.storage.index

import java.lang.{Double => JavaDouble, Long => JavaLong}
import java.nio.ByteBuffer
import java.util.Arrays

/** One query's candidates as bytes: what a first-stage task sends on.
  *
  * A candidate is a place and a score — segment id, row offset, the metric
  * score — and a task sends at most k of them for each of its queries, best
  * first under [[Candidate.ranking]]. Packing them into one array per query is
  * what keeps the first stage's output at "queries × k × Width" bytes rather
  * than that many JVM objects, and lets the merge stage join two tasks' answers
  * by walking both once (docs/design/architecture/vector-search.html sections
  * 2.1 and 2.6).
  *
  * The bytes are big-endian, so what one executor writes another reads.
  */
object CandidateBytes {

  /** Segment id, row offset, score: the three fields a candidate is. */
  val Width: Int = 8 + 8 + 8

  val Empty: Array[Byte] = Array.emptyByteArray

  def count(packed: Array[Byte]): Int = {
    require(
      packed.length % Width == 0,
      s"Packed candidates come in $Width bytes each, not ${packed.length}"
    )
    packed.length / Width
  }

  def segmentId(packed: Array[Byte], at: Int): Long =
    ByteBuffer.wrap(packed).getLong(at * Width)

  def rowOffset(packed: Array[Byte], at: Int): Long =
    ByteBuffer.wrap(packed).getLong(at * Width + 8)

  def score(packed: Array[Byte], at: Int): Double =
    ByteBuffer.wrap(packed).getDouble(at * Width + 16)

  /** The candidates in the order they were packed, best first. */
  def foreach(packed: Array[Byte])(
      each: (Long, Long, Double) => Unit
  ): Unit = {
    val buffer = ByteBuffer.wrap(packed)
    val size = count(packed)
    var at = 0
    while (at < size) {
      val offset = at * Width
      each(
        buffer.getLong(offset),
        buffer.getLong(offset + 8),
        buffer.getDouble(offset + 16)
      )
      at += 1
    }
  }

  /** These candidates, sorted best first and packed. For callers that hold
    * candidates as objects — a test, or a caller that built them some other
    * way; the search path packs straight out of [[TopKMerger]].
    */
  def of(candidates: Seq[Candidate], metric: String): Array[Byte] = {
    val sorted = candidates.toArray
    Arrays.sort(sorted, Candidate.ranking(metric))
    write(sorted, sorted.length)
  }

  /** `sorted` holds `size` candidates, best first already. */
  private[index] def write(sorted: Array[Candidate], size: Int): Array[Byte] = {
    require(
      size >= 0 && size <= sorted.length,
      s"$size candidates of an array of ${sorted.length}"
    )
    if (size == 0) return Empty
    val packed = new Array[Byte](size * Width)
    val buffer = ByteBuffer.wrap(packed)
    var at = 0
    while (at < size) {
      val candidate = sorted(at)
      val offset = at * Width
      buffer.putLong(offset, candidate.segmentId)
      buffer.putLong(offset + 8, candidate.rowOffset)
      buffer.putDouble(offset + 16, candidate.score)
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
      metric: String
  ): Array[Byte] = {
    require(k > 0, s"topK must be positive: $k")
    val sign = if (Candidate.smallerIsBetter(metric)) 1 else -1
    val leftSize = count(left)
    val rightSize = count(right)
    if (rightSize == 0) return if (leftSize <= k) left else left.take(k * Width)
    if (leftSize == 0)
      return if (rightSize <= k) right else right.take(k * Width)
    val size = math.min(k, leftSize + rightSize)
    val packed = new Array[Byte](size * Width)
    val target = ByteBuffer.wrap(packed)
    val leftBuffer = ByteBuffer.wrap(left)
    val rightBuffer = ByteBuffer.wrap(right)
    var leftAt = 0
    var rightAt = 0
    var at = 0
    while (at < size) {
      val takeLeft =
        if (leftAt >= leftSize) false
        else if (rightAt >= rightSize) true
        else better(sign, leftBuffer, leftAt, rightBuffer, rightAt) <= 0
      val source = if (takeLeft) leftBuffer else rightBuffer
      val from = (if (takeLeft) leftAt else rightAt) * Width
      val to = at * Width
      target.putLong(to, source.getLong(from))
      target.putLong(to + 8, source.getLong(from + 8))
      target.putDouble(to + 16, source.getDouble(from + 16))
      if (takeLeft) leftAt += 1 else rightAt += 1
      at += 1
    }
    packed
  }

  /** Negative when the left candidate ranks better. */
  private def better(
      sign: Int,
      left: ByteBuffer,
      leftAt: Int,
      right: ByteBuffer,
      rightAt: Int
  ): Int = {
    val leftOffset = leftAt * Width
    val rightOffset = rightAt * Width
    val score = JavaDouble.compare(
      left.getDouble(leftOffset + 16),
      right.getDouble(rightOffset + 16)
    ) * sign
    if (score != 0) score
    else {
      val segment = JavaLong.compare(
        left.getLong(leftOffset),
        right.getLong(rightOffset)
      )
      if (segment != 0) segment
      else
        JavaLong.compare(
          left.getLong(leftOffset + 8),
          right.getLong(rightOffset + 8)
        )
    }
  }
}
