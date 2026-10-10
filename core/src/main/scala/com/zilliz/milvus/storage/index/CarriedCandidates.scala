package com.zilliz.milvus.storage.index

import java.lang.{Double => JavaDouble, Long => JavaLong}
import java.nio.ByteBuffer

import com.zilliz.milvus.storage.schema.MetricType

/** One query's candidates with the row each one carries, as bytes: what a
  * first-stage task over a DataFrame input sends on
  * (docs/design/architecture/dataframe-api.html section 4).
  *
  * A DataFrame input has no table to take a row from after the merge, so a
  * candidate brings its row along: the three fields of [[CandidateBytes]] --
  * where the row is, which for this input is its base partition and its
  * position there, and its score -- then the row's length and the row's bytes,
  * which nothing here reads. A task sends at most k for each of its queries,
  * best first under [[Candidate.ranking]], so the merge walks two answers as it
  * does for [[CandidateBytes]]. Big-endian, so what one executor writes another
  * reads.
  */
object CarriedCandidates {

  /** What precedes a row: its place, its score and its length. */
  private val Header: Int = CandidateBytes.Width + 4

  /** These candidates, sorted best first, each followed by its row. */
  def of(
      rows: Seq[(Candidate, Array[Byte])],
      metric: MetricType
  ): Array[Byte] = {
    val sorted = rows.sortBy(_._1)(Candidate.ranking(metric))
    val buffer = ByteBuffer.allocate(sorted.map(Header + _._2.length).sum)
    sorted.foreach { case (candidate, row) =>
      buffer
        .putLong(candidate.segmentId)
        .putLong(candidate.rowOffset)
        .putDouble(candidate.score)
        .putInt(row.length)
        .put(row)
    }
    buffer.array()
  }

  /** The candidates in the order they were packed, best first: each one's place
    * and score, and where its row lies in `packed` and how long it is.
    */
  def foreach(packed: Array[Byte])(
      each: (Long, Long, Double, Int, Int) => Unit
  ): Unit = {
    val buffer = ByteBuffer.wrap(packed)
    var at = 0
    while (at < packed.length) {
      val length = buffer.getInt(at + CandidateBytes.Width)
      each(
        buffer.getLong(at),
        buffer.getLong(at + 8),
        buffer.getDouble(at + 16),
        at + Header,
        length
      )
      at += Header + length
    }
  }

  /** The best `k` of two answers, each best first, by one walk over both. Two
    * answers never hold the same place: a base row is in one partition, and a
    * query in one group.
    */
  def merge(
      left: Array[Byte],
      right: Array[Byte],
      k: Int,
      metric: MetricType
  ): Array[Byte] = {
    require(k > 0, s"topK must be positive: $k")
    val better = if (metric.smallerIsBetter) 1 else -1
    val leftBuffer = ByteBuffer.wrap(left)
    val rightBuffer = ByteBuffer.wrap(right)
    def length(buffer: ByteBuffer, at: Int): Int =
      Header + buffer.getInt(at + CandidateBytes.Width)
    // Negative when the left candidate goes first.
    def compare(l: Int, r: Int): Int = {
      val byScore = JavaDouble.compare(
        leftBuffer.getDouble(l + 16),
        rightBuffer.getDouble(r + 16)
      ) * better
      if (byScore != 0) byScore
      else {
        val bySegment =
          JavaLong.compare(leftBuffer.getLong(l), rightBuffer.getLong(r))
        if (bySegment != 0) bySegment
        else
          JavaLong.compare(
            leftBuffer.getLong(l + 8),
            rightBuffer.getLong(r + 8)
          )
      }
    }
    // The output's size is known once the k are chosen, so they are chosen
    // first, as (side, offset, length), and copied after.
    val chosen = new Array[Long](k)
    var count = 0
    var l = 0
    var r = 0
    var bytes = 0
    while (count < k && (l < left.length || r < right.length)) {
      val fromLeft =
        r >= right.length || (l < left.length && compare(l, r) <= 0)
      if (fromLeft) {
        chosen(count) = l.toLong
        bytes += length(leftBuffer, l)
        l += length(leftBuffer, l)
      } else {
        chosen(count) = -(r.toLong + 1L)
        bytes += length(rightBuffer, r)
        r += length(rightBuffer, r)
      }
      count += 1
    }
    val out = new Array[Byte](bytes)
    var written = 0
    var at = 0
    while (at < count) {
      val (source, buffer, from) =
        if (chosen(at) >= 0L) (left, leftBuffer, chosen(at).toInt)
        else (right, rightBuffer, (-chosen(at) - 1L).toInt)
      val size = length(buffer, from)
      System.arraycopy(source, from, out, written, size)
      written += size
      at += 1
    }
    out
  }
}
