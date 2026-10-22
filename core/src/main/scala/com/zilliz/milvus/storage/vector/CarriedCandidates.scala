package com.zilliz.milvus.storage.vector

import java.nio.ByteBuffer
import java.util.Arrays
import scala.collection.IndexedSeq

import com.zilliz.milvus.storage.schema.MetricType

/** One query's candidates with the row each one carries, as bytes: what a
  * first-stage task over a DataFrame input sends on.
  *
  * A candidate holds a UInt32 base-partition ordinal, UInt32 physical row
  * offset and Float32 score, followed by its row's length and opaque bytes. The
  * partition ordinal is used directly, without a Milvus segment dictionary.
  * Runs are best first under [[Candidate.ranking]], with big-endian headers.
  * Binary and multiway merging select candidates before copying their complete
  * records. Inputs and returned arrays are read-only; a single run can be
  * returned without copying.
  */
object CarriedCandidates {

  /** What precedes a row: its place, its score and its length. */
  private val Header: Int = CandidateBytes.Width + 4

  /** Writes candidates already ranked best first, each followed by its row. The
    * caller supplies the order used by the merge; encoding preserves it without
    * sorting again.
    */
  def writeSorted(rows: Seq[(Candidate, Array[Byte])]): Array[Byte] = {
    val bytes = rows.foldLeft(0L) { case (sum, (_, row)) =>
      sum + Header.toLong + row.length
    }
    val buffer = ByteBuffer.allocate(CandidateBytes.checkedLength(bytes))
    rows.foreach { case (candidate, row) =>
      putRecord(buffer, candidate, row)
    }
    buffer.array()
  }

  /** The encoded size without collecting lengths or fetching row bytes. */
  def encodedSize(candidates: Seq[Candidate])(
      rowSize: Candidate => Int
  ): Long = {
    var bytes = 0L
    candidates.foreach { candidate =>
      bytes += Header.toLong + rowSize(candidate).toLong
    }
    bytes
  }

  /** Writes the supplied order into an exactly sized answer. The caller checks
    * [[encodedSize]] against its budget and the array limit before narrowing it
    * to `bytes`; candidates and associated rows must stay unchanged.
    */
  def writeSorted(candidates: Seq[Candidate], bytes: Int)(
      rowBytes: Candidate => Array[Byte]
  ): Array[Byte] = {
    val buffer = ByteBuffer.allocate(bytes)
    candidates.foreach { candidate =>
      putRecord(buffer, candidate, rowBytes(candidate))
    }
    buffer.array()
  }

  private def putRecord(
      buffer: ByteBuffer,
      candidate: Candidate,
      row: Array[Byte]
  ): Unit = {
    require(
      candidate.segmentId >= 0L && candidate.segmentId < CandidateBytes.MaxRows,
      s"Carried partition ${candidate.segmentId} is outside UInt32 addresses"
    )
    require(
      candidate.rowOffset >= 0L && candidate.rowOffset < CandidateBytes.MaxRows,
      s"Carried row ${candidate.rowOffset} is outside UInt32 addresses"
    )
    buffer
      .putInt(candidate.segmentId.toInt)
      .putInt(candidate.rowOffset.toInt)
      .putFloat(candidate.score)
      .putInt(row.length)
      .put(row)
    ()
  }

  /** The candidates in the order they were packed, best first: each one's place
    * and score, and where its row lies in `packed` and how long it is.
    */
  def foreach(packed: Array[Byte])(
      each: (Long, Long, Float, Int, Int) => Unit
  ): Unit = {
    val buffer = ByteBuffer.wrap(packed)
    var at = 0
    while (at < packed.length) {
      val size = recordSize(buffer, at)
      each(
        buffer.getInt(at).toLong & 0xffffffffL,
        buffer.getInt(at + 4).toLong & 0xffffffffL,
        buffer.getFloat(at + 8),
        at + Header,
        size - Header
      )
      at += size
    }
  }

  /** Maps each row's byte region when pulled, without retaining row views. The
    * caller keeps `packed` unchanged while consuming the iterator and performs
    * any eager checks it needs with [[foreach]] before handing it out.
    */
  def mapRows[A](packed: Array[Byte])(
      read: (Int, Int) => A
  ): Iterator[A] = new Iterator[A] {
    private val buffer = ByteBuffer.wrap(packed)
    private var at = 0

    override def hasNext: Boolean = at < packed.length

    override def next(): A = {
      if (!hasNext) throw new NoSuchElementException("end of carried answer")
      val length = buffer.getInt(at + CandidateBytes.Width)
      val from = at + Header
      at += Header + length
      read(from, length)
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
    if (left.isEmpty) return prefix(right, k)
    if (right.isEmpty) return prefix(left, k)
    val sign = if (metric.smallerIsBetter) 1 else -1
    val buffers = Array(ByteBuffer.wrap(left), ByteBuffer.wrap(right))
    val chosen = new ChosenRecords(buffers, k)
    var l = 0
    var r = 0
    var leftSize = recordSize(buffers(0), l)
    var rightSize = recordSize(buffers(1), r)
    while (chosen.size < k && (l < left.length || r < right.length)) {
      val fromLeft =
        r >= right.length || (l < left.length &&
          CandidateHeads.compare(
            sign,
            buffers(0),
            l,
            buffers(1),
            r
          ) <= 0)
      if (fromLeft) {
        chosen.append(0, l, leftSize)
        l += leftSize
        if (chosen.size < k && l < left.length)
          leftSize = recordSize(buffers(0), l)
      } else {
        chosen.append(1, r, rightSize)
        r += rightSize
        if (chosen.size < k && r < right.length)
          rightSize = recordSize(buffers(1), r)
      }
    }
    chosen.result()
  }

  /** The best k across sorted runs, selecting once before copying rows. */
  def merge(
      inputs: IndexedSeq[Array[Byte]],
      k: Int,
      metric: MetricType
  ): Array[Byte] = {
    require(k > 0, s"topK must be positive: $k")
    if (inputs.isEmpty) return CandidateBytes.Empty
    if (inputs.size == 1) return prefix(inputs.head, k)
    if (inputs.size == 2) return merge(inputs(0), inputs(1), k, metric)
    val runs = inputs.filter(_.nonEmpty)
    if (runs.isEmpty) return CandidateBytes.Empty
    if (runs.size == 1) return prefix(runs.head, k)
    if (runs.size == 2) return merge(runs(0), runs(1), k, metric)
    val buffers = runs.iterator.map(ByteBuffer.wrap).toArray
    val heads = new CandidateHeads(buffers, metric, recordSize)
    val chosen = new ChosenRecords(buffers, k)
    while (chosen.size < k && heads.nonEmpty) {
      chosen.append(heads.run, heads.offset, heads.recordSize)
      if (chosen.size < k) heads.advance()
    }
    chosen.result()
  }

  private def prefix(packed: Array[Byte], k: Int): Array[Byte] = {
    val buffer = ByteBuffer.wrap(packed)
    var at = 0
    var count = 0
    while (count < k && at < packed.length) {
      at += recordSize(buffer, at)
      count += 1
    }
    if (at == packed.length) packed else Arrays.copyOf(packed, at)
  }

  /** Validate a complete record before its header participates in ranking. */
  private def recordSize(buffer: ByteBuffer, at: Int): Int = {
    val remaining = buffer.limit().toLong - at
    require(
      at >= 0 && remaining >= Header,
      s"Incomplete carried candidate header at byte $at"
    )
    val rowLength = buffer.getInt(at + CandidateBytes.Width)
    require(rowLength >= 0, s"Negative carried row length: $rowLength")
    val size = Header.toLong + rowLength
    require(
      size <= remaining,
      s"Carried candidate at byte $at needs $size bytes, only $remaining remain"
    )
    size.toInt
  }

  /** Two primitive columns grow with actual winners, not the requested k. */
  private final class ChosenRecords(buffers: Array[ByteBuffer], k: Int) {
    private var runs = {
      // Each record needs at least Header bytes. Bound the initial allocation
      // without scanning rows, while avoiding growth for common small top-k.
      val limit = math.min(k, 128)
      var upperBound = 0L
      var at = 0
      while (at < buffers.length && upperBound < limit) {
        upperBound += buffers(at).limit().toLong / Header
        at += 1
      }
      new Array[Int](math.min(limit.toLong, upperBound).toInt)
    }
    private var offsets = new Array[Int](runs.length)
    private var count = 0
    private var bytes = 0L

    def size: Int = count

    def append(run: Int, offset: Int, width: Int): Unit = {
      val total = bytes + width.toLong
      CandidateBytes.checkedLength(total)
      if (count == runs.length) {
        val capacity = math.min(k.toLong, runs.length.toLong * 2L).toInt
        runs = Arrays.copyOf(runs, capacity)
        offsets = Arrays.copyOf(offsets, capacity)
      }
      runs(count) = run
      offsets(count) = offset
      count += 1
      bytes = total
    }

    def result(): Array[Byte] = {
      val out = new Array[Byte](CandidateBytes.checkedLength(bytes))
      var at = 0
      var written = 0
      while (at < count) {
        val buffer = buffers(runs(at))
        val offset = offsets(at)
        // Selection already validated this whole record; its input is read-only.
        val width = Header + buffer.getInt(offset + CandidateBytes.Width)
        System.arraycopy(buffer.array(), offset, out, written, width)
        written += width
        at += 1
      }
      out
    }
  }
}
