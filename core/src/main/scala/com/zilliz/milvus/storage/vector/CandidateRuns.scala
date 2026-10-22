package com.zilliz.milvus.storage.vector

import scala.collection.mutable.ArrayBuffer

import com.zilliz.milvus.storage.schema.MetricType

/** A bounded batch of one query's sorted answers, compacted into its best k.
  *
  * Inputs contain at most k candidates and share a metric and layout. Their
  * arrays, including arrays returned by finish, are read-only: a single run may
  * be returned directly. The caller owns this mutable state exclusively.
  *
  * The byte target triggers compaction; it is not a heap ceiling. A single
  * oversized answer is retained intact, and old inputs coexist with a new
  * output during compaction. Only runs and pure parameters are serialized;
  * merge cursors and heaps are local to a compaction call.
  *
  * See design document architecture/candidate-merge.html sections 4-6.
  */
final class CandidateRuns(
    val k: Int,
    val metric: MetricType,
    val layout: CandidateRuns.Layout,
    val maxRuns: Int,
    val targetBytes: Long
) extends Serializable {
  require(k > 0, s"topK must be positive: $k")
  require(metric != null, "Candidate runs need a metric")
  require(layout != null, "Candidate runs need a layout")
  require(maxRuns >= 2, s"A merge batch needs at least two runs: $maxRuns")
  require(
    targetBytes > 0L,
    s"The merge byte target must be positive: $targetBytes"
  )

  private val runs = new ArrayBuffer[Array[Byte]](math.min(maxRuns, 16))
  private var bytes = 0L

  private[vector] def runCount: Int = runs.size
  private[vector] def retainedBytes: Long = bytes

  /** Append one sorted answer. Empty input leaves the state unchanged. */
  def append(incoming: Array[Byte]): CandidateRuns = {
    if (incoming.isEmpty) return this
    if (runs.size >= 2 && bytes + incoming.length.toLong > targetBytes)
      compact()
    val overTarget =
      runs.nonEmpty && bytes + incoming.length.toLong > targetBytes
    runs += incoming
    bytes += incoming.length.toLong
    if (overTarget || runs.size >= maxRuns) compact()
    this
  }

  /** Absorb another partial answer without changing it or its byte arrays.
    * Append each run separately so a spill merge does not concatenate batches.
    * A failed operation propagates; the owning task must abandon this state.
    */
  def absorb(other: CandidateRuns): CandidateRuns = {
    require(other ne this, "Candidate runs cannot absorb themselves")
    require(
      k == other.k && metric == other.metric && layout == other.layout &&
        maxRuns == other.maxRuns && targetBytes == other.targetBytes,
      "Candidate runs must share k, metric, layout and batch limits"
    )
    var at = 0
    while (at < other.runs.size) {
      append(other.runs(at))
      at += 1
    }
    this
  }

  /** Return the sorted top-k, repeatedly if requested. Further appends remain
    * valid and never modify an array returned by an earlier call.
    */
  def finish(): Array[Byte] = {
    compact()
    if (runs.isEmpty) CandidateBytes.Empty else runs.head
  }

  private def compact(): Unit = {
    if (runs.size < 2) return
    val result = layout match {
      case CandidateRuns.Plain =>
        CandidateBytes.merge(runs, k, metric)
      case CandidateRuns.Carried => CarriedCandidates.merge(runs, k, metric)
    }
    // Keep the previous inputs intact until the complete output exists.
    runs.clear()
    runs += result
    bytes = result.length.toLong
  }
}

object CandidateRuns {
  sealed trait Layout extends Product with Serializable
  case object Plain extends Layout
  case object Carried extends Layout
}
