package com.zilliz.spark.connector.options

import com.zilliz.milvus.storage.index.MachineResources

/** The exact scan's base block, chosen for the machine a task runs on.
  *
  * One brute-force call hands Knowhere one block of base vectors, and every
  * query of the call scans the whole block; the block stays in L3 only while
  * the blocks of all concurrent tasks fit there together. So the block is the
  * machine's L3, times the share of the host this process may use, times a
  * safety factor, divided by the tasks that share the executor — clamped to [1
  * MiB, 32 MiB] and rounded down to a power of two. Below 1 MiB the per-call
  * overhead dominates; above 32 MiB one block alone leaves L3
  * (docs/design/architecture/search-resources.html section 3.2).
  */
object SearchResources {

  final case class Choice(bytes: Long, reason: String)

  val MinBatchBytes: Long = 1L << 20
  val MaxBatchBytes: Long = 32L << 20

  /** What a machine that cannot be read gets: good from 4 to 64 concurrent
    * tasks on the measured hardware, four times better than 32 MiB.
    */
  val FallbackBatchBytes: Long = 4L << 20

  /** The share of the usable L3 the blocks may take; the rest is for query
    * groups, output buffers and neighbours.
    */
  val CacheShare: Double = 0.6

  /** The share when the CPU quota is unknown: assume the cache is split with an
    * unknown neighbour.
    */
  val CacheShareUnknownQuota: Double = 0.3

  /** @param configured
    *   the value of `milvus.read.batch.max.bytes` when the call set it, which
    *   is then used as it is
    * @param slots
    *   the tasks that run at once on this executor
    */
  def exactScanBatch(
      machine: MachineResources,
      slots: Int,
      configured: Option[Long]
  ): Choice = configured match {
    case Some(bytes) =>
      Choice(bytes, s"$bytes (option ${MilvusOption.ReadBatchMaxBytes})")
    case None =>
      machine.l3Bytes match {
        case None =>
          Choice(
            FallbackBatchBytes,
            s"L3 unknown -> ${mib(FallbackBatchBytes)} (default)"
          )
        case Some(l3) =>
          val tasks = math.max(1, slots)
          val (quota, share) = machine.cpuQuota match {
            case Some(q) => (q, CacheShare)
            case None    => (1.0, CacheShareUnknownQuota)
          }
          val usable = l3 * quota * share
          val raw = (usable / tasks).toLong
          val bytes = powerOfTwoFloor(
            math.min(MaxBatchBytes, math.max(MinBatchBytes, raw))
          )
          val quotaText = machine.hostCpus match {
            case Some(host) => s"${machine.availableCpus}/$host"
            case None       => s"${machine.availableCpus}/unknown"
          }
          Choice(
            bytes,
            s"L3=${mib(l3)} quota=$quotaText slots=$tasks -> ${mib(bytes)} (auto)"
          )
      }
  }

  private def powerOfTwoFloor(value: Long): Long =
    java.lang.Long.highestOneBit(math.max(1L, value))

  private def mib(bytes: Long): String =
    if (bytes % (1L << 20) == 0) s"${bytes >> 20}MiB" else s"${bytes}B"
}
