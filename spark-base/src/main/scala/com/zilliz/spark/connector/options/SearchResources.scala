package com.zilliz.spark.connector.options

import com.zilliz.milvus.storage.index.MachineResources

/** The two machine-bound sizes of a search, chosen for the machine they run on:
  * the exact scan's base block and the vectors a task keeps resident.
  *
  * One brute-force call hands Knowhere one block of base vectors, and every
  * query of the call scans the whole block; the block stays in L3 only while
  * the blocks of all concurrent tasks fit there together. So the block is the
  * machine's L3, times the share of the host this process may use, times a
  * safety factor, divided by the tasks that share the executor — clamped to [1
  * MiB, 32 MiB] and rounded down to a power of two. Below 1 MiB the per-call
  * overhead dominates; above 32 MiB one block alone leaves L3
  * (docs/design/architecture/search-resources.html section 3.2).
  *
  * A task that answers several query groups keeps its segment set in Arrow's
  * off-heap memory between groups. That memory is what the executor's limit
  * leaves after the heap and the JVM's own share, and half of that is for
  * vectors, the rest for index loads, shuffle and Arrow's other buffers; the
  * tasks of one executor divide it (section 3.3).
  */
object SearchResources {

  final case class Choice(bytes: Long, reason: String)

  /** What the JVM and Spark themselves take off the heap. */
  val JvmReserveBytes: Long = 1L << 30

  /** The share of the off-heap room the resident vectors may take. */
  val OffHeapShare: Double = 0.5

  /** The least a task plans against, however small the room: below this the
    * plan would cut segments into more sets than there are row groups to read,
    * and a task that overruns it streams anyway.
    */
  val MinVectorsBudgetBytes: Long = 64L << 20

  /** What an executor whose memory limit cannot be read plans against, per
    * executor, as the option's fixed default used to be.
    */
  val FallbackVectorsBytes: Long = 2L << 30

  /** The bytes of vectors one task keeps resident.
    *
    * @param memoryLimitBytes
    *   what the executor may use in all: its cgroup limit or the machine's
    *   total in local mode, the container Spark asked for otherwise
    * @param heapBytes
    *   the executor JVM's maximum heap
    * @param slots
    *   the tasks that run at once on the executor
    * @param configured
    *   `milvus.search.vectors.max.bytes` when the call set it, which is per
    *   executor and is divided by the slots as it is
    */
  def vectorsBudget(
      memoryLimitBytes: Option[Long],
      heapBytes: Long,
      slots: Int,
      configured: Option[Long]
  ): Choice = {
    val tasks = math.max(1, slots)
    configured match {
      case Some(bytes) =>
        val perTask = math.max(1L, bytes / tasks)
        Choice(
          perTask,
          s"$bytes / $tasks slots -> ${mib(perTask)} (option ${MilvusOption.SearchVectorsMaxBytes})"
        )
      case None =>
        memoryLimitBytes match {
          case None =>
            val perTask = math.max(1L, FallbackVectorsBytes / tasks)
            Choice(
              perTask,
              s"memory limit unknown -> ${mib(FallbackVectorsBytes)} / $tasks slots = ${mib(perTask)} (default)"
            )
          case Some(limit) =>
            val offHeap = math.max(0L, limit - heapBytes - JvmReserveBytes)
            val raw = (offHeap * OffHeapShare / tasks).toLong
            val perTask = math.max(MinVectorsBudgetBytes, raw)
            val how = if (raw < MinVectorsBudgetBytes) "floor" else "auto"
            Choice(
              perTask,
              s"limit=${mib(limit)} heap=${mib(heapBytes)} offheap=${mib(
                  offHeap
                )} x$OffHeapShare / $tasks slots -> ${mib(perTask)} ($how)"
            )
        }
    }
  }

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
    if (bytes >= (1L << 30) && bytes % (1L << 30) == 0) s"${bytes >> 30}GiB"
    else if (bytes % (1L << 20) == 0) s"${bytes >> 20}MiB"
    else if (bytes >= (1L << 20)) f"${bytes / (1024.0 * 1024.0)}%.1fMiB"
    else s"${bytes}B"
}
