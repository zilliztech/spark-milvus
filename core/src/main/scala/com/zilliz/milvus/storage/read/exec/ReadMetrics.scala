package com.zilliz.milvus.storage.read.exec

/** What one segment read cost on the C/JVM crossing, as far as this side can
  * see it. Design: docs/design/architecture/storage-io.html section 5.
  *
  * @param jniCalls
  *   calls into the upstream milvus-storage binding by this reader, the opens
  *   included; the closes land after Spark's last read of the values
  * @param jniNanos
  *   wall time spent inside those calls
  * @param batches
  *   Arrow batches handed over
  * @param arrowBytes
  *   bytes of the buffers those batches carry, summed over their vectors
  * @param copies
  *   columns the C side materialized with `arrow::Concatenate` because they
  *   arrived sliced, as reported by the upstream batch reader
  * @param copiedBytes
  *   bytes those copies produced
  * @param allocatedMax
  *   the allocator's high-water mark seen after a batch: JVM buffers plus the C
  *   buffers registered on import, the number capability G3 budgets
  */
final case class ReadMetrics(
    jniCalls: Long,
    jniNanos: Long,
    batches: Long,
    arrowBytes: Long,
    copies: Long,
    copiedBytes: Long,
    allocatedMax: Long
) {

  /** Aggregate readers in one task; allocator high-water marks are not sums. */
  def +(other: ReadMetrics): ReadMetrics = ReadMetrics(
    jniCalls + other.jniCalls,
    jniNanos + other.jniNanos,
    batches + other.batches,
    arrowBytes + other.arrowBytes,
    copies + other.copies,
    copiedBytes + other.copiedBytes,
    math.max(allocatedMax, other.allocatedMax)
  )
}

object ReadMetrics {
  val Zero: ReadMetrics = ReadMetrics(0L, 0L, 0L, 0L, 0L, 0L, 0L)
}
