package com.zilliz.milvus.storage.write.exec

/** What one segment write cost on the C/JVM crossing. Design:
  * docs/design/architecture/storage-io.html section 5.
  *
  * @param jniCalls
  *   calls into `StorageNative` by this writer, the open and the finishing
  *   close included
  * @param jniNanos
  *   wall time spent inside those calls
  * @param batches
  *   Arrow batches handed to the native writer
  * @param arrowBytes
  *   bytes of the buffers those batches carry, summed over their vectors
  * @param allocatedMax
  *   the allocator's high-water mark seen after a batch
  */
final case class WriteMetrics(
    jniCalls: Long,
    jniNanos: Long,
    batches: Long,
    arrowBytes: Long,
    allocatedMax: Long
)

object WriteMetrics {
  val Zero: WriteMetrics = WriteMetrics(0L, 0L, 0L, 0L, 0L)
}
