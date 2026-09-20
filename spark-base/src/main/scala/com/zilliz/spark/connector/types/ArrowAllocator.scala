package com.zilliz.spark.connector.types

import org.apache.arrow.memory.{BufferAllocator, RootAllocator}

/** One read task's Arrow allocation scope.
  *
  * The final Spark partition reader owns this object. Closing it never closes
  * the process-wide root, and a repeated close is a no-op.
  */
private[connector] final class TaskArrowAllocator private[types] (
    val allocator: BufferAllocator
) extends AutoCloseable {
  private var closed = false

  override def close(): Unit = synchronized {
    if (!closed) {
      allocator.close()
      closed = true
    }
  }

  private[connector] def isClosed: Boolean = synchronized { closed }
}

/** The allocator every Arrow buffer this connector owns comes from.
  *
  * One per process, shared by the readers and writers, so what crosses from the
  * native layer is accounted in one place. Spark's own
  * `org.apache.spark.sql.util.ArrowUtils.rootAllocator` would be the natural
  * choice but it is `private[sql]`.
  *
  * The root remains unbounded so unrelated tasks do not share one arbitrary
  * cap. Each read task gets a child whose limit comes from its serialized
  * [[com.zilliz.milvus.storage.read.plan.ReadLimits]].
  */
object ArrowAllocator {

  private lazy val root: RootAllocator = new RootAllocator(Long.MaxValue)

  def get: RootAllocator = root

  /** A vector search task's scope: it holds the query group, what it keeps of
    * its segment set, and the buffers Knowhere writes into.
    */
  private[connector] def forSearchTask(
      partition: Int,
      maxBytes: Long
  ): TaskArrowAllocator = {
    require(maxBytes > 0L, s"maxBytes must be positive, got $maxBytes")
    new TaskArrowAllocator(
      root.newChildAllocator(s"milvus-search-task-$partition", 0L, maxBytes)
    )
  }

  /** One index build's scope: the segment's vectors gathered into one buffer
    * and what Knowhere serializes from them.
    */
  private[connector] def forIndexBuild(
      segmentId: Long,
      maxBytes: Long
  ): TaskArrowAllocator = {
    require(maxBytes > 0L, s"maxBytes must be positive, got $maxBytes")
    new TaskArrowAllocator(
      root.newChildAllocator(s"milvus-index-build-$segmentId", 0L, maxBytes)
    )
  }

  private[connector] def forReadTask(
      segmentId: Long,
      maxBytes: Long
  ): TaskArrowAllocator = {
    require(maxBytes > 0L, s"maxBytes must be positive, got $maxBytes")
    new TaskArrowAllocator(
      root.newChildAllocator(s"milvus-read-segment-$segmentId", 0L, maxBytes)
    )
  }
}
