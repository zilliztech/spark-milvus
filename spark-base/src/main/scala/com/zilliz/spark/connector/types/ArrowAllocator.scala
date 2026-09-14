package com.zilliz.spark.connector.types

import org.apache.arrow.memory.RootAllocator

/** The allocator every Arrow buffer this connector owns comes from.
  *
  * One per process, shared by the readers and writers, so what crosses from the
  * native layer is accounted in one place. Spark's own
  * `org.apache.spark.sql.util.ArrowUtils.rootAllocator` would be the natural
  * choice but it is `private[sql]`.
  *
  * The limit is unbounded, which matches what the connector did before. Capping
  * it against the executor's off-heap budget is capability G3.
  */
object ArrowAllocator {

  private lazy val root: RootAllocator = new RootAllocator(Long.MaxValue)

  def get: RootAllocator = root
}
