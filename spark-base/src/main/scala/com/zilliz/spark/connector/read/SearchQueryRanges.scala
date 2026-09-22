package com.zilliz.spark.connector.read

import org.apache.spark.rdd.{PartitionCoalescer, PartitionGroup, RDD}

/** Joins the packed query groups, one per parent partition, into the planned
  * query ranges: coalesced partition `r` is the groups of range `r`, in order.
  * Spark's default coalescer balances by locality and size and may put any
  * groups together, while a task needs to know how many groups it answers and
  * which, so that the cartesian's task `set × ranges + range` is the planned
  * one (docs/design/architecture/vector-search.html section 2.1, decision 28).
  */
private[read] final case class SearchQueryRanges(ranges: Seq[Range])
    extends PartitionCoalescer
    with Serializable {

  override def coalesce(
      maxPartitions: Int,
      parent: RDD[_]
  ): Array[PartitionGroup] = {
    val partitions = parent.partitions
    require(
      ranges.flatten == partitions.indices,
      s"Query ranges ${ranges.mkString(", ")} do not cover the " +
        s"${partitions.length} packed groups in order"
    )
    require(
      maxPartitions == ranges.size,
      s"Coalescing into $maxPartitions partitions, planned ${ranges.size} ranges"
    )
    ranges.map { range =>
      val group = new PartitionGroup()
      range.foreach(index => group.partitions += partitions(index))
      group
    }.toArray
  }
}
