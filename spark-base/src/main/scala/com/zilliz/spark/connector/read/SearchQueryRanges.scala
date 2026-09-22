package com.zilliz.spark.connector.read

import org.apache.spark.{NarrowDependency, Partition, TaskContext}
import org.apache.spark.rdd.RDD

/** The first stage's view of a packed query set: partition `set × ranges +
  * range` holds the query groups of that range, in order, for that segment set
  * (docs/design/architecture/vector-search.html section 2.1, decision 28).
  *
  * `groups` is the output of the shuffle that packed them, one partition per
  * group, so a task reads its range one group at a time and never holds the
  * range whole. A group's shuffle reader sends its remote requests as soon as
  * it is opened. The task opens the first group's reader when it starts, so
  * that fetch runs while the task loads its segment set. It opens the next
  * group's reader when it hands a group on, so a remote group arrives while the
  * group before it is searched. A group packed on this executor is read from
  * its local disk when it is handed on. What a task holds of the query set is
  * the group being searched and the group being read, whatever the size of the
  * set.
  */
private[read] final class SearchQueryRanges(
    @transient private var groups: RDD[SearchQueries.Group],
    sets: Int,
    ranges: Seq[Range]
) extends RDD[SearchQueries.Group](groups.context, Nil) {

  require(sets > 0, s"A search reads its query groups for $sets segment sets")
  require(
    ranges.nonEmpty && ranges.flatten == (0 until groups.getNumPartitions),
    s"Query ranges ${ranges.mkString(", ")} do not cover the " +
      s"${groups.getNumPartitions} packed groups in order"
  )

  override protected def getPartitions: Array[Partition] = {
    val parents = groups.partitions
    Array.tabulate(sets * ranges.size) { index =>
      new SearchQueryRange(
        index,
        ranges(index % ranges.size).map(parents(_)).toArray
      )
    }
  }

  override def getDependencies: Seq[NarrowDependency[_]] = Seq(
    new NarrowDependency(groups) {
      override def getParents(partitionId: Int): Seq[Int] =
        ranges(partitionId % ranges.size)
    }
  )

  override def compute(
      split: Partition,
      context: TaskContext
  ): Iterator[SearchQueries.Group] = {
    val parent = firstParent[SearchQueries.Group]
    val parts = split.asInstanceOf[SearchQueryRange].groups
    new Iterator[SearchQueries.Group] {
      private var at = 0
      private var reading = open(0)

      private def open(position: Int): Iterator[SearchQueries.Group] =
        if (position < parts.length) parent.iterator(parts(position), context)
        else Iterator.empty

      override def hasNext: Boolean = at < parts.length

      override def next(): SearchQueries.Group = {
        if (!hasNext)
          throw new NoSuchElementException(
            s"Query range of partition ${split.index} has no more groups"
          )
        val index = parts(at).index
        require(reading.hasNext, s"Query group $index arrived with no record")
        val group = reading.next()
        // Asking once more lets the reader see its end and release what it
        // fetched, before the next reader starts fetching.
        require(
          !reading.hasNext,
          s"Query group $index arrived as more than one record"
        )
        at += 1
        reading = open(at)
        group
      }
    }
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    groups = null
  }
}

/** One first-stage task's query range: the packed groups it reads, in order. */
private[read] final class SearchQueryRange(
    override val index: Int,
    val groups: Array[Partition]
) extends Partition
