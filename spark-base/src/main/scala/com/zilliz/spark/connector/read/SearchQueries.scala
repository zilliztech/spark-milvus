package com.zilliz.spark.connector.read

import java.lang.{Float => JavaFloat}
import java.util.Arrays
import scala.collection.mutable

import org.apache.spark.rdd.RDD
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.Partitioner

import com.zilliz.milvus.storage.index.{QueryMatrix, SearchPlan}
import com.zilliz.milvus.storage.schema.{MetricType, VectorLayout}
import com.zilliz.spark.connector.options.{MilvusOption, TaskResources}

/** The query set a search takes: `query_id` and `vector`, the query rows of a
  * nearest-by join numbered by their position, packed into the bytes Knowhere
  * reads.
  *
  * The same packing serves both delivery paths of section 2.1: when the set
  * fits `milvus.search.queries.max.bytes` the executors pack their partitions
  * and the driver only concatenates the bytes before broadcasting them
  * ([[packOnExecutors]]); when it does not, the packing job packs one group per
  * task and the groups travel with the shuffle ([[packedGroups]]). A query
  * vector is ARRAY<FLOAT>, what Spark's vector functions take, and is packed in
  * the field's element type.
  */
private[read] object SearchQueries {

  val IdColumn = "query_id"
  val VectorColumn = "vector"

  /** A query set as a search takes it. */
  sealed trait Input {

    /** How many queries the set holds. */
    def count: Long
  }

  /** `query_id` and `vector` as a frame of `count` rows, packed by Spark jobs.
    * The caller has counted the rows already; counting them here would run the
    * frame once more.
    */
  final case class Frame(selected: DataFrame, count: Long) extends Input

  /** A set already packed on the driver: row `i` of `vectors` is the query of
    * `ids(i)`. It is broadcast as it is, and nothing about it runs a job
    * (docs/design/architecture/dataframe-api.html section 4).
    */
  final case class Packed(ids: Array[Long], vectors: Array[Byte])
      extends Input
      with Delivery {
    override def count: Long = ids.length.toLong
  }

  /** How a query set reaches the first-stage tasks, decided once for a search
    * (docs/design/architecture/vector-search.html section 2.1): a set the
    * driver packed is broadcast as it is, a frame that fits
    * `milvus.search.queries.max.bytes` is packed on the executors and
    * broadcast, and a larger one travels by group with the shuffle.
    */
  sealed trait Delivery {

    /** Whether the query groups travel with the shuffle. */
    def shuffled: Boolean = false

    /** How the plan log describes this delivery. */
    def describe: String = this match {
      case Packed(_, _) => "broadcast, packed on the driver"
      case Broadcast(_) => "broadcast"
      case Shuffled(_)  => "shuffle"
    }

    /** Delivers the query set as one partition for each planned group of
      * `planned`.
      */
    def groups(
        spark: SparkSession,
        planned: Seq[SearchPlan.QueryGroup],
        metric: MetricType,
        layout: VectorLayout,
        resources: TaskResources,
        executorQueryHeapBytes: Long
    ): RDD[Group] = this match {
      case Packed(ids, vectors) =>
        broadcastGroups(spark, ids, vectors, planned)
      case Broadcast(selected) =>
        // Packed where the rows are read: the driver gets bytes, not boxed
        // vectors, and executors pack in parallel instead of one driver thread.
        val (ids, vectors) = packOnExecutors(selected, layout, metric)
        broadcastGroups(spark, ids, vectors, planned)
      case Shuffled(selected) =>
        packedGroups(
          selected,
          planned,
          metric,
          layout,
          packProfile(planned, layout, resources, executorQueryHeapBytes)
        )
    }
  }

  /** A frame packed on the executors, concatenated on the driver and broadcast
    * whole ([[packOnExecutors]]).
    */
  final case class Broadcast(selected: DataFrame) extends Delivery

  /** A frame packed by group on the executors, never through the driver, each
    * group read with the shuffle by the tasks of its range.
    */
  final case class Shuffled(selected: DataFrame) extends Delivery {
    override def shuffled: Boolean = true
  }

  object Delivery {

    def of(queries: Input, layout: VectorLayout, maxBytes: Long): Delivery =
      queries match {
        case packed: Packed => packed
        case Frame(selected, count) =>
          if (bytes(count, layout) > maxBytes) Shuffled(selected)
          else Broadcast(selected)
      }
  }

  /** One query group as it reaches a task: the ids of its queries, the bytes
    * they were packed into, and where the group starts in those bytes. A
    * broadcast query set is packed once and every group points into it; a group
    * delivered with the shuffle carries its own bytes and starts at zero.
    */
  final case class Group(
      ids: Array[Long],
      vectors: Array[Byte],
      firstQuery: Int
  ) extends Serializable {
    def queries: Int = ids.length
  }

  object Group {

    /** The planned group `group` of a set broadcast whole: its ids sliced out,
      * the bytes shared.
      */
    def of(
        ids: Array[Long],
        vectors: Array[Byte],
        group: SearchPlan.QueryGroup
    ): Group =
      Group(
        ids.slice(group.firstQuery, group.untilQuery),
        vectors,
        group.firstQuery
      )
  }

  /** The bytes a query set of this many queries occupies. */
  def bytes(queries: Long, layout: VectorLayout): Long =
    queries * layout.rowBytes.toLong

  /** A query set broadcast whole, as one partition for each planned group. */
  private def broadcastGroups(
      spark: SparkSession,
      ids: Array[Long],
      vectors: Array[Byte],
      groups: Seq[SearchPlan.QueryGroup]
  ): RDD[Group] = {
    val total = groups.map(_.queries.toLong).sum
    require(
      ids.length.toLong == total,
      s"The query set packed to ${ids.length} queries, but the plan counted $total"
    )
    val held = spark.sparkContext.broadcast((ids, vectors))
    val planned = groups.toVector
    spark.sparkContext
      .parallelize(planned.indices, planned.size)
      .map { index =>
        val (allIds, allVectors) = held.value
        Group.of(allIds, allVectors, planned(index))
      }
  }

  /** The task resources of the stage that packs the query groups for the
    * shuffle. A packing task holds one group's ids and vectors on the heap,
    * outside what Spark manages, so no more of them run at once on an executor
    * than that heap holds with room to spare.
    */
  private def packProfile(
      groups: Seq[SearchPlan.QueryGroup],
      layout: VectorLayout,
      resources: TaskResources,
      executorQueryHeapBytes: Long
  ): Option[ResourceProfile] = {
    val perGroup = groups.map(_.queries.toLong).max *
      (SearchPlan.QueryIdBytes + layout.rowBytes.toLong)
    val fits =
      math.max(1L, executorQueryHeapBytes / (perGroup + perGroup / 2))
    resources.atMost(math.min(fits, Int.MaxValue.toLong).toInt)
  }

  /** The query set packed on the executors, one partition per query group, each
    * the output of a shuffle.
    *
    * `zipWithIndex` gives every query the position that decides its group and
    * its place in the group, so the groups are the same ones the driver planned
    * and a group's rows are packed in query order without sorting. One task
    * packs one group, and the groups are packed in parallel.
    *
    * The packed group then goes through a second shuffle, keyed by its group,
    * so that it is written once to the local disk of the executor that packed
    * it and every first-stage task that needs it reads it from there. Nothing
    * is stored in memory: [[SearchQueryRanges]] reads a task's groups one at a
    * time, and reading a group again is a shuffle read, not a second packing of
    * the query set.
    */
  private[read] def packedGroups(
      selected: DataFrame,
      groups: Seq[SearchPlan.QueryGroup],
      metric: MetricType,
      layout: VectorLayout,
      profile: Option[ResourceProfile] = None
  ): RDD[Group] = {
    // Where each planned group starts. The byte limit cuts equal groups and a
    // shorter last one, an even cut puts the longer groups first (decision
    // 32), so a position finds its group among the starts, not by dividing.
    val starts = groups.map(_.firstQuery.toLong).toArray
    val rowBytes = layout.rowBytes
    val sizes = groups.map(_.queries).toVector
    // A row arrives, is written where it belongs and is done with: its
    // position says both its group and its place in the group, so the task
    // holds the group's arrays and the row in hand. Holding the group's rows
    // before copying them holds the group twice. `groupByKey` did that (a heap
    // dump of a failed 1 GiB query set found 2.03 GB in `byte[4096]` beside
    // the arrays they were being copied into), and so did sorting each group
    // by position first: four packing tasks of 232 MB groups ran a 4 GiB
    // executor heap out (UAT chenbiao-qs2c-31m-8g-ef151-0).
    val byGroup = new Partitioner {
      override def numPartitions: Int = sizes.size
      override def getPartition(key: Any): Int = {
        val found = Arrays.binarySearch(starts, key.asInstanceOf[Long])
        if (found >= 0) found else -found - 2
      }
    }
    // The packed group keeps its group's partition through the second shuffle.
    val byIndex = new Partitioner {
      override def numPartitions: Int = sizes.size
      override def getPartition(key: Any): Int = key.asInstanceOf[Int]
    }
    // Numbered before they are packed: zipWithIndex first counts the rows of
    // every partition in a job of its own, and that job has no need to pack.
    val packed = selected.queryExecution.toRdd
      .zipWithIndex()
      .map { case (row, position) =>
        val (id, values) = readQuery(row, position, layout, metric)
        (position, (id, QueryMatrix.packFloats(Seq(values), layout)))
      }
      .partitionBy(byGroup)
      .mapPartitionsWithIndex { (index, entries) =>
        val queries = sizes(index)
        val first = starts(index)
        val ids = new Array[Long](queries)
        val vectors = new Array[Byte](queries * rowBytes)
        var placed = 0
        entries.foreach { case (position, (id, packed)) =>
          val at = position - first
          require(
            at >= 0L && at < queries,
            s"Query at position $position does not belong to group $index"
          )
          ids(at.toInt) = id
          System.arraycopy(packed, 0, vectors, at.toInt * rowBytes, rowBytes)
          placed += 1
        }
        require(
          placed == queries,
          s"Query group $index takes $queries queries and was given $placed"
        )
        Iterator((index, Group(ids, vectors, 0)))
      }
    profile
      .fold(packed)(packed.withResources)
      .partitionBy(byIndex)
      .values
  }

  /** Rows an executor packs at a time: 2,048 queries of 768 dimensions are 6
    * MiB, small enough to hold as float arrays beside the packed bytes.
    */
  private val PackChunkRows = 2048

  /** The whole query set packed on the executors, one block per partition, and
    * brought to the driver as bytes: what the broadcast path ships.
    *
    * `collect()` would bring the rows themselves: every vector boxed to a
    * `Seq[Float]` and unpacked again on the driver, one thread doing it all.
    * That work falls between the collect job and the next stage, where no job
    * timing shows it: 10 s of a 250,000-query chunk on eight executors, of
    * which 2 s was the broadcast and 8 s the driver repacking. Here each
    * partition reads its rows as `InternalRow`, takes the vector out as a
    * primitive array, and packs it in place; the driver receives `rowBytes` per
    * query and no objects. Partitions keep their order, so row `i` of the
    * result is query `ids(i)`, the set's order.
    */
  def packOnExecutors(
      selected: DataFrame,
      layout: VectorLayout,
      metric: MetricType
  ): (Array[Long], Array[Byte]) = {
    val blocks = selected.queryExecution.toRdd
      .mapPartitionsWithIndex { (partition, rows) =>
        Iterator.single((partition, packPartition(rows, layout, metric)))
      }
      .collect()
      .sortBy(_._1)
    val queries = blocks.iterator.map(_._2._1.length.toLong).sum
    val total = blocks.iterator.map(_._2._2.length.toLong).sum
    require(
      total <= Int.MaxValue,
      s"The packed query set is $total bytes, over what one array holds; lower ${MilvusOption.SearchQueriesMaxBytes} so the set travels with the shuffle"
    )
    val ids = new Array[Long](queries.toInt)
    val vectors = new Array[Byte](total.toInt)
    var idAt = 0
    var byteAt = 0
    blocks.foreach { case (_, (blockIds, blockBytes)) =>
      System.arraycopy(blockIds, 0, ids, idAt, blockIds.length)
      System.arraycopy(blockBytes, 0, vectors, byteAt, blockBytes.length)
      idAt += blockIds.length
      byteAt += blockBytes.length
    }
    (ids, vectors)
  }

  /** One partition's rows packed on the executor that read them. Rows are taken
    * as they come and never retained: a chunk of primitive arrays is packed and
    * dropped before the next is read.
    */
  private[read] def packPartition(
      rows: Iterator[InternalRow],
      layout: VectorLayout,
      metric: MetricType
  ): (Array[Long], Array[Byte]) = {
    val ids = mutable.ArrayBuilder.make[Long]
    val chunks = mutable.ArrayBuffer.empty[Array[Byte]]
    var total = 0L
    val floats = mutable.ArrayBuffer.empty[Array[Float]]
    def flush(): Unit = {
      val packed = QueryMatrix.packFloats(floats.toSeq, layout)
      floats.clear()
      if (packed.nonEmpty) {
        chunks += packed
        total += packed.length
      }
    }
    var index = 0
    while (rows.hasNext) {
      val (id, values) = readQuery(rows.next(), index, layout, metric)
      floats += values
      ids += id
      index += 1
      if (index % PackChunkRows == 0) flush()
    }
    flush()
    val packed = new Array[Byte](Math.toIntExact(total))
    var at = 0
    chunks.foreach { chunk =>
      System.arraycopy(chunk, 0, packed, at, chunk.length)
      at += chunk.length
    }
    (ids.result(), packed)
  }

  /** The id and values of the query at `position`, counted from zero. */
  private def readQuery(
      row: InternalRow,
      position: Long,
      layout: VectorLayout,
      metric: MetricType
  ): (Long, Array[Float]) = {
    require(
      !row.isNullAt(0),
      s"Query ${position + 1} of the set has no $IdColumn"
    )
    val id = row.getLong(0)
    require(!row.isNullAt(1), s"Query $id has no $VectorColumn")
    (id, finiteValues(id, row.getArray(1).toFloatArray(), layout, metric))
  }

  private def finiteValues(
      id: Long,
      values: Array[Float],
      layout: VectorLayout,
      metric: MetricType
  ): Array[Float] = {
    require(
      values.length == layout.dimension,
      s"Query $id has ${values.length} values; the field has ${layout.dimension} dimensions"
    )
    require(
      values.forall(JavaFloat.isFinite),
      s"Query $id holds a value that is not finite"
    )
    require(
      metric != MetricType.Cosine || values.exists(_ != 0.0f),
      s"Query $id has a zero norm, which COSINE has no answer for"
    )
    values
  }
}
