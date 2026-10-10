package com.zilliz.spark.connector.read

import scala.reflect.ClassTag

import org.apache.spark.{
  HashPartitioner,
  NarrowDependency,
  Partition,
  SparkContext,
  TaskContext
}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{
  Expression,
  GenericInternalRow,
  JoinedRow,
  UnsafeProjection,
  UnsafeRow
}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{
  ArrayType,
  DataType,
  FloatType,
  LongType,
  StructField,
  StructType
}
import org.apache.spark.sql.SparkSession
import org.apache.spark.unsafe.Platform

import com.zilliz.milvus.storage.index.{
  CandidateBytes,
  CarriedCandidates,
  EngineRange,
  ExactScan,
  QueryMatrix,
  RankingFunction,
  SearchPlan,
  SegmentSearch,
  TopKMerger,
  VectorBatch
}
import com.zilliz.milvus.storage.read.plan.ReadLimits
import com.zilliz.milvus.storage.schema.{
  MetricType,
  VectorElementType,
  VectorLayout
}
import com.zilliz.spark.connector.metrics.SearchMetrics
import com.zilliz.spark.connector.options.{
  SearchLimits,
  SearchResources,
  TaskResources
}
import com.zilliz.spark.connector.types.ArrowAllocator

/** A nearest-by join's search over a DataFrame input: a base the connector
  * cannot search as a Milvus table, read row by row in its own partitions and
  * scanned exactly, each candidate carrying its row to the merge
  * (docs/design/architecture/dataframe-api.html sections 2 and 4).
  *
  * The query groups are cut so that a group, its candidates and the rows they
  * carry stay inside `milvus.search.group.max.bytes`, and reach the tasks as a
  * Milvus table input's do: a broadcast when the driver packed them or they fit
  * `milvus.search.queries.max.bytes`, a shuffle otherwise. A task is one base
  * partition and one group, so the base is computed once for each group. It
  * reads its partition a block at a time: each row's vector into a float32
  * batch for [[ExactScan]], each row into an `UnsafeRow` copy, kept while some
  * query's heap holds the row. The search's dimension is the queries': a base
  * vector of another length fails as Spark's function fails on that pair, a
  * NULL vector or one with a NULL element ranks nothing, and a vector outside
  * [[EngineRange]] is scored by the ranking function.
  */
private[connector] object FrameSearch extends Logging {

  /** The most rows a block takes, whatever its vectors' size: every row of a
    * block is an `UnsafeRow` copy and a float array on the heap until the block
    * is searched.
    */
  private val MaxBlockRows = 65536

  /** The base as the first stage reads it: its rows, the types of its output,
    * and its vector, bound to that output.
    */
  final case class Base(
      rows: RDD[InternalRow],
      types: Seq[DataType],
      vector: Expression
  )

  /** Each searched query's best k base rows, as rows of the query id followed
    * by the base row. With `required`, an id that found nothing has a row with
    * the base columns NULL, as a Milvus table input's search gives it.
    *
    * @param queries
    *   the searched queries, numbered by the caller, every vector `dimension`
    *   long and inside [[EngineRange]]
    */
  def execute(
      spark: SparkSession,
      queries: SearchQueries.Input,
      base: Base,
      dimension: Int,
      k: Int,
      metric: MetricType,
      function: RankingFunction,
      limits: SearchLimits,
      metrics: SearchMetrics,
      required: Option[Array[Long]]
  ): MilvusSearch.Hits = {
    val layout = VectorLayout(VectorElementType.Float32, dimension)
    val types = base.types
    val schema = StructType(
      StructField(SearchQueries.IdColumn, LongType, nullable = false) +:
        types.zipWithIndex.map { case (dataType, at) =>
          StructField(s"base_$at", dataType, nullable = true)
        }
    )
    val queryCount = queries match {
      case SearchQueries.Packed(ids, _)  => ids.length.toLong
      case SearchQueries.Frame(selected) => selected.count()
    }
    if (queryCount == 0L)
      return MilvusSearch.Hits(missing(spark, required, types.size), schema)
    require(
      queryCount <= Int.MaxValue,
      s"A search takes at most ${Int.MaxValue} queries at a time, not $queryCount"
    )
    val rowBytes = rowWidth(types, dimension)
    val groups =
      cut(queryCount.toInt, layout, k, rowBytes, limits.groupMaxBytes)
    val blockRows = math
      .max(
        1L,
        SearchResources.exactScanBatch(None).bytes / layout.rowBytes.toLong
      )
      .min(MaxBlockRows.toLong)
      .toInt
    val queryBytes = SearchQueries.bytes(queryCount, layout)
    val shuffled = queries.isInstanceOf[SearchQueries.Frame] &&
      queryBytes > limits.queriesMaxBytes
    val delivered: RDD[SearchQueries.Group] = queries match {
      case SearchQueries.Frame(selected) if shuffled =>
        MilvusSearch.packedGroups(selected, groups, metric, layout)
      case _ =>
        val (ids, vectors) = queries match {
          case SearchQueries.Packed(packedIds, packedVectors) =>
            (packedIds, packedVectors)
          case SearchQueries.Frame(selected) =>
            SearchQueries.packOnExecutors(selected, layout, metric)
        }
        val held = spark.sparkContext.broadcast((ids, vectors))
        val planned = groups.toVector
        spark.sparkContext
          .parallelize(planned.indices, planned.size)
          .map { index =>
            val (allIds, allVectors) = held.value
            val group = planned(index)
            SearchQueries.Group(
              allIds.slice(group.firstQuery, group.untilQuery),
              allVectors,
              group.firstQuery
            )
          }
    }
    val tasks = base.rows.getNumPartitions * groups.size
    logInfo(
      s"Search plan: DataFrame input, exact, metric=$metric, topK=$k, " +
        s"queries=$queryCount, queryBytes=$queryBytes delivered by " +
        s"${if (shuffled) "shuffle" else "broadcast"}, queryGroups=${groups.size}, " +
        s"basePartitions=${base.rows.getNumPartitions}, tasks=$tasks, " +
        s"blockRows=$blockRows, rowBytes=$rowBytes"
    )
    val vector = base.vector
    val candidates = Pairs(
      spark.sparkContext,
      base.rows,
      delivered,
      (rows: Iterator[InternalRow], group: SearchQueries.Group, unit: Int) =>
        search(
          rows,
          group,
          unit,
          vector,
          types,
          layout,
          k,
          metric,
          function,
          blockRows,
          metrics
        )
    )
    // The merge runs under Spark's own task slots, so a task's share of the
    // unmanaged heap is the queries' budget at that concurrency, as for a
    // Milvus table input; here every candidate carries its row.
    val (_, heap) = MilvusSearch.executorMemory(spark)
    val stageHeap = SearchResources.queryBudget(
      heap,
      MilvusSearch.memoryFraction(spark),
      TaskResources.of(spark).tasksPerExecutor
    )
    val parts = MilvusSearch.mergePartitions(
      spark,
      queryCount,
      math.max(1, tasks),
      k,
      stageHeap.bytes,
      CandidateBytes.Width.toLong + 4L + rowBytes
    )
    val partitioner = new HashPartitioner(parts)
    val merged = candidates.combineByKeyWithClassTag[Array[Byte]](
      (packed: Array[Byte]) => packed,
      (kept: Array[Byte], packed: Array[Byte]) =>
        CarriedCandidates.merge(kept, packed, k, metric),
      (left: Array[Byte], right: Array[Byte]) =>
        CarriedCandidates.merge(left, right, k, metric),
      partitioner,
      mapSideCombine = false
    )
    val width = types.size
    val wanted = required.map(spark.sparkContext.broadcast(_))
    val rows = merged.mapPartitionsWithIndex { (index, answers) =>
      val found = new java.util.HashSet[java.lang.Long]()
      val hits = answers.flatMap { case (query, packed) =>
        found.add(query)
        carried(query, packed, width)
      }
      val absent = wanted.iterator.flatMap(ids =>
        ids.value.iterator
          .filter(id =>
            partitioner.getPartition(id) == index && !found.contains(id)
          )
          .map(id => absentRow(id, width))
      )
      hits ++ absent
    }
    MilvusSearch.Hits(rows, schema)
  }

  /** The bytes one base row takes as an `UnsafeRow`: its fixed part, and for a
    * column that is not fixed in length, its default size, or a vector of the
    * search's dimension for an array of floats.
    */
  private[connector] def rowWidth(types: Seq[DataType], dimension: Int): Long =
    UnsafeRow.calculateBitSetWidthInBytes(types.size).toLong +
      8L * types.size + types.map {
        case ArrayType(FloatType, _) => 8L + 8L + 4L * dimension
        case dataType if UnsafeRow.isFixedLength(dataType) => 0L
        case dataType => dataType.defaultSize.toLong
      }.sum

  /** Query groups of equal size, the last one shorter, each within the group
    * limit with its candidates and the rows they carry. One query is a group
    * however large it is: a DataFrame input takes no options to raise the limit
    * with.
    */
  private[connector] def cut(
      queries: Int,
      layout: VectorLayout,
      k: Int,
      rowBytes: Long,
      groupMaxBytes: Long
  ): Seq[SearchPlan.QueryGroup] = {
    val perQuery = SearchPlan.bytesPerQuery(layout, k) + k.toLong * rowBytes
    val size = math.max(1L, groupMaxBytes / perQuery).min(queries.toLong).toInt
    (0 until queries by size).map(first =>
      SearchPlan.QueryGroup(first, math.min(size, queries - first))
    )
  }

  /** One task: a base partition read a block at a time against one group. */
  private def search(
      rows: Iterator[InternalRow],
      group: SearchQueries.Group,
      unit: Int,
      vector: Expression,
      types: Seq[DataType],
      layout: VectorLayout,
      k: Int,
      metric: MetricType,
      function: RankingFunction,
      blockRows: Int,
      metrics: SearchMetrics
  ): Iterator[(Long, Array[Byte])] = {
    val dimension = layout.dimension
    val allocator = ArrowAllocator.forSearchTask(
      TaskContext.get().partitionId(),
      ReadLimits.DefaultArrowMaxBytes
    )
    def stepped(step: SegmentSearch.Progress): Unit = {
      if (step.nativeCalls != 0)
        metrics.knowhereCalls.add(step.nativeCalls.toLong)
      if (step.nativeNanos != 0L) metrics.knowhereNanos.add(step.nativeNanos)
      if (step.compared != 0L) metrics.comparedPairs.add(step.compared)
    }
    val matrix = QueryMatrix.ofPacked(
      group.vectors,
      group.firstQuery,
      group.queries,
      layout,
      allocator.allocator
    )
    try {
      // The first query, as the engine received it: the one a base vector of
      // another length is compared with, so the failure is the function's.
      val first = new Array[Float](dimension)
      EngineRange.decode(matrix.buffer, 0, layout, first)
      val merger = new TopKMerger(group.queries, k, metric)
      val project = UnsafeProjection.create(types.toArray)
      val vectors = new Array[Array[Float]](blockRows)
      val copies = new Array[UnsafeRow](blockRows)
      // The rows some query's heap holds, by their position in the partition.
      val kept = new java.util.HashMap[java.lang.Long, UnsafeRow]()
      var position = 0L
      while (rows.hasNext) {
        val firstRow = position
        var count = 0
        while (count < blockRows && rows.hasNext) {
          val row = rows.next()
          vectors(count) = vector.eval(row) match {
            case null => null
            case array: ArrayData =>
              val size = array.numElements()
              if (size != dimension) {
                function.score(first, new Array[Float](size))
                throw new IllegalStateException(
                  s"A base vector of $size elements met a query of $dimension, and the ranking function did not fail"
                )
              } else if ((0 until size).exists(array.isNullAt)) null
              else array.toFloatArray()
          }
          copies(count) =
            if (vectors(count) == null) null else project(row).copy()
          count += 1
          position += 1
        }
        val batch = VectorBatch.ofFloats(
          vectors,
          count,
          dimension,
          firstRow,
          allocator.allocator
        )
        try
          ExactScan.batch(
            batch,
            matrix,
            unit.toLong,
            k,
            metric,
            allocator.allocator,
            merger,
            stepped,
            Some(EngineRange.Batch.of(batch, layout, metric, function))
          )
        finally batch.close()
        merger.foreachKept { (_, offset) =>
          if (offset >= firstRow)
            kept.putIfAbsent(offset, copies((offset - firstRow).toInt))
        }
        java.util.Arrays.fill(copies.asInstanceOf[Array[AnyRef]], null)
        java.util.Arrays.fill(vectors.asInstanceOf[Array[AnyRef]], null)
        // Rows pushed out of every heap are let go once they are as many as
        // the heaps hold.
        if (kept.size > 2L * group.queries * k) {
          val held = new java.util.HashMap[java.lang.Long, UnsafeRow]()
          merger.foreachKept((_, offset) =>
            held.putIfAbsent(offset, kept.get(offset))
          )
          kept.clear()
          kept.putAll(held)
        }
      }
      val answers = Vector.newBuilder[(Long, Array[Byte])]
      var query = 0
      while (query < group.queries) {
        val results = merger.results(query)
        if (results.nonEmpty)
          answers += group.ids(query) -> CarriedCandidates.of(
            results.map(candidate =>
              candidate -> kept.get(candidate.rowOffset).getBytes
            ),
            metric
          )
        query += 1
      }
      metrics.candidates.add(merger.size.toLong)
      answers.result().iterator
    } finally {
      matrix.close()
      allocator.close()
    }
  }

  /** One query's merged answer as rows of the query id and the base row. */
  private def carried(
      query: Long,
      packed: Array[Byte],
      width: Int
  ): Iterator[InternalRow] = {
    val rows = Vector.newBuilder[InternalRow]
    CarriedCandidates.foreach(packed) { (_, _, _, at, length) =>
      val row = new UnsafeRow(width)
      row.pointTo(packed, Platform.BYTE_ARRAY_OFFSET + at, length)
      val id = new GenericInternalRow(1)
      id.setLong(0, query)
      rows += new JoinedRow(id, row)
    }
    rows.result().iterator
  }

  private def absentRow(query: Long, width: Int): InternalRow = {
    val row = new GenericInternalRow(width + 1)
    row.setLong(0, query)
    row
  }

  /** The rows of required ids when no search ran, every base column NULL. */
  private def missing(
      spark: SparkSession,
      required: Option[Array[Long]],
      width: Int
  ): RDD[InternalRow] = required match {
    case None => spark.sparkContext.emptyRDD[InternalRow]
    case Some(ids) =>
      spark.sparkContext
        .parallelize(ids.toSeq, math.max(1, math.min(ids.length, 64)))
        .map(absentRow(_, width))
  }

  /** One task for every base partition and query group: task `i` computes base
    * partition `i / groupCount` and reads group `i % groupCount`, each through
    * a narrow dependency, so neither side is shuffled to pair them.
    */
  private[read] final class Pairs[G: ClassTag] private (
      context: SparkContext,
      @transient private var base: RDD[InternalRow],
      @transient private var groups: RDD[G],
      groupCount: Int,
      run: (Iterator[InternalRow], G, Int) => Iterator[(Long, Array[Byte])]
  ) extends RDD[(Long, Array[Byte])](
        context,
        Seq(
          new NarrowDependency(base) {
            override def getParents(partition: Int): Seq[Int] =
              Seq(partition / groupCount)
          },
          new NarrowDependency(groups) {
            override def getParents(partition: Int): Seq[Int] =
              Seq(partition % groupCount)
          }
        )
      ) {

    override protected def getPartitions: Array[Partition] =
      Array.tabulate[Partition](base.getNumPartitions * groupCount) { index =>
        Pairs.Part(
          index,
          base.partitions(index / groupCount),
          groups.partitions(index % groupCount)
        )
      }

    override def compute(
        split: Partition,
        task: TaskContext
    ): Iterator[(Long, Array[Byte])] = {
      val part = split.asInstanceOf[Pairs.Part]
      val baseRdd = dependencies.head.rdd.asInstanceOf[RDD[InternalRow]]
      val groupRdd = dependencies(1).rdd.asInstanceOf[RDD[G]]
      val group = groupRdd.iterator(part.group, task).toSeq match {
        case Seq(one) => one
        case other =>
          throw new IllegalStateException(
            s"A query group partition holds ${other.size} groups, not one"
          )
      }
      run(baseRdd.iterator(part.base, task), group, part.base.index)
    }

    override def getPreferredLocations(split: Partition): Seq[String] =
      dependencies.head.rdd.preferredLocations(
        split.asInstanceOf[Pairs.Part].base
      )

    override def clearDependencies(): Unit = {
      super.clearDependencies()
      base = null
      groups = null
    }
  }

  private[read] object Pairs {

    def apply[G: ClassTag](
        context: SparkContext,
        base: RDD[InternalRow],
        groups: RDD[G],
        run: (Iterator[InternalRow], G, Int) => Iterator[(Long, Array[Byte])]
    ): Pairs[G] =
      new Pairs(context, base, groups, groups.getNumPartitions, run)

    final case class Part(index: Int, base: Partition, group: Partition)
        extends Partition
  }
}
