package com.zilliz.spark.connector.read

import scala.reflect.ClassTag

import org.apache.spark.{NarrowDependency, Partition, SparkContext, TaskContext}
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
    val queryCount = queries.count
    if (queryCount == 0L)
      return MilvusSearch.Hits(
        MilvusSearch.missing(spark, required, schema.size),
        schema
      )
    require(
      queryCount <= Int.MaxValue,
      s"A search takes at most ${Int.MaxValue} queries at a time, not $queryCount"
    )
    val rowBytes = rowWidth(types, dimension)
    // The frame search and its merge run under Spark's own task slots, so a
    // task's share of the unmanaged heap is the queries' budget at that
    // concurrency, as for a Milvus table input's merge.
    val (_, heap) = SearchResources.executorMemory(spark)
    val fraction = SearchResources.memoryFraction(spark)
    val resources = TaskResources.of(spark)
    val taskHeap = SearchResources.queryBudget(
      heap,
      fraction,
      resources.tasksPerExecutor
    )
    // One task per core here, so a group is sized for one core within that
    // task's share of the heap.
    val group = SearchResources.groupBytes(
      limits.groupMaxBytes,
      taskHeap.bytes,
      1,
      SearchPlan.bytesPerQuery(layout, k) + k.toLong * rowBytes
    )
    val groups =
      cut(queryCount.toInt, layout, k, rowBytes, group.bytes)
    val blockRows = math
      .max(
        1L,
        SearchResources.exactScanBatch(None).bytes / layout.rowBytes.toLong
      )
      .min(MaxBlockRows.toLong)
      .toInt
    val queryBytes = SearchQueries.bytes(queryCount, layout)
    val delivery =
      SearchQueries.Delivery.of(queries, layout, limits.queriesMaxBytes)
    val delivered = delivery.groups(
      spark,
      groups,
      metric,
      layout,
      resources,
      SearchResources.queryBudget(heap, fraction, 1).bytes
    )
    val tasks = base.rows.getNumPartitions * groups.size
    logInfo(
      s"Search plan: DataFrame input, exact, metric=$metric, topK=$k, " +
        s"queries=$queryCount, queryBytes=$queryBytes delivered by " +
        s"${delivery.describe}, queryGroups=${groups.size}, " +
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
    // Here every candidate carries its row.
    val parts = MilvusSearch.mergePartitions(
      spark,
      queryCount,
      math.max(1, tasks),
      k,
      taskHeap.bytes,
      CandidateBytes.Width.toLong + 4L + rowBytes
    )
    val width = types.size
    val hitWidth = schema.size
    val rows = MilvusSearch.mergedByQuery[InternalRow](
      spark,
      candidates,
      parts,
      CarriedCandidates.merge(_, _, k, metric),
      carried(_, _, width),
      required,
      MilvusSearch.absentRow(_, hitWidth)
    )
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
          vectors(count) = baseVector(vector.eval(row), first, function)
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
            metrics.record,
            Some(EngineRange.Batch.of(batch, layout, metric, function))
          )
        finally batch.close()
        // Rows pushed out of every heap are let go once they are as many as
        // the heaps hold.
        keepHeld(kept, merger, copies, firstRow, 2L * group.queries * k)
        java.util.Arrays.fill(copies.asInstanceOf[Array[AnyRef]], null)
        java.util.Arrays.fill(vectors.asInstanceOf[Array[AnyRef]], null)
      }
      val answers = carriedAnswers(merger, group, kept, metric)
      metrics.candidates.add(merger.size.toLong)
      answers
    } finally {
      matrix.close()
      allocator.close()
    }
  }

  /** A base row's vector as the scan takes it, or NULL for one that ranks
    * nothing: a NULL vector, or one with a NULL element. A vector of another
    * length than the queries' is scored against `first`, the first query as the
    * engine received it, so that it fails as Spark's function fails on that
    * pair.
    */
  private def baseVector(
      value: Any,
      first: Array[Float],
      function: RankingFunction
  ): Array[Float] = value match {
    case null => null
    case array: ArrayData =>
      val size = array.numElements()
      if (size != first.length) {
        function.score(first, new Array[Float](size))
        throw new IllegalStateException(
          s"A base vector of $size elements met a query of ${first.length}, and the ranking function did not fail"
        )
      } else if ((0 until size).exists(array.isNullAt)) null
      else array.toFloatArray()
  }

  /** Keeps, by position in the partition, the rows of the block starting at
    * `firstRow` that some query's heap now holds; once `kept` holds more than
    * `limit`, the rows no heap holds any more are let go.
    */
  private def keepHeld(
      kept: java.util.HashMap[java.lang.Long, UnsafeRow],
      merger: TopKMerger,
      copies: Array[UnsafeRow],
      firstRow: Long,
      limit: Long
  ): Unit = {
    merger.foreachKept { (_, offset) =>
      if (offset >= firstRow)
        kept.putIfAbsent(offset, copies((offset - firstRow).toInt))
    }
    if (kept.size > limit) {
      val held = new java.util.HashMap[java.lang.Long, UnsafeRow]()
      merger.foreachKept((_, offset) =>
        held.putIfAbsent(offset, kept.get(offset))
      )
      kept.clear()
      kept.putAll(held)
    }
  }

  /** Each query's best k, every candidate carrying its row. */
  private def carriedAnswers(
      merger: TopKMerger,
      group: SearchQueries.Group,
      kept: java.util.HashMap[java.lang.Long, UnsafeRow],
      metric: MetricType
  ): Iterator[(Long, Array[Byte])] = {
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
    answers.result().iterator
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
