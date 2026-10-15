package com.zilliz.spark.connector.search

import java.util.BitSet
import scala.reflect.ClassTag

import org.apache.arrow.memory.ArrowBuf
import org.apache.spark.{NarrowDependency, Partition, SparkContext, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{
  Expression,
  GenericInternalRow,
  UnsafeProjection,
  UnsafeRow
}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{ArrayType, DataType, FloatType}
import org.apache.spark.sql.SparkSession
import org.apache.spark.unsafe.Platform

import com.zilliz.milvus.storage.read.plan.ReadLimits
import com.zilliz.milvus.storage.schema.{
  MetricType,
  VectorElementType,
  VectorLayout
}
import com.zilliz.milvus.storage.vector.{
  CarriedCandidates,
  EngineRange,
  ExactScan,
  QueryMatrix,
  RankingFunction,
  TopKMerger,
  VectorBatch
}
import com.zilliz.spark.connector.metrics.SearchMetrics
import com.zilliz.spark.connector.options.SearchLimits
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

  /** The base as the first stage reads it: its rows, the types of its output,
    * and its vector, bound to that output.
    */
  final case class Base(
      rows: RDD[InternalRow],
      projection: Seq[Expression],
      vector: Expression
  ) {
    def types: Seq[DataType] = projection.map(_.dataType)
  }

  /** Each searched query's id with its best k base rows, each owning its bytes.
    * With `required`, an id that found nothing has a row with the base columns
    * NULL, as a Milvus table input's search gives it.
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
  ): RDD[(Long, InternalRow)] = {
    val layout = VectorLayout(VectorElementType.Float32, dimension)
    val types = base.types
    val width = types.size
    val absent: Long => (Long, InternalRow) =
      id => id -> new GenericInternalRow(width)
    val queryCount = queries.count
    if (queryCount == 0L)
      return SearchMerge.missing(spark, required, width)
    require(
      queryCount <= Int.MaxValue,
      s"A search takes at most ${Int.MaxValue} queries at a time, not $queryCount"
    )
    val rowBytes = rowWidth(types, dimension)
    val execution = SearchExecutionPlan.frame(
      spark,
      limits,
      queries,
      layout,
      k,
      rowBytes,
      base.rows.getNumPartitions
    )
    val groups = execution.groups
    val delivery = execution.delivery
    val blockRows = execution.blockRows
    val queryBytes = SearchQueries.bytes(queryCount, layout)
    val delivered = delivery.groups(
      spark,
      groups,
      metric,
      layout,
      execution.packingProfile
    )
    val tasks = execution.tasks
    logInfo(
      s"Search plan: DataFrame input, exact, metric=$metric, topK=$k, " +
        s"queries=$queryCount, queryBytes=$queryBytes delivered by " +
        s"${delivery.describe}, queryGroups=${groups.size}, " +
        s"basePartitions=${base.rows.getNumPartitions}, tasks=$tasks, " +
        s"blockRows=$blockRows, rowBytes=$rowBytes"
    )
    val vector = base.vector
    val projection = base.projection
    val blockMaxBytes = execution.blockMaxBytes
    val retainedMaxBytes = execution.retainedMaxBytes
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
          projection,
          layout,
          k,
          metric,
          function,
          blockRows,
          blockMaxBytes,
          retainedMaxBytes,
          metrics
        )
    )
    // Here every candidate carries its row.
    val parts = execution.mergePartitions
    SearchMerge.byQuery[(Long, InternalRow)](
      spark,
      candidates,
      parts,
      CarriedCandidates.merge(_, _, k, metric),
      carried(_, _, width),
      required,
      absent
    )
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

  /** One task: a base partition read a block at a time against one group. */
  private def search(
      rows: Iterator[InternalRow],
      group: SearchQueries.Group,
      unit: Int,
      vector: Expression,
      projection: Seq[Expression],
      layout: VectorLayout,
      k: Int,
      metric: MetricType,
      function: RankingFunction,
      blockRows: Int,
      blockMaxBytes: Long,
      retainedMaxBytes: Long,
      metrics: SearchMetrics
  ): Iterator[(Long, Array[Byte])] = {
    if (!rows.hasNext) return Iterator.empty
    val dimension = layout.dimension
    val allocator = ArrowAllocator.forSearchTask(
      TaskContext.get().partitionId(),
      ReadLimits.DefaultArrowMaxBytes
    )
    try {
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
        val project = UnsafeProjection.create(projection)
        val capacity = math
          .min(blockRows.toLong, math.max(1L, blockMaxBytes / layout.rowBytes))
          .toInt
        val vectors =
          allocator.allocator.buffer(capacity.toLong * layout.rowBytes)
        try {
          val copies = new Array[UnsafeRow](capacity)
          // The rows some query's heap holds, by their position in the partition.
          var kept = new java.util.HashMap[java.lang.Long, UnsafeRow]()
          var position = 0L
          while (rows.hasNext) {
            val firstRow = position
            val excluded = new BitSet(capacity)
            var count = 0
            var blockBytes = 0L
            while (
              count < capacity && blockBytes < blockMaxBytes && rows.hasNext
            ) {
              val row = rows.next()
              copies(count) =
                if (
                  writeVector(
                    vector.eval(row),
                    first,
                    function,
                    vectors,
                    count.toLong * layout.rowBytes
                  )
                )
                  project(row).copy()
                else {
                  excluded.set(count)
                  null
                }
              blockBytes += layout.rowBytes.toLong +
                (if (copies(count) == null) 0L
                 else copies(count).getSizeInBytes.toLong)
              count += 1
              position += 1
            }
            val batch = VectorBatch.borrow(
              vectors.nioBuffer(0L, count * layout.rowBytes),
              count,
              firstRow,
              excluded
            )
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
            // The synchronous scan no longer borrows the vector buffer.
            kept = keepHeld(kept, merger, copies, firstRow, retainedMaxBytes)
            java.util.Arrays.fill(copies.asInstanceOf[Array[AnyRef]], null)
          }
          val answers = carriedAnswers(merger, group, kept, retainedMaxBytes)
          metrics.candidates.add(merger.size.toLong)
          answers
        } finally vectors.close()
      } finally matrix.close()
    } finally allocator.close()
  }

  /** Writes a base vector directly into the scan's buffer. A NULL vector or
    * element excludes the whole row, including any prefix already written.
    * Dimension mismatches fail through the ranking function before inspecting
    * NULL elements, as they do for a Spark pair.
    */
  private def writeVector(
      value: Any,
      first: Array[Float],
      function: RankingFunction,
      buffer: ArrowBuf,
      offset: Long
  ): Boolean = value match {
    case null => false
    case array: ArrayData =>
      val size = array.numElements()
      if (size != first.length) {
        function.score(first, new Array[Float](size))
        throw new IllegalStateException(
          s"A base vector of $size elements met a query of ${first.length}, and the ranking function did not fail"
        )
      }
      var element = 0
      while (element < size) {
        if (array.isNullAt(element)) return false
        buffer.setFloat(offset + element * 4L, array.getFloat(element))
        element += 1
      }
      true
  }

  /** Retain only rows still held by a heap, measuring their actual bytes. */
  private def keepHeld(
      kept: java.util.HashMap[java.lang.Long, UnsafeRow],
      merger: TopKMerger,
      copies: Array[UnsafeRow],
      firstRow: Long,
      maxBytes: Long
  ): java.util.HashMap[java.lang.Long, UnsafeRow] = {
    val held = new java.util.HashMap[java.lang.Long, UnsafeRow]()
    var bytes = 0L
    merger.foreachKept { (_, offset) =>
      val row =
        if (offset >= firstRow) copies((offset - firstRow).toInt)
        else kept.get(offset)
      if (held.putIfAbsent(offset, row) == null)
        bytes += row.getSizeInBytes.toLong
    }
    require(
      bytes <= maxBytes,
      s"DataFrame search candidates retain $bytes row bytes, exceeding the task budget of $maxBytes; reduce k or the returned columns, or increase executor memory"
    )
    held
  }

  /** Encodes one query when pulled. The iterator owns only heap data, so the
    * query matrix and allocator can close before its answers are consumed.
    */
  private[search] def carriedAnswers(
      merger: TopKMerger,
      group: SearchQueries.Group,
      kept: java.util.HashMap[java.lang.Long, UnsafeRow],
      maxBytes: Long
  ): Iterator[(Long, Array[Byte])] = {
    val ids = group.ids
    ids.indices.iterator.flatMap { query =>
      merger.sort(query)
      val results = merger.results(query)
      if (results.isEmpty) Iterator.empty
      else {
        val bytes = CarriedCandidates.encodedSize(results)(candidate =>
          kept.get(candidate.rowOffset).getSizeInBytes
        )
        require(
          bytes <= maxBytes && bytes <= Int.MaxValue.toLong,
          s"DataFrame search query ${ids(query)} encodes $bytes candidate bytes, exceeding the answer limit of ${math
              .min(maxBytes, Int.MaxValue.toLong)}; reduce k or the returned columns, or increase executor memory"
        )
        Iterator.single(
          ids(query) -> CarriedCandidates.writeSorted(results, bytes.toInt)(
            candidate => kept.get(candidate.rowOffset).getBytes
          )
        )
      }
    }
  }

  /** Each merged base row owns its bytes when retained by the join. */
  private[search] def carried(
      query: Long,
      packed: Array[Byte],
      width: Int
  ): Iterator[(Long, InternalRow)] = {
    // Preserve eager format and per-Spark row-region checks before handoff.
    val row = new UnsafeRow(width)
    CarriedCandidates.foreach(packed) { (_, _, _, at, length) =>
      row.pointTo(packed, Platform.BYTE_ARRAY_OFFSET + at, length)
    }
    CarriedCandidates.mapRows(packed) { (at, length) =>
      row.pointTo(packed, Platform.BYTE_ARRAY_OFFSET + at, length)
      query -> row.copy()
    }
  }

  /** One task for every base partition and query group: task `i` computes base
    * partition `i / groupCount` and reads group `i % groupCount`, each through
    * a narrow dependency, so neither side is shuffled to pair them.
    */
  private[search] final class Pairs[G: ClassTag] private (
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

  private[search] object Pairs {

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
