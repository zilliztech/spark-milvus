package com.zilliz.spark.connector.search

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.HashPartitioner

import com.zilliz.milvus.storage.schema.MetricType
import com.zilliz.milvus.storage.vector.{CandidateBytes, CandidateRuns}

/** The merge stage both inputs share, and the hits a search gives back.
  *
  * A first-stage task answers each query it found anything for with one packed
  * record of that query's best k; the merge combines a query's records, one per
  * task, into its global top k by query id over an RDD shuffle
  * (docs/design/architecture/vector-search.html sections 2.1 and 2.6). A Milvus
  * table input merges `CandidateBytes` into addresses of [[HitSchema]], which
  * its take stage reads for the base columns; a DataFrame input's candidates
  * carry their rows (`CarriedCandidates`), so its merge gives the hits whole. A
  * query's answers are retained as a bounded batch of sorted runs and compacted
  * into its top k by one k-way selection (`CandidateRuns`,
  * docs/design/architecture/candidate-merge.html).
  */
private[search] object SearchMerge {

  /** Each query's selected row addresses. Scores are needed only until the
    * global top-k is settled; the take reads the selected addresses.
    */
  val HitSchema: StructType = Encoders.product[SearchHit].schema

  /** The hit columns when some query is required to appear without a hit: the
    * columns of the hit are NULL in its row.
    */
  val NullableHitSchema: StructType = StructType(
    HitSchema.fields.map(field =>
      if (field.name == SearchQueries.IdColumn) field
      else field.copy(nullable = true)
    )
  )

  /** Count limit before compacting the retained sorted runs. */
  val MaxRuns: Int = 8

  /** One merge's partitioning and per-query candidate budget. The byte target
    * triggers compaction; a single answer may exceed it. Expected key counts
    * and variable-width row sizes are estimates, not heap bounds.
    */
  final case class Plan(
      partitions: Int,
      k: Int,
      layout: CandidateRuns.Layout,
      maxRuns: Int,
      targetBytes: Long,
      bytesPerQuery: Long,
      queriesPerPartition: Long
  ) {
    require(partitions > 0, s"The merge needs a partition: $partitions")
    require(k > 0, s"topK must be positive: $k")
    require(maxRuns >= 2, s"A merge batch needs at least two runs: $maxRuns")
    require(targetBytes > 0L, s"The byte target must be positive: $targetBytes")
  }

  /** The batch limits of a merge whose partition count
    * `SearchExecutionPlan.mergePartitions` fixed. Half of the task's unmanaged
    * heap share is the data budget; what one query's share of it leaves after
    * the scratch a compaction needs is split between retained runs and the
    * output, and the retained part never plans above one full batch. Spark's
    * spill streams and hash skew can retain more than two states; neither this
    * estimate nor the per-state target is a task memory limit.
    */
  def plan(
      partitions: Int,
      queries: Long,
      sources: Int,
      k: Int,
      heapBytesPerTask: Long,
      layout: CandidateRuns.Layout = CandidateRuns.Plain,
      candidateBytes: Long = CandidateBytes.Width.toLong,
      maxRuns: Int = MaxRuns
  ): Plan = {
    require(partitions > 0, s"The merge needs a partition: $partitions")
    require(queries > 0, s"A search needs at least one query: $queries")
    require(sources > 0, "Every query needs at least one candidate source")
    require(k > 0, s"A search needs a positive k: $k")
    require(
      candidateBytes > 0L,
      s"Candidate width must be positive: $candidateBytes"
    )
    require(maxRuns >= 2, s"A merge batch needs at least two runs: $maxRuns")
    val answerBytes = multiply(k.toLong, candidateBytes)
    val batchBytes = multiply(math.min(sources, maxRuns).toLong, answerBytes)
    // Conservative estimates for views, cursors, heap slots and two states'
    // containers/array headers. Carried selection can hold old and new Int
    // arrays together during growth: two arrays per generation, each <= k.
    val headBytes = multiply(96L, maxRuns.toLong)
    val selectionBytes = layout match {
      case CandidateRuns.Plain   => 0L
      case CandidateRuns.Carried => multiply(16L, k.toLong)
    }
    val scratchBytes = add(headBytes, selectionBytes)
    val metadataBytes = add(512L, multiply(64L, maxRuns.toLong))
    val temporaryBytes = add(
      multiply(2L, answerBytes),
      add(scratchBytes, metadataBytes)
    )
    val bytesPerQuery = add(
      math.max(multiply(sources.toLong, answerBytes), multiply(2L, batchBytes)),
      temporaryBytes
    )
    val dataBudget = math.max(0L, heapBytesPerTask) / 2L
    val expected = divideUp(queries, partitions.toLong)
    val perQueryBudget = dataBudget / expected
    val available = math.max(0L, perQueryBudget - temporaryBytes)
    val target = math.min(batchBytes, math.max(1L, available / 2L))
    Plan(partitions, k, layout, maxRuns, target, bytesPerQuery, expected)
  }

  private def add(left: Long, right: Long): Long =
    if (left > Long.MaxValue - right) Long.MaxValue else left + right

  private def multiply(left: Long, right: Long): Long =
    if (right != 0L && left > Long.MaxValue / right) Long.MaxValue
    else left * right

  private def divideUp(value: Long, divisor: Long): Long =
    value / divisor + (if (value % divisor == 0L) 0L else 1L)

  /** A Milvus table input's merge: every query's candidates become its global
    * top-k addresses, best first, as rows of [[HitSchema]]; with `required`, an
    * id that found nothing has a row whose hit columns are NULL.
    */
  def merged(
      spark: SparkSession,
      candidates: RDD[(Long, Array[Byte])],
      metric: MetricType,
      plan: Plan,
      required: Option[Array[Long]] = None
  ): DataFrame =
    spark.createDataFrame(
      byQuery[Row](
        spark,
        candidates,
        plan,
        metric,
        rows,
        required,
        id => Row(id, null, null)
      ),
      if (required.isEmpty) HitSchema else NullableHitSchema
    )

  /** The merge of either input: every query's answers merged into one, the rows
    * `rowsOf` makes of it, and with `required` the `absent` row of each id that
    * found nothing.
    *
    * Each task already merged its own segments, so what arrives here is one
    * packed answer per (query, task). A query retains a bounded batch of those
    * sorted runs and compacts it into its top k when a limit is reached, not an
    * aggregation over candidates. Map-side combining is off on purpose: a task
    * emits each of its queries once, so combining on the map side would hold
    * every query's answer until the task ends instead of writing each one as it
    * is packed (section 2.1).
    */
  def byQuery[A: ClassTag](
      spark: SparkSession,
      answers: RDD[(Long, Array[Byte])],
      plan: Plan,
      metric: MetricType,
      rowsOf: (Long, Array[Byte]) => Iterator[A],
      required: Option[Array[Long]],
      absent: Long => A
  ): RDD[A] = {
    val partitioner = new HashPartitioner(plan.partitions)
    val combined = answers.combineByKeyWithClassTag[CandidateRuns](
      (packed: Array[Byte]) =>
        new CandidateRuns(
          plan.k,
          metric,
          plan.layout,
          plan.maxRuns,
          plan.targetBytes
        )
          .append(packed),
      (state: CandidateRuns, packed: Array[Byte]) => state.append(packed),
      (left: CandidateRuns, right: CandidateRuns) => left.absorb(right),
      partitioner,
      mapSideCombine = false
    )
    required match {
      case None =>
        combined.flatMap { case (query, state) =>
          rowsOf(query, state.finish())
        }
      case Some(ids) =>
        // A required id that no task found anything for has no key here; the
        // partition its key would hash to says so after its own answers.
        val wanted = spark.sparkContext.broadcast(ids)
        combined.mapPartitionsWithIndex { (index, merged) =>
          val found = new java.util.HashSet[java.lang.Long]()
          merged.flatMap { case (query, state) =>
            found.add(query)
            rowsOf(query, state.finish())
          } ++ wanted.value.iterator
            .filter(id =>
              partitioner.getPartition(id) == index && !found.contains(id)
            )
            .map(absent)
        }
    }
  }

  /** Required ids with NULL base columns when no search ran. */
  def missing(
      spark: SparkSession,
      required: Option[Array[Long]],
      width: Int
  ): RDD[(Long, InternalRow)] = required match {
    case None => spark.sparkContext.emptyRDD[(Long, InternalRow)]
    case Some(ids) =>
      spark.sparkContext
        .parallelize(ids.toSeq, math.max(1, math.min(ids.length, 64)))
        .map(id => id -> (new GenericInternalRow(width): InternalRow))
  }

  /** One query's selected addresses, best first. */
  def rows(query: Long, packed: Array[Byte]): Iterator[Row] = {
    // One row at a time: at k=16,384 a query's answer is 16,384 rows, and a
    // vector of them per query is what the reduce task would otherwise hold
    // on top of the packed bytes.
    val count = CandidateBytes.count(packed)
    Iterator.tabulate(count)(at =>
      Row(
        query,
        CandidateBytes.segmentId(packed, at),
        CandidateBytes.rowOffset(packed, at)
      )
    )
  }
}

/** A selected row address associated with its query. */
private[search] case class SearchHit(
    query_id: Long,
    _segment_id: Long,
    _row_offset: Long
)
