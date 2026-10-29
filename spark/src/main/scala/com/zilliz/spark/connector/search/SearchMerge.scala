package com.zilliz.spark.connector.search

import java.lang.{Long => JavaLong}
import java.util.HashSet
import scala.reflect.ClassTag

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.HashPartitioner

import com.zilliz.milvus.storage.schema.MetricType
import com.zilliz.milvus.storage.vector.{
  CandidateBytes,
  CandidateDictionary,
  CandidateRuns,
  SearchPlan
}

/** The merge stage both inputs share, and the hits a search gives back. A
  * first-stage task answers each query it found anything for with one packed
  * record of that query's best k; the merge combines a query's records, one per
  * task, into its global top k by query id over an RDD shuffle. A Milvus table
  * input merges `CandidateBytes` into addresses of [[HitSchema]], which its
  * take stage reads for the base columns; a DataFrame input's candidates carry
  * their rows (`CarriedCandidates`), so its merge gives the hits whole. A
  * query's answers are retained as a bounded batch of sorted runs and compacted
  * into its top k by one k-way selection.
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

  /** Count limit before compacting the retained sorted runs, and the least the
    * plan lowers it to when the executor's memory is tight: at two, each
    * arriving answer is merged into the state at once.
    */
  val MaxRuns: Int = 8
  val MinRuns: Int = 2

  /** The default of `spark.network.maxRemoteBlockSizeFetchToMem`: Spark fetches
    * a shuffle block to memory below it and to disk above it, so a block up to
    * this size is heap the reduce task holds while it reads it. A submission
    * that lowers the setting keeps big answers off the heap, and the plan reads
    * the actual value (`SearchResources.fetchToMemoryBytes`).
    */
  val FetchToMemoryBytes: Long = 200L << 20

  /** What one query's merge holds at its peak, by where it lives. The retained
    * runs, up to `maxRuns` answers of `answerBytes` each, are the value in the
    * reduce-side map, which Spark accounts in its execution pool and spills
    * when it outgrows the task's share. The compaction's output and the answer
    * being deserialized are the task's own objects, in the heap Spark does not
    * manage, together with the cursors, heads and selection arrays. Answers are
    * counted at the width they are written: every candidate takes
    * `CandidateBytes.Width` bytes, a carried one its header and row.
    */
  final case class Footprint(
      answerBytes: Long,
      maxRuns: Int,
      stateBytes: Long,
      temporaryBytes: Long
  ) {
    def bytesPerQuery: Long = add(stateBytes, temporaryBytes)
  }

  def footprint(
      sources: Int,
      k: Int,
      layout: CandidateRuns.Layout = CandidateRuns.Plain,
      candidateBytes: Long = CandidateBytes.Width.toLong,
      maxRuns: Int = MaxRuns
  ): Footprint = {
    require(sources > 0, "Every query needs at least one candidate source")
    require(k > 0, s"A search needs a positive k: $k")
    require(
      candidateBytes > 0L,
      s"Candidate width must be positive: $candidateBytes"
    )
    require(
      maxRuns >= MinRuns,
      s"A merge batch needs at least $MinRuns runs: $maxRuns"
    )
    val answerBytes = multiply(k.toLong, candidateBytes)
    val stateBytes = multiply(math.min(sources, maxRuns).toLong, answerBytes)
    // Views, cursors and heap slots of a compaction, and the containers' and
    // array headers of two states. Carried selection can hold old and new Int
    // arrays together during growth: two arrays per generation, each <= k.
    val headBytes = multiply(96L, maxRuns.toLong)
    val selectionBytes = layout match {
      case CandidateRuns.Plain   => 0L
      case CandidateRuns.Carried => multiply(16L, k.toLong)
    }
    val metadataBytes = add(512L, multiply(64L, maxRuns.toLong))
    val temporaryBytes = add(
      multiply(2L, answerBytes),
      add(add(headBytes, selectionBytes), metadataBytes)
    )
    Footprint(answerBytes, maxRuns, stateBytes, temporaryBytes)
  }

  /** The heap a shuffle block of `queries` answers takes while it is read: the
    * block itself up to `fetchToMemoryBytes`, beyond which Spark spools it to
    * disk and the task holds only what it deserializes.
    */
  def blockBytes(
      queries: Long,
      answerBytes: Long,
      fetchToMemoryBytes: Long = FetchToMemoryBytes
  ): Long =
    math.min(multiply(queries, answerBytes), math.max(0L, fetchToMemoryBytes))

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
    require(
      maxRuns >= MinRuns,
      s"A merge batch needs at least $MinRuns runs: $maxRuns"
    )
    require(targetBytes > 0L, s"The byte target must be positive: $targetBytes")
  }

  /** The batch limits of a merge whose partition count and runs
    * `SearchExecutionPlan.mergeStage` fixed. `stateBudgetBytes` is the
    * execution pool share of one task; the queries a partition is expected to
    * hold divide it, and a query's retained runs are compacted once they reach
    * that share or one full batch, whichever is less. Hash skew can retain more
    * than the expected queries; neither the estimate nor the target is a task
    * memory limit. The footprint uses the merge's candidate capacity `k`,
    * including candidates accumulated from multiple sources.
    */
  def plan(
      partitions: Int,
      queries: Long,
      sources: Int,
      k: Int,
      stateBudgetBytes: Long,
      layout: CandidateRuns.Layout = CandidateRuns.Plain,
      candidateBytes: Long = CandidateBytes.Width.toLong,
      maxRuns: Int = MaxRuns
  ): Plan = {
    require(partitions > 0, s"The merge needs a partition: $partitions")
    require(queries > 0, s"A search needs at least one query: $queries")
    val held = footprint(sources, k, layout, candidateBytes, maxRuns)
    val expected = divideUp(queries, partitions.toLong)
    val perQueryBudget = math.max(0L, stateBudgetBytes) / expected
    val target = math.min(held.stateBytes, math.max(1L, perQueryBudget))
    Plan(
      partitions,
      k,
      layout,
      maxRuns,
      target,
      held.bytesPerQuery,
      expected
    )
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
    * id that found nothing has a row whose hit columns are NULL. `profile` is
    * the task resources the merge stage declares, when the plan lowered its
    * concurrency below the default task slots.
    */
  def merged(
      spark: SparkSession,
      candidates: RDD[(Long, Array[Byte])],
      metric: MetricType,
      plan: Plan,
      dictionary: Broadcast[CandidateDictionary],
      required: Option[Array[Long]] = None,
      profile: Option[ResourceProfile] = None
  ): DataFrame = {
    require(
      plan.layout == CandidateRuns.Plain,
      "A Milvus merge needs address candidates"
    )
    spark.createDataFrame(
      byQuery[Row](
        spark,
        candidates,
        plan,
        metric,
        (query, packed) => rows(query, packed, dictionary.value),
        required,
        id => Row(id, null, null),
        profile
      ),
      if (required.isEmpty) HitSchema else NullableHitSchema
    )
  }

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
    * is packed. With `profile` the reduce stage declares the cores each of its
    * tasks takes, which is how the plan runs fewer of them at once on an
    * executor when one query's state needs more heap than a default slot has.
    */
  def byQuery[A: ClassTag](
      spark: SparkSession,
      answers: RDD[(Long, Array[Byte])],
      plan: Plan,
      metric: MetricType,
      rowsOf: (Long, Array[Byte]) => Iterator[A],
      required: Option[Array[Long]],
      absent: Long => A,
      profile: Option[ResourceProfile] = None
  ): RDD[A] = {
    val partitioner = new HashPartitioner(plan.partitions)
    val reduced = answers.combineByKeyWithClassTag[CandidateRuns](
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
    val combined = profile.fold(reduced)(reduced.withResources)
    required match {
      case None =>
        combined.flatMap { case (query, state) =>
          rowsOf(query, state.finish())
        }
      case Some(ids) =>
        // A required id that no task found anything for has no key here; the
        // partition its key would hash to says so after its own answers.
        val wanted = spark.sparkContext.broadcast(requiredIds(ids, partitioner))
        combined.mapPartitionsWithIndex { (index, merged) =>
          val found = new HashSet[JavaLong]()
          merged.flatMap { case (query, state) =>
            found.add(query)
            rowsOf(query, state.finish())
          } ++ wanted.value
            .missing(index, partitioner, found)
            .map(absent)
        }
    }
  }

  /** Required ids in their original order, or stably grouped by merge
    * partition. Empty ends keep the original filtering path. The bucketed
    * representation adds one Int per partition to the ids already broadcast.
    */
  private[search] final class RequiredIds(
      val ids: Array[Long],
      val ends: Array[Int]
  ) extends Serializable {
    def missing(
        index: Int,
        partitioner: HashPartitioner,
        found: HashSet[JavaLong]
    ): Iterator[Long] =
      if (ends.isEmpty)
        ids.iterator.filter(id =>
          partitioner.getPartition(id) == index && !found.contains(id)
        )
      else {
        val start = if (index == 0) 0 else ends(index - 1)
        new RequiredIdCursor(ids, start, ends(index), found)
      }
  }

  private final class RequiredIdCursor(
      ids: Array[Long],
      start: Int,
      end: Int,
      found: HashSet[JavaLong]
  ) extends Iterator[Long] {
    private var at = start
    private var ready = false
    private var value = 0L

    override def hasNext: Boolean = {
      while (!ready && at < end) {
        val id = ids(at)
        at += 1
        if (!found.contains(id)) {
          value = id
          ready = true
        }
      }
      ready
    }

    override def next(): Long = {
      if (!hasNext) throw new NoSuchElementException("next on empty iterator")
      ready = false
      value
    }
  }

  private[search] def requiredIds(
      ids: Array[Long],
      partitioner: HashPartitioner
  ): RequiredIds = {
    val partitions = partitioner.numPartitions
    // Two passes over the ids prepare the buckets. Keep small partition counts
    // on the original path, and bound the added offsets by the id count.
    if (partitions <= 2 || ids.length < partitions)
      return new RequiredIds(ids, Array.empty[Int])
    val ends = new Array[Int](partitions)
    var at = 0
    while (at < ids.length) {
      ends(partitioner.getPartition(ids(at))) += 1
      at += 1
    }
    var partition = 0
    var total = 0
    while (partition < partitions) {
      total += ends(partition)
      ends(partition) = total
      partition += 1
    }
    val positions = ends.clone()
    val grouped = new Array[Long](ids.length)
    at = ids.length - 1
    while (at >= 0) {
      val id = ids(at)
      partition = partitioner.getPartition(id)
      positions(partition) -= 1
      grouped(positions(partition)) = id
      at -= 1
    }
    new RequiredIds(grouped, ends)
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
  def rows(
      query: Long,
      packed: Array[Byte],
      dictionary: CandidateDictionary
  ): Iterator[Row] = {
    // One row at a time: at k=16,384 a query's answer is 16,384 rows, and a
    // vector of them per query is what the reduce task would otherwise hold
    // on top of the packed bytes.
    val count = CandidateBytes.count(packed)
    Iterator.tabulate(count)(at =>
      Row(
        query,
        CandidateBytes.segmentId(packed, at, dictionary),
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
