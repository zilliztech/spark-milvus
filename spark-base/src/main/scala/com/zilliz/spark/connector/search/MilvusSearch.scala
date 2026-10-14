package com.zilliz.spark.connector.search

import scala.jdk.CollectionConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import com.zilliz.milvus.storage.expr.{FilterExpr, PredicateExpr}
import com.zilliz.milvus.storage.read.exec.SegmentIndexHandle
import com.zilliz.milvus.storage.schema.{MetricType, VectorLayout}
import com.zilliz.milvus.storage.vector.{
  CandidateBytes,
  RankingFunction,
  SearchPlan
}
import com.zilliz.spark.connector.metrics.SearchMetrics
import com.zilliz.spark.connector.options.{MilvusOption, SearchLimits}
import com.zilliz.spark.connector.read.MilvusInputPartition
import io.milvus.grpc.schema.FieldSchema

/** The two-stage search over the segments of a Milvus table input: a query set
  * in, every query's global top-k out, with the base columns the take stage
  * reads (docs/design/architecture/vector-search.html section 2.1).
  *
  * A nearest-by join over a Milvus table runs it (`NearestBySearch`); the join
  * is how a search is written (docs/design/architecture/dataframe-api.html
  * sections 2 and 4). The `exact` mode computes every distance in every
  * segment; the `index` mode searches the persisted index the snapshot pinned.
  * A hit carries `query_id`, `rank`, `_score`, `_segment_id` and `_row_offset`,
  * then the base columns asked for. A DataFrame input's search (`FrameSearch`)
  * shares its delivery of the query set (`SearchQueries`) and its merge stage
  * (`SearchMerge`).
  */
object MilvusSearch extends Logging {

  /** The two Spark stages over segments already planned: candidates from every
    * segment set, merged into each query's best k, then the output columns
    * taken for the hits alone (docs/design/architecture/vector-search.html
    * section 2.1).
    *
    * `queries` is the query set: a frame of `query_id` and `vector`, or a set
    * the driver already packed. The ids are the positions the nearest-by join
    * numbered its query rows by, so they cannot repeat and are not checked.
    * `required` names the ids that must each have a row, one with NULL hit
    * columns when nothing was found for it, which is what a LEFT OUTER
    * nearest-by join whose query rows the driver holds needs. `filter` and
    * `predicate` exclude rows before the top k: the Milvus filter, and what
    * Spark pushed into the scan whose partitions these are
    * (docs/design/architecture/dataframe-api.html section 4). An index search
    * scans exactly a segment it has no usable index for, and `metrics` is where
    * the tasks count. `function` is what the join ranks by where Knowhere does
    * not score a pair as it does (section 2).
    */
  private[connector] def execute(
      spark: SparkSession,
      options: Map[String, String],
      queries: SearchQueries.Input,
      partitions: Seq[MilvusInputPartition],
      field: FieldSchema,
      layout: VectorLayout,
      k: Int,
      searchMetric: MetricType,
      searchMode: SearchMode,
      searchParameters: Map[String, String],
      filter: Option[FilterExpr],
      predicate: Option[PredicateExpr],
      outputSchema: StructType,
      metrics: SearchMetrics,
      function: RankingFunction,
      required: Option[Array[Long]]
  ): SearchMerge.Hits = {
    val limits = SearchLimits.from(options)
    val tasks = partitions.map(_.task)
    if (searchMode == SearchMode.PreferIndex) {
      SegmentIndexHandle.check(tasks, field.fieldID, searchMetric)
    }
    val queryCount = queries.count
    val hitSchema =
      if (required.isEmpty) SearchMerge.HitSchema
      else SearchMerge.NullableHitSchema
    val resultSchema = StructType(hitSchema.fields ++ outputSchema.fields)
    // The set is what a nearest-by join kept of its query rows: rows whose
    // ranking value is NULL search nothing, and all of them may be. The
    // snapshot may hold no segment.
    if (queryCount == 0L || tasks.isEmpty)
      return SearchMerge.Hits(
        SearchMerge.missing(spark, required, resultSchema.size),
        resultSchema
      )
    require(
      queryCount <= Int.MaxValue,
      s"A search takes at most ${Int.MaxValue} queries at a time, not $queryCount"
    )

    val caseInsensitive = new CaseInsensitiveStringMap(options.asJava)
    val read = MilvusOption(caseInsensitive).readLimits
    val namedBatch =
      if (caseInsensitive.containsKey(MilvusOption.ReadBatchMaxBytes))
        Some(read.batchMaxBytes)
      else None
    val execution = SearchExecutionPlan.milvus(
      spark,
      limits,
      queries,
      tasks,
      field.fieldID,
      layout,
      k,
      searchMetric,
      searchMode,
      namedBatch,
      takeColumns = outputSchema.nonEmpty
    )
    val plan = execution.segments
    val delivery = execution.delivery
    val request = SegmentSetSearch.Request(
      vectorColumn = field.name,
      layout = layout,
      fieldId = field.fieldID,
      nullable = field.nullable,
      k = k,
      metric = searchMetric,
      mode = searchMode,
      filter = filter,
      parameters = searchParameters,
      arrowMaxBytes = read.arrowMaxBytes,
      predicate = predicate,
      function = Some(function)
    )
    // What the first stage has to get through, so its progress has a
    // denominator. An indexed search visits part of a segment and Knowhere does
    // not say how much, so index mode has no pair total and counts segment
    // searches instead.
    val comparedPairsTotal =
      if (searchMode != SearchMode.Exact) 0L
      else SearchProgress.comparedPairs(queryCount, tasks)
    logPlan(
      execution.budget,
      plan,
      request,
      execution.tasks.head,
      execution.batch,
      queries,
      delivery,
      tasks.size,
      comparedPairsTotal
    )

    // The merge and take stages run under Spark's own task slots, so a task's
    // share of the unmanaged heap is the queries' budget at that concurrency.
    val stageHeap = execution.stageHeap
    val mergeParts = execution.mergePartitions
    logInfo(
      s"Merge stage: $queryCount queries x ${plan.sets.size} answers of $k over $mergeParts partitions, " +
        s"about ${queryCount / mergeParts} queries a partition at ${plan.sets.size.toLong * k * CandidateBytes.Width} bytes each " +
        s"against ${stageHeap.bytes} bytes of heap a task"
    )
    val found = candidates(
      spark,
      partitions,
      execution,
      request,
      layout,
      metrics
    )
    SearchProgress.watch(
      spark.sparkContext,
      found,
      metrics,
      comparedPairsTotal,
      index = searchMode == SearchMode.PreferIndex,
      queryGroups = plan.groups.size,
      segments = plan.sets.map(_.size).sum
    )
    val hits =
      SearchMerge.merged(spark, found, k, searchMetric, mergeParts, required)
    if (outputSchema.isEmpty)
      SearchMerge.Hits(hits.queryExecution.toRdd, hits.schema)
    else
      take(
        hits,
        outputSchema,
        partitions,
        request.arrowMaxBytes,
        metrics,
        queryCount * k,
        stageHeap,
        execution.takePartitioning.get
      )
  }

  /** The two lines a planned search leaves in the driver log: the budgets and
    * how the plan stands against them, then the plan. A native allocation that
    * fails does not surface as a Java exception, so these lines are what there
    * is to read afterwards (search-resources.html sections 3.3, 3.4).
    */
  private def logPlan(
      budget: SearchExecutionPlan.Budget,
      plan: SearchPlan.Plan,
      request: SegmentSetSearch.Request,
      policy: SearchExecutionPlan.Task,
      batch: SearchResources.Choice,
      queries: SearchQueries.Input,
      delivery: SearchQueries.Delivery,
      segments: Int,
      comparedPairsTotal: Long
  ): Unit = {
    val needs = plan.needs
      .map(need =>
        s"queries kept needs heap=${need.queriesHeap} offheap=${need.queriesOffHeap}, " +
          s"segments kept needs offheap=${need.segmentsOffHeap}"
      )
      .getOrElse("no segments")
    val over = plan.kept.count(_.exists(_ > plan.capacity))
    logInfo(
      s"Search budget: ${budget.resources.describe}; ${budget.concurrency} tasks at once " +
        s"(${budget.slots} per executor${if (budget.profile.nonEmpty) ", each taking every core"
          else ""}); " +
        s"segments ${budget.segments.reason}; queries ${budget.queries.reason}; group ${budget.group.reason}; " +
        s"collectThreads=${policy.collectThreads} callsInFlight=${policy.callsInFlight}; batch=${batch.reason}; $needs; " +
        s"kept=${plan.resident}, sets=${plan.sets.size}" +
        (if (plan.resident == SearchPlan.Resident.Segments)
           s", capacity=${plan.capacity}, " +
             (if (over == 0) "no set known to stream"
              else s"$over sets stream once per query group")
         else "") +
        s", ${plan.estimated.size} segments estimated at half the budget"
    )
    logInfo(
      s"Search plan: mode=${request.mode}, metric=${request.metric}, topK=${request.k}, " +
        s"queries=${queries.count}, queryBytes=${SearchQueries
            .bytes(queries.count, request.layout)} " +
        s"delivered by ${delivery.describe}, " +
        s"queryGroups=${plan.groups.size}, segments=$segments in " +
        s"${plan.sets.size} sets x ${plan.queryRanges.size} query ranges = " +
        s"${plan.tasks} tasks, collectThreads=${policy.collectThreads}, " +
        s"work=${plan.groups.size.toLong * plan.sets.map(_.size.toLong).sum} segment searches" +
        (if (comparedPairsTotal > 0L)
           s" over $comparedPairsTotal distance pairs"
         else "")
    )
  }

  /** The take stage: the columns of `outputSchema` read for the hits alone.
    * What a take task buffers is every hit of its partition, `hitCount` over
    * the tasks, against the same heap share as the merge.
    */
  private def take(
      hits: DataFrame,
      outputSchema: StructType,
      partitions: Seq[MilvusInputPartition],
      arrowMaxBytes: Long,
      metrics: SearchMetrics,
      hitCount: Long,
      stageHeap: SearchResources.Choice,
      partitioning: SearchTake.Partitioning
  ): SearchMerge.Hits = {
    logInfo(
      s"Take stage: $hitCount hits over ${partitioning.partitions} tasks" +
        s" (${partitioning.buckets} bucket(s) per segment), " +
        s"about ${hitCount / partitioning.partitions} hits a task at " +
        s"${SearchTake.HitRowBytes} bytes each against ${stageHeap.bytes} bytes of heap a task"
    )
    SearchMerge.Hits(
      SearchTake.rows(
        hits,
        outputSchema,
        partitions
          .map(partition => partition.task.segmentId -> partition)
          .toMap,
        arrowMaxBytes,
        metrics,
        partitioning
      ),
      StructType(hits.schema.fields ++ outputSchema.fields)
    )
  }

  /** The candidates of the first stage: each task searches one segment set over
    * one query range, reading the groups of its range one at a time
    * (docs/design/architecture/vector-search.html section 2.1).
    */
  private def candidates(
      spark: SparkSession,
      partitions: Seq[MilvusInputPartition],
      execution: SearchExecutionPlan.Milvus,
      request: SegmentSetSearch.Request,
      layout: VectorLayout,
      metrics: SearchMetrics
  ): RDD[(Long, Array[Byte])] = {
    val plan = execution.segments
    val byId =
      partitions.map(partition => partition.task.segmentId -> partition).toMap
    val sets = plan.sets.map(_.map(task => byId(task.segmentId)))
    val groups = plan.groups
    val delivered = execution.delivery.groups(
      spark,
      groups,
      request.metric,
      layout,
      execution.packingProfile
    )
    require(
      delivered.getNumPartitions == groups.size,
      s"The packed query set is ${groups.size} groups, not " +
        s"${delivered.getNumPartitions} partitions"
    )
    // Task `index` is set `index / ranges` and range `index % ranges`, which
    // is how `SearchQueryRanges` numbers its partitions. The task learns its
    // set from that index before it reads a group, and the sets are
    // broadcast once for the executor rather than once for each task.
    val held = spark.sparkContext.broadcast((sets.toVector, execution.tasks))
    val profile = execution.budget.profile
    val ranges = plan.queryRanges
    val rangeCount = ranges.size
    val rangeSizes = ranges.map(_.size).toVector
    val searched = new SearchQueryRanges(delivered, sets.size, ranges)
      .mapPartitionsWithIndex { (index, streamed) =>
        SegmentSetSearch.run(
          held.value._1(index / rangeCount),
          request,
          held.value._2(index),
          streamed,
          rangeSizes(index % rangeCount),
          metrics
        )
      }
    profile.fold(searched)(searched.withResources)
  }

}
