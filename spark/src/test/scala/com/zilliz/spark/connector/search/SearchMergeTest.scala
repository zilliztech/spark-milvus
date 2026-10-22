package com.zilliz.spark.connector.search

import java.lang.{Long => JavaLong}
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import java.util.HashSet
import scala.collection.concurrent.TrieMap

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.scheduler.{
  SparkListener,
  SparkListenerJobEnd,
  SparkListenerJobStart
}
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer, Serializer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.HashPartitioner
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

import com.zilliz.milvus.storage.read.plan.SegmentReadTask
import com.zilliz.milvus.storage.schema.MetricType
import com.zilliz.milvus.storage.snapshot.SegmentLayout
import com.zilliz.milvus.storage.vector.{
  Candidate,
  CandidateBytes,
  CandidateBytesFixture,
  CandidateDictionary,
  CandidateEncoding,
  CandidateRuns,
  CarriedCandidates,
  TopKMerger
}
import com.zilliz.spark.connector.extensions.ConnectorVectorRanking
import com.zilliz.spark.connector.metrics.SearchMetrics
import com.zilliz.spark.connector.options.TaskResources

/** The merge stage inside Spark: candidates from several tasks become each
  * query's selected addresses, best first.
  */
class SearchMergeTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private val packedDictionary = CandidateDictionary(0L until 72L)
  private val encoding = CandidateEncoding(packedDictionary, 0L until 72L)
  private var packedBroadcast: Broadcast[CandidateDictionary] = _
  private val layouts: Seq[CandidateRuns.Layout] =
    Seq(CandidateRuns.Plain, CandidateRuns.Carried)

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("search-merge")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    packedBroadcast = spark.sparkContext.broadcast(packedDictionary)
  }

  override def afterAll(): Unit = {
    if (packedBroadcast != null) packedBroadcast.destroy()
    if (spark != null) spark.stop()
  }

  /** What the first stage sends on: one packed answer per (query, task). The
    * candidates of one query that came from one task travel together, already
    * merged and sorted there, which is what the merge stage takes apart.
    */
  private def candidates(
      rows: Seq[(Long, Long, Long, Float)],
      metric: MetricType = MetricType.L2,
      encoding: CandidateEncoding = this.encoding
  ) =
    spark.sparkContext
      .parallelize(
        rows
          .groupBy { case (query, segment, _, _) => (query, segment) }
          .toSeq
          .map { case ((query, _), grouped) =>
            query -> CandidateBytesFixture.pack(
              grouped.map { case (_, segment, offset, score) =>
                Candidate(0, segment, offset, score)
              },
              metric,
              encoding
            )
          },
        2
      )

  private def merged(
      rows: Seq[(Long, Long, Long, Float)],
      k: Int,
      metric: MetricType
  ) =
    SearchMerge.merged(
      spark,
      candidates(rows, metric),
      metric,
      plan(math.max(1, rows.map(_._1).distinct.size.toLong), k),
      packedBroadcast
    )

  /** The merge plan of two first-stage tasks over two candidate sources. */
  private def plan(queries: Long, k: Int, heap: Long = 1L << 30) = {
    val perPartition = math
      .max(1L, math.min(4096L, heap / 2L / (2L * k * CandidateBytes.Width)))
      .toInt
    SearchMerge.plan(
      SearchExecutionPlan.mergePartitions(
        spark.sparkContext.defaultParallelism,
        queries,
        2,
        perPartition
      ),
      queries,
      2,
      k,
      heap
    )
  }

  private def completed(
      answers: Seq[(Long, Array[Byte])],
      partitions: Int,
      required: Option[Array[Long]]
  ) = {
    val dictionary = packedBroadcast
    SearchMerge.byQuery[(Long, Option[Int])](
      spark,
      spark.sparkContext.parallelize(answers, 2),
      SearchMerge.plan(
        partitions,
        math.max(1L, answers.map(_._1).distinct.size.toLong),
        2,
        16,
        1L << 30
      ),
      MetricType.L2,
      (query, packed) =>
        Iterator.tabulate(CandidateBytes.count(packed)) { at =>
          query -> Some(
            CandidateBytes
              .segmentId(packed, at, dictionary.value)
              .toInt
          )
        },
      required,
      query => query -> None
    )
  }

  private def completedAnswer(value: Int): Array[Byte] =
    CandidateBytesFixture.pack(
      Seq(Candidate(0, value.toLong, 0L, value.toFloat)),
      MetricType.L2,
      encoding
    )

  test(
    "fixed candidates recover signed segment ids and unsigned offsets after shuffle"
  ) {
    val rows = Seq(
      (1L, Long.MinValue, 0xffffffffL, 2.0f),
      (1L, -1L, 0x80000000L, 1.0f),
      (1L, Long.MaxValue, 7L, 1.0f),
      (2L, Long.MaxValue, 8L, 0.0f),
      (2L, -1L, 0xffffffffL, 0.0f)
    )
    val ids = rows.map(_._2).distinct
    val global = CandidateDictionary(ids)
    val expected = Map(
      1L -> Vector(-1L -> 0x80000000L, Long.MaxValue -> 7L),
      2L -> Vector(-1L -> 0xffffffffL, Long.MaxValue -> 8L)
    )
    val broadcast = spark.sparkContext.broadcast(global)
    val encoding = CandidateEncoding(global, ids)
    val mergePlan = SearchMerge.plan(2, 2L, 3, 2, 1L << 30)
    try {
      val result = SearchMerge
        .merged(
          spark,
          candidates(rows, MetricType.L2, encoding),
          MetricType.L2,
          mergePlan,
          broadcast
        )
        .collect()
        .toVector
        .groupBy(_.getLong(0))
        .map { case (query, hits) =>
          query -> hits.map(row => row.getLong(1) -> row.getLong(2))
        }
      result shouldBe expected
    } finally broadcast.destroy()
  }

  test("the merge budget counts candidates at their fixed wire width") {
    val baseline = SearchMerge.plan(17, 100L, 4, 100000, 1L << 30)
    CandidateBytes.Width shouldBe 12
    val planned = SearchMerge.plan(
      17,
      100L,
      4,
      100000,
      1L << 30,
      CandidateRuns.Plain,
      candidateBytes = CandidateBytes.Width.toLong
    )
    planned shouldBe baseline
    SearchMerge.footprint(4, 100000).answerBytes shouldBe 100000L * 12L
    SearchMerge
      .plan(17, 100L, 4, 100000, 1L << 30, CandidateRuns.Plain, 24L)
      .bytesPerQuery should be > baseline.bytesPerQuery
  }

  test("Float L2 ties select the same addresses before and after shuffle") {
    val ranking = ConnectorVectorRanking(
      MetricType.L2,
      "vector_l2_distance",
      queryFirst = true
    )
    // The first address has the larger mathematical square in the first two
    // cases. Rounding before local top-k makes it the deterministic tie winner.
    val distances = Seq(
      Seq(3e20f, 2e20f, 2.5e20f),
      Seq(2e-30f, 1e-30f, 1.5e-30f),
      Seq(-0.0f, 0.0f, -0.0f),
      Seq(Float.NaN, Float.NaN, Float.NaN)
    )
    val scenarios = distances.zipWithIndex.map { case (values, query) =>
      query.toLong -> values.zipWithIndex.map { case (distance, address) =>
        Candidate(
          0,
          address.toLong,
          0L,
          ranking.score(Array(0f), Array(distance)).floatValue()
        )
      }
    }
    for {
      layout <- layouts
      groupSize <- Seq(1, 2, 3)
    } {
      val source = scenarios.flatMap { case (query, entries) =>
        entries.reverse.grouped(groupSize).map { group =>
          val merger = new TopKMerger(1, 1, MetricType.L2)
          group.foreach(merger.add)
          merger.sort(0)
          val bytes = layout match {
            case CandidateRuns.Plain =>
              merger.takePacked(
                0,
                CandidateEncoding(packedDictionary, group.map(_.segmentId))
              )
            case CandidateRuns.Carried => packed(layout, merger.results(0))
          }
          query -> bytes
        }
      }
      val expected = scenarios.map { case (query, entries) =>
        query -> packed(layout, entries.take(1)).toVector
      }.toMap
      val result = SearchMerge
        .byQuery[(Long, Vector[Byte])](
          spark,
          spark.sparkContext.parallelize(source, 3),
          SearchMerge.plan(2, scenarios.size.toLong, 3, 1, 1L << 30, layout),
          MetricType.L2,
          (query, bytes) => Iterator.single(query -> bytes.toVector),
          None,
          _ => throw new AssertionError("no query needs completion")
        )
        .collect()
        .toMap
      withClue(s"layout=$layout groupSize=$groupSize: ") {
        result shouldBe expected
      }
    }
  }

  test("required ids use buckets only above two partitions with enough ids") {
    Seq(1, 2, 3, 8).foreach { partitions =>
      Seq(0, partitions - 1, partitions, partitions + 1).foreach { count =>
        val ids = Array.tabulate(count)(_.toLong)
        val original = ids.toVector
        val prepared =
          SearchMerge.requiredIds(ids, new HashPartitioner(partitions))
        val bucketed = partitions > 2 && count >= partitions

        prepared.ends.length shouldBe (if (bucketed) partitions else 0)
        prepared.ids.length shouldBe count
        (prepared.ids eq ids) shouldBe !bucketed
        ids.toVector shouldBe original
        if (bucketed) prepared.ends.last shouldBe count
      }
    }
    // The small-Q path needs no offsets even when P+1 would overflow Int.
    SearchMerge
      .requiredIds(Array(1L), new HashPartitioner(Int.MaxValue))
      .ends shouldBe empty
  }

  test("required buckets preserve id order, duplicates and Spark's Long hash") {
    val ids = Array(
      0L,
      -1L,
      1L,
      Long.MinValue,
      1L << 32,
      0x100000001L,
      Long.MaxValue,
      -2L,
      -1L,
      17L,
      0x100000001L,
      0L,
      -17L,
      8L
    )
    Seq(1, 2, 3, 7, 11, 16).foreach { partitions =>
      val partitioner = new HashPartitioner(partitions)
      val prepared = SearchMerge.requiredIds(ids, partitioner)
      Seq(false, true).foreach { hasFound =>
        val found = new HashSet[JavaLong]()
        if (hasFound) {
          found.add(0L)
          found.add(Long.MinValue)
          found.add(17L)
        }
        (0 until partitions).foreach { index =>
          val expected = ids.iterator
            .filter(id =>
              partitioner.getPartition(id) == index && !found.contains(id)
            )
            .toVector
          prepared.missing(index, partitioner, found).toVector shouldBe expected
          prepared.missing(index, partitioner, found).toVector shouldBe expected
        }
      }
    }
  }

  test("required-id lookahead stops at its bucket and stays exhausted") {
    val ids = Array(0L, 3L, 6L, 9L, 1L, 4L, 7L, 10L)
    val partitioner = new HashPartitioner(3)
    val prepared = SearchMerge.requiredIds(ids, partitioner)
    val original = prepared.ids.toVector
    val found = new HashSet[JavaLong]()
    found.add(0L)
    found.add(6L)
    found.add(7L)

    val first = prepared.missing(0, partitioner, found)
    first.hasNext shouldBe true
    first.hasNext shouldBe true
    first.next() shouldBe 3L
    first.next() shouldBe 9L
    first.hasNext shouldBe false
    first.hasNext shouldBe false
    the[NoSuchElementException] thrownBy first.next()

    prepared.missing(1, partitioner, found).toVector shouldBe Vector(
      1L,
      4L,
      10L
    )
    val empty = prepared.missing(2, partitioner, found)
    empty.hasNext shouldBe false
    the[NoSuchElementException] thrownBy empty.next()

    val allFound = new HashSet[JavaLong]()
    ids.foreach(id => allFound.add(id))
    val skipped = prepared.missing(0, partitioner, allFound)
    skipped.hasNext shouldBe false
    the[NoSuchElementException] thrownBy skipped.next()
    prepared.missing(0, partitioner, found).toVector shouldBe Vector(3L, 9L)
    prepared.ids.toVector shouldBe original
  }

  test(
    "required ids complete by found key on both paths and can be read again"
  ) {
    val ids = Array(
      0L,
      -1L,
      7L,
      -1L,
      0x100000001L,
      Long.MinValue,
      Long.MaxValue,
      8L,
      9L,
      9L,
      1L,
      1L << 32
    )
    val answers = Seq(
      0L -> completedAnswer(2),
      0L -> completedAnswer(3),
      // A key is found even when rowsOf produces no output for it.
      -1L -> Array.emptyByteArray,
      0x100000001L -> completedAnswer(4),
      // Found keys outside required still keep their rows.
      99L -> completedAnswer(5)
    )
    val found = Set(0L, -1L, 0x100000001L, 99L)
    val expected = Vector(
      0L -> Some(2),
      0L -> Some(3),
      0x100000001L -> Some(4),
      99L -> Some(5)
    ) ++ ids.iterator.filterNot(found).map(id => id -> None).toVector

    Seq(1, 2, 3, 5, 16).foreach { partitions =>
      val result = completed(answers, partitions, Some(ids))
      // The output type is arbitrary; the input's key partitioner is not its
      // downstream partitioner, even when this test happens to return pairs.
      result.partitioner shouldBe None
      result.collect().toVector should contain theSameElementsAs expected
      result.collect().toVector should contain theSameElementsAs expected
    }
  }

  test("None and empty required ids add no rows on either merge path") {
    val answers = Seq(4L -> completedAnswer(2), 9L -> Array.emptyByteArray)
    Seq(1, 3).foreach { partitions =>
      Seq(None, Some(Array.empty[Long])).foreach { required =>
        completed(answers, partitions, required).collect().toVector shouldBe
          Vector(4L -> Some(2))
        completed(Seq.empty, partitions, required).collect() shouldBe empty
      }
    }
  }

  test("outer completion keeps duplicate missing ids and NULL hit columns") {
    val ids = Array(1L, 2L, 2L, -1L, Long.MinValue, Long.MaxValue, 0x100000001L)
    val hits = candidates(Seq((1L, 10L, 3L, 1.0f)))
    Seq(2, 3).foreach { partitions =>
      val mergePlan =
        SearchMerge.plan(partitions, ids.length.toLong, 2, 1, 1L << 30)
      val result =
        SearchMerge.merged(
          spark,
          hits,
          MetricType.L2,
          mergePlan,
          packedBroadcast,
          Some(ids)
        )
      result.schema shouldBe SearchMerge.NullableHitSchema
      val rows = result.collect().toVector
      rows.map(_.getLong(0)) should contain theSameElementsAs ids.toVector
      rows
        .filter(_.getLong(0) == 1L)
        .map(row => row.getLong(1) -> row.getLong(2)) shouldBe Vector(10L -> 3L)
      rows.filter(_.getLong(0) != 1L).foreach { row =>
        row.isNullAt(1) shouldBe true
        row.isNullAt(2) shouldBe true
      }
      val empty = SearchMerge
        .merged(
          spark,
          candidates(Seq.empty),
          MetricType.L2,
          mergePlan,
          packedBroadcast,
          Some(ids)
        )
        .collect()
        .toVector
      empty.map(_.getLong(0)) should contain theSameElementsAs ids.toVector
      empty.foreach { row =>
        row.isNullAt(1) shouldBe true
        row.isNullAt(2) shouldBe true
      }
    }
  }

  test("each query keeps its own best k addresses, best first") {
    val merged = this.merged(
      Seq(
        (1L, 10L, 0L, 5.0f),
        (1L, 11L, 3L, 1.0f),
        (1L, 11L, 4L, 3.0f),
        (2L, 10L, 7L, 9.0f),
        (2L, 11L, 8L, 2.0f)
      ),
      2,
      MetricType.L2
    )

    merged.schema.fieldNames shouldBe Array(
      "query_id",
      "_segment_id",
      "_row_offset"
    )
    val rows = merged
      .collect()
      .groupBy(_.getAs[Long]("query_id"))

    rows.keySet shouldBe Set(1L, 2L)
    rows(1L).map(row => row.getLong(1) -> row.getLong(2)).toSeq shouldBe
      Seq(11L -> 3L, 11L -> 4L)
    rows(2L).map(row => row.getLong(1) -> row.getLong(2)).toSeq shouldBe
      Seq(11L -> 8L, 10L -> 7L)
  }

  test("a larger score wins under COSINE") {
    val rows = merged(
      Seq((1L, 10L, 0L, 0.2f), (1L, 10L, 1L, 0.9f), (1L, 11L, 2L, 0.5f)),
      2,
      MetricType.Cosine
    ).collect()
      .map(row => row.getLong(1) -> row.getLong(2))
      .toSeq

    rows shouldBe Seq(10L -> 1L, 11L -> 2L)
  }

  test("a query with fewer candidates than k keeps them all") {
    val rows = merged(Seq((7L, 10L, 0L, 1.5f)), 10, MetricType.L2).collect()

    rows.map(_.getAs[Long]("query_id")).toSeq shouldBe Seq(7L)
    rows.map(row => row.getLong(1) -> row.getLong(2)).toSeq shouldBe
      Seq(10L -> 0L)
  }

  test("a local master's tasks all share one JVM's memory") {
    // local[2] in beforeAll: one executor, this JVM, running two tasks at
    // once, so an executor budget is halved before a task plans against it;
    // and a local master cannot declare a stage's task cores.
    val resources = TaskResources.of(spark)
    resources.executors shouldBe 1
    resources.tasksPerExecutor shouldBe 2
    resources.declarable shouldBe false
    resources.wholeExecutor shouldBe None
  }

  test("a progress count reads at a glance") {
    SearchProgress.grouped(0L) shouldBe "0"
    SearchProgress.grouped(999L) shouldBe "999"
    SearchProgress.grouped(10000L) shouldBe "10,000"
    SearchProgress.grouped(12060426240L) shouldBe "12,060,426,240"
    SearchProgress.grouped(53687091200L) shouldBe "53,687,091,200"
  }

  test("progress reads its own search's counters by id, an unset one as none") {
    SearchProgress.searchValues(
      Seq(
        7L -> Some(java.lang.Long.valueOf(7L)),
        // Another search's counter, under the same name.
        9L -> Some(java.lang.Long.valueOf(40L)),
        // A nearest-by node's size or duration metric no task added to.
        8L -> Some(java.lang.Long.valueOf(-1L)),
        // One of Spark's own task metrics.
        1L -> Some(java.lang.Long.valueOf(5L)),
        6L -> Some("not a number")
      ),
      Map(
        7L -> "milvus.search.compared.pairs",
        8L -> "milvus.search.read.bytes",
        6L -> "milvus.search.candidates"
      )
    ) shouldBe Map("milvus.search.compared.pairs" -> 7L)
  }

  test("progress counts its search's tasks and leaves when the job ends") {
    val context = spark.sparkContext
    val ours = SearchMetrics.of(SearchMetrics.sqlMetrics(context))
    val other = SearchMetrics.of(SearchMetrics.sqlMetrics(context))
    val firstStage = context.parallelize(1 to 4, 2).map { row =>
      ours.comparedPairs.add(1L)
      other.comparedPairs.add(10L)
      row
    }
    val later = context.parallelize(1 to 4, 2).map { row =>
      ours.comparedPairs.add(1L)
      row
    }
    // Says which of the two RDDs a job that ended ran. Added before the
    // search's listener, it is on the same queue, so when it hears of an end,
    // that listener has had every event before it.
    val ended = new LinkedBlockingQueue[Int]()
    val ends = new SparkListener {
      private val jobs = TrieMap.empty[Int, Int]
      override def onJobStart(event: SparkListenerJobStart): Unit =
        event.stageInfos
          .flatMap(_.rddInfos)
          .map(_.id)
          .find(Set(firstStage.id, later.id))
          .foreach(jobs.put(event.jobId, _))
      override def onJobEnd(event: SparkListenerJobEnd): Unit =
        jobs.remove(event.jobId).foreach(ended.put)
    }
    context.addSparkListener(ends)
    try {
      val progress = SearchProgress.watch(
        context,
        firstStage,
        ours,
        comparedPairsTotal = 4L,
        index = false,
        queryGroups = 1,
        segments = 1
      )

      firstStage.count()
      ended.poll(60L, TimeUnit.SECONDS) shouldBe firstStage.id
      progress.totals shouldBe Map(SearchMetrics.ComparedPairs -> 4L)

      later.count()
      ended.poll(60L, TimeUnit.SECONDS) shouldBe later.id
      progress.totals shouldBe Map(SearchMetrics.ComparedPairs -> 4L)
    } finally context.removeSparkListener(ends)
  }

  test("compared pairs have a total only when every row count is known") {
    def task(rows: Option[Long]): SegmentReadTask = SegmentReadTask(
      segmentId = 1L,
      partitionId = 1L,
      layout = SegmentLayout.Manifest("segments/1", 3L),
      schemaBytes = Array.emptyByteArray,
      properties = Map.empty,
      snapshotRows = rows
    )

    val known = Seq(task(Some(10L)), task(Some(6L)))

    SearchProgress.comparedPairs(3L, known) shouldBe 48L
    SearchProgress.comparedPairs(3L, known :+ task(None)) shouldBe 0L
  }

  test("a search counts under the names the design gives its metrics") {
    val metrics = SearchMetrics.of(SearchMetrics.sqlMetrics(spark.sparkContext))

    metrics.all.map(_._1) shouldBe Seq(
      "milvus.search.segment.searches",
      "milvus.search.read.bytes",
      "milvus.search.read.nanos",
      "milvus.search.index.bytes",
      "milvus.search.index.load.nanos",
      "milvus.search.bitmap.nanos",
      "milvus.search.knowhere.calls",
      "milvus.search.knowhere.nanos",
      "milvus.search.compared.pairs",
      "milvus.search.exact.rescans",
      "milvus.search.candidates",
      "milvus.search.take.rows",
      "milvus.search.take.nanos"
    )
    metrics.all.foreach { case (name, counter) =>
      counter.name shouldBe Some(name)
    }

    spark.sparkContext
      .parallelize(Seq(3L, 4L), 2)
      .foreach(metrics.candidates.add)

    metrics.candidates.value shouldBe 7L
    metrics.summary should include("milvus.search.candidates=7")
  }

  test("each node that runs a search counts on its own metrics") {
    val first = SearchMetrics.of(SearchMetrics.sqlMetrics(spark.sparkContext))
    val second = SearchMetrics.of(SearchMetrics.sqlMetrics(spark.sparkContext))

    first.segmentSearches.add(2L)

    second.segmentSearches.value shouldBe 0L
  }

  test("no candidates give no rows") {
    SearchMerge
      .merged(
        spark,
        candidates(Seq.empty),
        MetricType.L2,
        plan(2L, 3),
        packedBroadcast
      )
      .collect() shouldBe empty
  }

  test("an empty result has the schema a merged one has") {
    SearchMerge.HitSchema shouldBe SearchMerge
      .merged(
        spark,
        candidates(Seq((1L, 10L, 0L, 1.0f))),
        MetricType.L2,
        plan(1L, 1),
        packedBroadcast
      )
      .schema
  }

  test(
    "the merge has a partition for every slot, task and few thousand queries"
  ) {
    // local[2]: two slots. A partition cannot hold part of a query, so one
    // query is one partition however wide the cluster is.
    val slots = spark.sparkContext.defaultParallelism
    slots shouldBe 2
    SearchExecutionPlan.mergePartitions(slots, 1L, 1, 4096) shouldBe 1
    SearchExecutionPlan.mergePartitions(slots, 100L, 1, 4096) shouldBe 2
    // More first-stage tasks than slots: every task's output still lands in a
    // partition of its own, which is what a cluster of many executors needs.
    SearchExecutionPlan.mergePartitions(slots, 100L, 16, 4096) shouldBe 16
    // A query count far above the parallelism: partitions grow with it so a
    // reduce-side map holds a few thousand queries, not all of them.
    SearchExecutionPlan.mergePartitions(slots, 1000000L, 6, 4096) shouldBe 245
    the[IllegalArgumentException] thrownBy SearchExecutionPlan
      .mergePartitions(slots, 0L, 1, 4096)
    the[IllegalArgumentException] thrownBy SearchExecutionPlan
      .mergePartitions(slots, 1L, 1, 0)
  }

  test("a large k cuts the merge into partitions of the queries a task holds") {
    // 100k queries, 15 first-stage tasks, 26 queries a partition (what the
    // stage planner found a task's shares hold at k=16,384) -> 3,847
    // partitions. The run that died had 64.
    val slots = spark.sparkContext.defaultParallelism
    SearchExecutionPlan.mergePartitions(slots, 100000L, 15, 26) shouldBe 3847
    // A small k stays at the fixed few thousand queries a partition: 100k /
    // 4096 = 25.
    SearchExecutionPlan.mergePartitions(slots, 100000L, 15, 4096) shouldBe 25
  }
  test(
    "query grouping changes task parallelism without multiplying candidate fan-in"
  ) {
    val slots = spark.sparkContext.defaultParallelism
    SearchExecutionPlan.mergePartitions(slots, 100000L, 1500, 26) shouldBe
      SearchExecutionPlan.mergePartitions(slots, 100000L, 15, 26)
  }

  test("a merge plan budgets its batch from the partitions it was given") {
    for {
      queries <- Seq(1L, 100L, 100000L)
      sources <- Seq(1, 8, 128)
      k <- Seq(1, 100, 16384)
      heap <- Seq(0L, 1L << 20, 1L << 30)
    } {
      val partitions =
        SearchExecutionPlan.mergePartitions(
          spark.sparkContext.defaultParallelism,
          queries,
          sources,
          4096
        )
      val plain = SearchMerge.plan(partitions, queries, sources, k, heap)
      val carried = SearchMerge.plan(
        partitions,
        queries,
        sources,
        k,
        heap,
        CandidateRuns.Carried,
        1052L
      )
      plain.partitions shouldBe partitions
      plain.queriesPerPartition shouldBe (queries + partitions - 1) / partitions
      plain.bytesPerQuery shouldBe SearchMerge
        .footprint(sources, k)
        .bytesPerQuery
      plain.bytesPerQuery should be >= math.min(sources, 8).toLong * k * 12L
      carried.bytesPerQuery should be > plain.bytesPerQuery
      plain.targetBytes should be > 0L
      plain.targetBytes should be <= math
        .min(sources, plain.maxRuns)
        .toLong * k * 12L
      if (heap == 0L) plain.targetBytes shouldBe 1L
    }
    // 15 sources at k=16,384 over 3,847 partitions: 26 queries a partition
    // share a 306 MB execution share, 11.8 MB each, more than the 1.6 MB of a
    // full batch of eight answers, so the batch is the compaction target.
    val heap = 306393907L
    val answer = 16384L * 12L
    val planned = SearchMerge.plan(3847, 100000L, 15, 16384, heap)
    planned.queriesPerPartition shouldBe 26L
    planned.targetBytes shouldBe 8L * answer
    // A share too small for a batch compacts earlier: 26 queries in 3 MB.
    SearchMerge.plan(3847, 100000L, 15, 16384, 3L << 20).targetBytes shouldBe
      (3L << 20) / 26L
  }

  test(
    "merge budget arithmetic saturates without wrapping or losing partitions"
  ) {
    val plan = SearchMerge.plan(
      100,
      100L,
      Int.MaxValue,
      Int.MaxValue,
      Long.MaxValue,
      CandidateRuns.Carried,
      Long.MaxValue
    )
    plan.partitions shouldBe 100
    plan.bytesPerQuery shouldBe Long.MaxValue
    plan.targetBytes shouldBe Long.MaxValue
    the[IllegalArgumentException] thrownBy SearchMerge.plan(0, 1L, 1, 1, 0L)
    the[IllegalArgumentException] thrownBy SearchMerge.plan(1, 0L, 1, 1, 0L)
    the[IllegalArgumentException] thrownBy SearchMerge
      .plan(1, 1L, 1, 1, 1L, candidateBytes = 0L)
    the[IllegalArgumentException] thrownBy SearchMerge
      .plan(1, 1L, 1, 1, 1L, maxRuns = 1)
  }

  private def packed(
      layout: CandidateRuns.Layout,
      entries: Seq[Candidate]
  ): Array[Byte] = layout match {
    case CandidateRuns.Plain =>
      CandidateBytesFixture.pack(
        entries,
        MetricType.L2,
        CandidateEncoding(packedDictionary, entries.map(_.segmentId))
      )
    case CandidateRuns.Carried =>
      CarriedCandidates.writeSorted(
        entries.sorted(Candidate.ranking(MetricType.L2)).map { candidate =>
          val bytes = Array.fill[Byte](1024 + candidate.rowOffset.toInt)(
            (candidate.segmentId + candidate.rowOffset).toByte
          )
          candidate -> bytes
        }
      )
  }

  test(
    "Java and Kryo preserve plain and carried candidates across serialization"
  ) {
    val conf = spark.sparkContext.getConf
    val serializers: Seq[Serializer] =
      Seq(new JavaSerializer(conf), new KryoSerializer(conf))
    val entries =
      (0 until 12).map(at => Candidate(0, at.toLong, 0L, (12 - at).toFloat))
    for {
      serializer <- serializers
      layout <- layouts
    } {
      def state() = new CandidateRuns(4, MetricType.L2, layout, 8, 1L << 20)
      val original = state()
        .append(packed(layout, entries.take(3)))
        .append(packed(layout, entries.slice(3, 6)))
      val instance = serializer.newInstance()
      val restored =
        instance.deserialize[CandidateRuns](instance.serialize(original))
      restored.append(packed(layout, entries.slice(6, 9)))
      restored.absorb(state().append(packed(layout, entries.drop(9))))
      val expected =
        packed(layout, entries.sorted(Candidate.ranking(MetricType.L2)).take(4))
      restored.finish() shouldBe expected
      restored.finish() shouldBe expected
      original.finish() shouldBe packed(
        layout,
        entries.take(6).sorted(Candidate.ranking(MetricType.L2)).take(4)
      )
    }
  }

  test(
    "plain and carried candidates survive reducer spills and preserve absent queries"
  ) {
    val inputs = (0 until 72).flatMap { run =>
      // Query zero receives three times as many answers as the other keys.
      val queryIds = if (run < 24) 0 until 8 else 0 until 1
      queryIds.map { query =>
        val entries = (0 until 3).map { offset =>
          Candidate(
            query,
            run.toLong,
            offset.toLong,
            ((run * 3 + offset) * 7 % 67).toFloat
          )
        }
        query.toLong -> entries
      }
    }
    val spillOption = "spark.shuffle.spill.numElementsForceSpillThreshold"
    val conf = SparkEnv.get.conf
    val previous = conf.getOption(spillOption)
    try {
      for {
        layout <- layouts
        required <- Seq(None, Some((0L until 9L).toArray))
        partitions <- Seq(2, 3)
      } {
        val source = inputs.map { case (query, entries) =>
          query -> packed(layout, entries)
        }
        val expected = inputs.groupBy(_._1).map { case (query, runs) =>
          query -> packed(
            layout,
            runs.flatMap(_._2).sorted(Candidate.ranking(MetricType.L2)).take(7)
          ).toVector
        } ++ (if (required.isDefined) Map(8L -> Vector.empty[Byte])
              else Map.empty[Long, Vector[Byte]])
        val plan = SearchMerge.plan(
          partitions,
          9L,
          72,
          7,
          1L << 30,
          layout,
          if (layout == CandidateRuns.Carried) 1054L else 24L
        )

        def run(threshold: Int): (Map[Long, Vector[Byte]], Long) = {
          conf.set(spillOption, threshold.toString)
          val result = SearchMerge.byQuery[(Long, Array[Byte])](
            spark,
            spark.sparkContext.parallelize(source, 4),
            plan,
            MetricType.L2,
            (query, bytes) => Iterator.single(query -> bytes),
            required,
            query => query -> Array.emptyByteArray
          )
          val parts = result
            .mapPartitions { rows =>
              val answers = rows.map { case (query, bytes) =>
                query -> bytes.toVector
              }.toVector
              // This runs after the reducer consumed its combined iterator;
              // it observes this reducer's spill, not the map shuffle writer's.
              Iterator.single(
                answers -> TaskContext.get().taskMetrics().diskBytesSpilled
              )
            }
            .collect()
          parts.flatMap(_._1).toMap -> parts.map(_._2).sum
        }

        val (withoutSpill, noSpillBytes) = run(Int.MaxValue)
        val (withSpill, spillBytes) = run(7)
        withoutSpill shouldBe expected
        withSpill shouldBe expected
        noSpillBytes shouldBe 0L
        spillBytes should be > 0L
      }
    } finally
      previous match {
        case Some(value) => conf.set(spillOption, value)
        case None        => conf.remove(spillOption)
      }
  }
}
