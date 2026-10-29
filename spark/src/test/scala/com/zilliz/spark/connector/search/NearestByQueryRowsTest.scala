package com.zilliz.spark.connector.search

import java.lang.{Float => JavaFloat}
import java.nio.file.{Files, Path, Paths}
import scala.collection.mutable

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{
  BoundReference,
  GenericInternalRow,
  Literal
}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{ArrayType, DataType, FloatType, LongType}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.RDDBlockId
import org.apache.spark.util.LongAccumulator
import org.apache.spark.SparkEnv
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

import com.zilliz.milvus.storage.schema.MetricType
import com.zilliz.milvus.storage.vector.RankingFunction
import com.zilliz.spark.connector.metrics.NearestByMetrics
import com.zilliz.spark.connector.options.SearchLimits

class NearestByQueryRowsTest
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterAll {

  import NearestByQueryRows._
  import NearestByQueryRowsTest._

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("nearest-by-query-rows")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
  }

  override def afterAll(): Unit = if (spark != null) spark.stop()

  test("partition statistics count every row and only pack searched vectors") {
    val packed = mutable.ArrayBuffer.empty[Int]
    val counts = countPartition(
      (0L until 5L).iterator.map(scalar),
      scalarTypes,
      scalarKind,
      length => {
        packed += length
        2L * length + 1L
      }
    )
    // A one-long UnsafeRow has an 8-byte null bitmap and an 8-byte value.
    counts shouldBe Counts(2L, 2L, 1L, 92L, Map(2 -> 1L, 3 -> 1L))
    packed.toSeq shouldBe Seq(2, 3)
    counts.at(2) shouldBe
      Counts(2L, 1L, 2L, 92L, Map(2 -> 1L, 3 -> 1L))
  }

  test("empty partitions and driver merging preserve the same statistics") {
    val partitions =
      Seq(Seq.empty[Long], Seq(0L, 1L), Seq(2L, 3L, 4L), Seq.empty[Long])
    val counts = partitions.map { ids =>
      countPartition(
        ids.iterator.map(scalar),
        scalarTypes,
        scalarKind,
        packedSize
      )
    }
    counts.head shouldBe Counts.Zero
    counts.last shouldBe Counts.Zero
    counts.foldLeft(Counts.Zero)(_ + _) shouldBe
      Counts(2L, 2L, 1L, 92L, Map(2 -> 1L, 3 -> 1L))
  }

  test("partition byte accounting retains ordinary Long wraparound") {
    val counts = countPartition(
      Iterator(scalar(1L), scalar(0L), scalar(2L)),
      scalarTypes,
      scalarKind,
      _ => Long.MaxValue
    )
    counts shouldBe
      Counts(1L, 1L, 1L, Long.MinValue + 47L, Map(2 -> 1L))
  }

  test(
    "projection, judgement and packed-byte failures remain visible in order"
  ) {
    val events = mutable.ArrayBuffer.empty[String]
    val judge: InternalRow => QueryVector = row => {
      events += "judge"
      scalarKind(row)
    }
    val pack: Int => Long = _ => {
      events += "pack"
      throw new IllegalArgumentException("packed bytes failed")
    }
    intercept[ClassCastException] {
      countPartition(
        Iterator(new GenericInternalRow(Array[Any]("not a long"))),
        scalarTypes,
        judge,
        pack
      )
    }
    events shouldBe empty

    val failure = new IllegalArgumentException("judgement failed")
    val caught = intercept[IllegalArgumentException] {
      countPartition(
        Iterator(scalar(1L)),
        scalarTypes,
        _ => {
          events += "judge"
          throw failure
        },
        pack
      )
    }
    caught should be theSameInstanceAs failure
    events.toSeq shouldBe Seq("judge")

    events.clear()
    intercept[IllegalArgumentException] {
      countPartition(Iterator(scalar(1L)), scalarTypes, judge, pack)
    }.getMessage shouldBe "packed bytes failed"
    events.toSeq shouldBe Seq("judge", "pack")
  }

  test("empty computed partitions prepare an empty local query set") {
    withPrepared(Seq.empty, 1L) { prepared =>
      prepared shouldBe a[Local]
      prepared.dimension shouldBe 0
      prepared.counts shouldBe Counts.Zero
      prepared.sparkRows(spark) shouldBe None
      prepared.asInstanceOf[Local].keptRows shouldBe empty
    }
  }

  test(
    "computed query collection includes row and packed bytes at its exact limit"
  ) {
    val rows = Seq(query(0L, vector(1f, 2f)), query(1L, null))
    val before = spark.sparkContext.getPersistentRDDs.keySet
    // 48 row bytes plus 8 packed bytes for the vector, and 24 for the NULL row.
    withPrepared(rows, 80L) { prepared =>
      prepared shouldBe a[Local]
      prepared.dimension shouldBe 2
      prepared.counts shouldBe Counts(1L, 1L, 0L, 0L, Map.empty)
      prepared.asInstanceOf[Local].keptRows.map(_.getLong(0)).toSeq shouldBe
        Seq(0L, 1L)
      spark.sparkContext.getPersistentRDDs.keySet shouldBe before
    }
    withPrepared(rows, 79L) { prepared =>
      prepared shouldBe a[Distributed]
      prepared.dimension shouldBe 2
      prepared.counts shouldBe Counts(1L, 1L, 0L, 80L, Map(2 -> 1L))
      prepared.sparkRows(spark) shouldBe None
    }
  }

  test(
    "distributed queries retain associations and fallback across repeated actions"
  ) {
    withChangingRows { (source, changed, evaluations) =>
      val prepared = prepare(
        Computed(source),
        types,
        vectorExpression,
        frameSearcher(1L)
      ).asInstanceOf[Distributed]
      val numbered = prepared.number()
      val vectors = prepared.vectors(numbered).selected
      val original = vectors
        .collect()
        .map { case (id, values) =>
          id -> values.toSeq
        }
        .toMap
      original.values.toSet shouldBe Set(Seq(1f), Seq(9f), Seq(5f))
      evaluations.value shouldBe 2L
      Files.createFile(changed)

      for (_ <- 0 until 2) {
        val associated = vectors.join(prepared.kept(numbered)).collect()
        associated.length shouldBe 3
        associated.foreach { case (id, (values, row)) =>
          values.toSeq shouldBe original(id)
          row.getArray(1).toFloatArray().toSeq shouldBe original(id)
        }
        prepared.sparkRows(spark).get.map(_.getLong(0)).collect().toSeq shouldBe
          Seq(40L)
      }
      evaluations.value shouldBe 2L
    }
  }

  test(
    "losing one query checkpoint block fails instead of reassigning query ids"
  ) {
    withChangingRows { (source, changed, evaluations) =>
      val before = spark.sparkContext.getPersistentRDDs.keySet
      val prepared = prepare(
        Computed(source),
        types,
        vectorExpression,
        frameSearcher(1L)
      ).asInstanceOf[Distributed]
      val numbered = prepared.number()
      // Search has already consumed these ids and vectors before the row join.
      val searched = prepared.vectors(numbered).selected.collect().toMap
      searched(0L).toSeq shouldBe Seq(1f)
      searched(2L).toSeq shouldBe Seq(9f)
      val cached = spark.sparkContext.getPersistentRDDs
        .filterNot { case (id, _) => before(id) }
        .values
        .toSeq
      cached.size shouldBe 1
      val lost = RDDBlockId(cached.head.id, 0)
      val retained = RDDBlockId(cached.head.id, 1)
      val blocks = SparkEnv.get.blockManager
      blocks.getStatus(lost) should not be empty
      blocks.getStatus(retained) should not be empty
      Files.createFile(changed)
      blocks.removeBlock(lost, tellMaster = true)
      blocks.getStatus(lost) shouldBe None
      blocks.getStatus(retained) should not be empty

      val failure = intercept[Exception] {
        prepared.kept(numbered).collect()
      }
      Iterator
        .iterate[Throwable](failure)(_.getCause)
        .takeWhile(_ != null)
        .exists(cause =>
          Option(cause.getMessage).exists(message =>
            message.contains(lost.name) &&
              message.toLowerCase.contains("checkpoint")
          )
        ) shouldBe true
      // Recomputing partition 0 would reverse its rows and silently swap ids.
      evaluations.value shouldBe 2L
    }
  }

  test(
    "Spark fallback keeps its query checkpoint for repeated result actions"
  ) {
    withChangingRows { (source, changed, evaluations) =>
      val before = spark.sparkContext.getPersistentRDDs.keySet
      val result = NearestBySearch.run(
        spark,
        Computed(source.map(row => query(row.getLong(0), vector()))),
        types.toSeq,
        vectorExpression,
        frameInput(1L),
        outer = false,
        approx = false,
        k = 1,
        metric = MetricType.L2,
        outputTypes = types.toSeq,
        function = UnusedRanking,
        bySpark = rows => rows,
        metrics = NearestByMetrics.create(spark.sparkContext)
      )
      val cached = spark.sparkContext.getPersistentRDDs.keySet -- before
      cached.size shouldBe 1
      result.map(_.getLong(0)).collect().toSeq shouldBe
        Seq(10L, 20L, 30L, 40L)
      Files.createFile(changed)
      for (_ <- 0 until 2) {
        result.map(_.getLong(0)).collect().toSeq shouldBe
          Seq(10L, 20L, 30L, 40L)
        spark.sparkContext.getPersistentRDDs.keySet should contain allElementsOf cached
      }
      evaluations.value shouldBe 2L
    }
  }

  test(
    "Spark fallback planning failures release the prepared query checkpoint"
  ) {
    withChangingRows { (source, _, _) =>
      val before = spark.sparkContext.getPersistentRDDs.keySet
      val failure = new IllegalArgumentException("fallback planning failed")
      val caught = intercept[IllegalArgumentException] {
        NearestBySearch.run(
          spark,
          Computed(source),
          types.toSeq,
          vectorExpression,
          frameInput(1L),
          outer = false,
          approx = false,
          k = 1,
          metric = MetricType.L2,
          outputTypes = types.toSeq,
          function = UnusedRanking,
          bySpark = _ => throw failure,
          metrics = NearestByMetrics.create(spark.sparkContext)
        )
      }
      caught should be theSameInstanceAs failure
      spark.sparkContext.getPersistentRDDs.keySet shouldBe before
    }
  }

  test(
    "mixed dimensions retain modal fallback and distributed query association"
  ) {
    val tied = Seq(
      query(0L, null),
      query(1L, vector(1f, 2f)),
      query(2L, vector(1f, 2f, 3f)),
      query(3L, vector(3f, 4f)),
      query(4L, vector()),
      query(5L, vector(Float.NaN, 1f)),
      query(6L, new GenericArrayData(Array[Any](1f, null))),
      query(7L, vector(4f, 5f, 6f))
    )
    for (
      (rows, dimension, searched, heldBytes, lengths) <- Seq(
        (tied, 2, Set(1L, 3L), 400L, Map(2 -> 2L, 3 -> 2L)),
        (
          tied :+ query(8L, vector(7f, 8f, 9f)),
          3,
          Set(2L, 7L, 8L),
          468L,
          Map(2 -> 2L, 3 -> 3L)
        )
      )
    ) withClue(s"dimension=$dimension: ") {
      withPrepared(rows, 1L) { prepared =>
        val distributed = prepared.asInstanceOf[Distributed]
        distributed.dimension shouldBe dimension
        distributed.counts shouldBe
          Counts(1L, searched.size.toLong, 5L, heldBytes, lengths)
        val numbered = distributed.number()
        val associations = numbered
          .collect()
          .map { case (row, number) =>
            number -> row.getLong(0)
          }
          .toMap
        associations.size shouldBe rows.size
        val selected = distributed.vectors(numbered)
        selected.count shouldBe searched.size.toLong
        val joined =
          selected.selected.join(distributed.kept(numbered)).collect()
        joined.map { case (number, (values, row)) =>
          associations(number) shouldBe row.getLong(0)
          values.toSeq shouldBe row.getArray(1).toFloatArray().toSeq
          row.getLong(0)
        }.toSet shouldBe searched
        distributed
          .kept(numbered)
          .values
          .map(_.getLong(0))
          .collect()
          .toSet shouldBe
          (searched + 0L)
        distributed
          .sparkRows(spark)
          .get
          .map(_.getLong(0))
          .collect()
          .toSet shouldBe
          (rows.map(_.getLong(0)).toSet -- searched - 0L)
      }
    }
  }

  test(
    "computed judgement failures propagate and release the preparation cache"
  ) {
    val context = spark.sparkContext
    val before = context.getPersistentRDDs.keySet
    val failure = intercept[Exception] {
      prepare(
        Computed(context.parallelize(Seq(scalar(0L)), 1)),
        scalarTypes,
        Literal(1L),
        frameSearcher(1L)
      )
    }
    Iterator
      .iterate[Throwable](failure)(_.getCause)
      .takeWhile(_ != null)
      .exists(cause =>
        Option(cause.getMessage)
          .exists(_.contains("A query vector is ARRAY<FLOAT>"))
      ) shouldBe true
    context.getPersistentRDDs.keySet shouldBe before
  }

  private val vectorType = ArrayType(FloatType, containsNull = true)
  private val vectorExpression = BoundReference(1, vectorType, nullable = true)
  private val types = Array[DataType](LongType, vectorType)

  private def frameSearcher(limit: Long): NearestByInput.Searcher =
    NearestByInput.searcher(
      spark,
      frameInput(limit),
      approx = false,
      k = 1,
      metric = MetricType.L2,
      function = UnusedRanking,
      metrics = Map.empty
    )

  private def frameInput(limit: Long): NearestByInput.Frame =
    NearestByInput.Frame(
      FrameSearch.Base(
        spark.sparkContext.emptyRDD[InternalRow],
        Seq(BoundReference(0, LongType, nullable = false)),
        vectorExpression
      ),
      SearchLimits(limit, None, None)
    )

  private def withChangingRows(
      check: (RDD[InternalRow], Path, LongAccumulator) => Unit
  ): Unit = {
    val context = spark.sparkContext
    val before = context.getPersistentRDDs.keySet
    val directory = Files.createTempDirectory("nearest-by-query-order")
    val changed = directory.resolve("reverse")
    val marker = changed.toString
    val evaluations = context.longAccumulator("query source evaluations")
    val source = context.parallelize(Seq(0, 1), 2).mapPartitions { partitions =>
      evaluations.add(1L)
      val rows = partitions.flatMap {
        case 0 => Iterator(query(10L, vector(1f)), query(20L, vector(9f)))
        case 1 => Iterator(query(30L, vector(5f)), query(40L, vector()))
      }.toVector
      if (Files.exists(Paths.get(marker))) rows.reverseIterator
      else rows.iterator
    }
    try check(source, changed, evaluations)
    finally {
      context.getPersistentRDDs
        .filterNot { case (id, _) => before(id) }
        .values
        .foreach(_.unpersist(blocking = true))
      Files.deleteIfExists(changed)
      Files.delete(directory)
    }
  }

  private def withPrepared(rows: Seq[InternalRow], limit: Long)(
      check: Prepared => Unit
  ): Unit = {
    val context = spark.sparkContext
    val before = context.getPersistentRDDs.keySet
    val computed = context
      .parallelize(rows, 3)
      .union(context.parallelize(Seq.empty[InternalRow], 2))
    try
      check(
        prepare(
          Computed(computed),
          types,
          vectorExpression,
          frameSearcher(limit)
        )
      )
    finally
      context.getPersistentRDDs
        .filterNot { case (id, _) => before(id) }
        .values
        .foreach(_.unpersist(blocking = true))
  }
}

object NearestByQueryRowsTest {
  val scalarTypes: Array[DataType] = Array(LongType)

  def scalar(value: Long): InternalRow =
    new GenericInternalRow(Array[Any](value))

  val scalarKind: InternalRow => QueryVector = row =>
    row.getLong(0) match {
      case 0L | 4L => QueryVector.Unranked
      case 1L      => QueryVector.Searched(new Array[Float](2))
      case 2L      => QueryVector.BySpark
      case 3L      => QueryVector.Searched(new Array[Float](3))
      case other   => throw new AssertionError(s"Unexpected fixture id $other")
    }

  val packedSize: Int => Long = length => 2L * length + 1L

  def vector(values: Float*): GenericArrayData =
    new GenericArrayData(values.toArray)

  def query(id: Long, values: GenericArrayData): InternalRow =
    new GenericInternalRow(Array[Any](id, values))

  object UnusedRanking extends RankingFunction {
    override def score(
        query: Array[Float],
        row: Array[Float]
    ): JavaFloat =
      throw new AssertionError("preparing query rows must not score a pair")
  }
}
