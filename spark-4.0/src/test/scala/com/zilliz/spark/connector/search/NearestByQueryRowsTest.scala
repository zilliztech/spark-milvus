package com.zilliz.spark.connector.search

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{
  BoundReference,
  GenericInternalRow,
  Literal
}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{ArrayType, DataType, FloatType, LongType}
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

import com.zilliz.milvus.storage.schema.MetricType
import com.zilliz.milvus.storage.vector.RankingFunction
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
    // 48 row bytes plus 8 packed bytes for the vector, and 24 for the NULL row.
    withPrepared(rows, 80L) { prepared =>
      prepared shouldBe a[Local]
      prepared.dimension shouldBe 2
      prepared.counts shouldBe Counts(1L, 1L, 0L, 0L, Map.empty)
      prepared.asInstanceOf[Local].keptRows.map(_.getLong(0)).toSeq shouldBe
        Seq(0L, 1L)
    }
    withPrepared(rows, 79L) { prepared =>
      prepared shouldBe a[Distributed]
      prepared.dimension shouldBe 2
      prepared.counts shouldBe Counts(1L, 1L, 0L, 80L, Map(2 -> 1L))
      prepared.sparkRows(spark) shouldBe None
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
      NearestByInput.Frame(
        FrameSearch.Base(
          spark.sparkContext.emptyRDD[InternalRow],
          Seq(BoundReference(0, LongType, nullable = false)),
          vectorExpression
        ),
        SearchLimits(limit, None, None)
      ),
      approx = false,
      k = 1,
      metric = MetricType.L2,
      function = UnusedRanking,
      metrics = Map.empty
    )

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
    ): java.lang.Double =
      throw new AssertionError("preparing query rows must not score a pair")
  }
}
