package com.zilliz.spark.connector.search

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Expression, UnsafeProjection}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.{
  InputAdapter,
  LocalTableScanExec,
  SparkPlan,
  WholeStageCodegenExec
}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import com.zilliz.milvus.storage.schema.{
  MetricType,
  VectorElementType,
  VectorLayout
}
import com.zilliz.milvus.storage.vector.{EngineRange, QueryMatrix}

/** Prepares the query rows of a nearest-by join: classifies their vectors,
  * counts and caches computed rows, chooses whether the driver can hold them,
  * and numbers and packs the rows the connector searches. It neither searches
  * the base nor assembles join results.
  *
  * Local rows exclude BySpark before numbering. Distributed rows are cached
  * before counting and numbered before filtering, so recomputing a query side
  * in another order cannot pair a hit with the wrong row. Numbering and packing
  * remain explicit steps after Spark's fallback is planned by the coordinator.
  */
private[connector] object NearestByQueryRows {

  sealed trait Source
  final case class Held(rows: Array[InternalRow]) extends Source
  final case class Computed(rows: RDD[InternalRow]) extends Source

  /** A local scan's rows need no job; any other plan supplies its RDD. */
  def of(plan: SparkPlan): Source = unwrapped(plan) match {
    case local: LocalTableScanExec => Held(local.executeCollect())
    case _                         => Computed(plan.execute())
  }

  private def unwrapped(plan: SparkPlan): SparkPlan = plan match {
    case stage: WholeStageCodegenExec => unwrapped(stage.child)
    case input: InputAdapter          => unwrapped(input.child)
    case other                        => other
  }

  /** How the query rows divide, the bytes the driver holds if it takes them all
    * -- the rows themselves and the packed vectors of the ones that could be
    * searched -- and, of those, how many have each length.
    */
  private[search] final case class Counts(
      unranked: Long,
      searched: Long,
      bySpark: Long,
      heldBytes: Long,
      lengths: Map[Int, Long]
  ) {
    def +(other: Counts): Counts = Counts(
      unranked + other.unranked,
      searched + other.searched,
      bySpark + other.bySpark,
      heldBytes + other.heldBytes,
      (lengths.keySet ++ other.lengths.keySet).iterator
        .map(length =>
          length -> (lengths.getOrElse(length, 0L) +
            other.lengths.getOrElse(length, 0L))
        )
        .toMap
    )

    /** With the search's dimension known: the vectors of another length go to
      * Spark.
      */
    def at(dimension: Int): Counts = {
      val searchable = lengths.getOrElse(dimension, 0L)
      copy(searched = searchable, bySpark = bySpark + searched - searchable)
    }
  }

  private[search] object Counts {
    val Zero: Counts = Counts(0L, 0L, 0L, 0L, Map.empty)

    /** One query row of `bytes`, by its kind. */
    def of(kind: QueryVector, bytes: Long, packedBytes: Int => Long): Counts =
      kind match {
        case QueryVector.Unranked => Counts(1L, 0L, 0L, bytes, Map.empty)
        case QueryVector.Searched(vector) =>
          Counts(
            0L,
            1L,
            0L,
            bytes + packedBytes(vector.length),
            Map(vector.length -> 1L)
          )
        case QueryVector.BySpark => Counts(0L, 0L, 1L, bytes, Map.empty)
      }
  }

  private[search] sealed trait Prepared {
    def counts: Counts
    def dimension: Int
    def sparkRows(spark: SparkSession): Option[RDD[InternalRow]]
  }

  /** Classified rows held on the driver. The coordinator asks for copies and
    * packed vectors only after planning the rows Spark joins.
    */
  private[search] final class Local private[NearestByQueryRows] (
      rows: Array[InternalRow],
      kinds: Array[QueryVector],
      val dimension: Int
  ) extends Prepared {
    private val toSpark = rows.indices.filter(kinds(_) == QueryVector.BySpark)
    private val kept = rows.indices.filter(kinds(_) != QueryVector.BySpark)
    private val searchedCount = kinds.count {
      case QueryVector.Searched(_) => true
      case _                       => false
    }
    val counts: Counts = Counts(
      unranked = kept.length.toLong - searchedCount,
      searched = searchedCount.toLong,
      bySpark = toSpark.size.toLong,
      heldBytes = 0L,
      lengths = Map.empty
    )

    def sparkRows(spark: SparkSession): Option[RDD[InternalRow]] =
      if (toSpark.isEmpty) None
      else
        Some(
          spark.sparkContext.parallelize(
            toSpark.map(rows(_).copy()),
            math.max(1, math.min(toSpark.size, 64))
          )
        )

    def keptRows: Array[InternalRow] = kept.map(rows(_).copy()).toArray

    def packed(layout: VectorLayout): SearchQueries.Packed = {
      val searched = kept.map(kinds(_)).zipWithIndex.collect {
        case (QueryVector.Searched(vector), number) => number.toLong -> vector
      }
      SearchQueries.Packed(
        searched.map(_._1).toArray,
        QueryMatrix.packFloats(searched.map(_._2), layout)
      )
    }
  }

  /** Cached rows kept on executors. number is called once, after the fallback
    * plan has been made; its RDD is shared by vectors and the final join.
    */
  private[search] final class Distributed private[NearestByQueryRows] (
      rows: RDD[InternalRow],
      kind: InternalRow => QueryVector,
      val counts: Counts,
      val dimension: Int
  ) extends Prepared {
    def sparkRows(spark: SparkSession): Option[RDD[InternalRow]] = {
      val judge = kind
      val toSpark = rows.filter(judge(_) == QueryVector.BySpark)
      if (counts.bySpark == 0L) None else Some(toSpark)
    }

    // Association ids may have gaps; only packed group positions are dense.
    def number(): RDD[(InternalRow, Long)] = rows.zipWithUniqueId()

    def keptRows: RDD[InternalRow] = {
      val judge = kind
      rows.filter(judge(_) != QueryVector.BySpark)
    }

    def vectors(
        numbered: RDD[(InternalRow, Long)]
    ): SearchQueries.Rows = {
      val judge = kind
      val vectors = numbered.flatMap { case (row, number) =>
        judge(row) match {
          case QueryVector.Searched(vector) => Some(number -> vector)
          case _                            => None
        }
      }
      SearchQueries.Rows(vectors, counts.searched)
    }

    def kept(numbered: RDD[(InternalRow, Long)]): RDD[(Long, InternalRow)] = {
      val judge = kind
      numbered.collect {
        case (row, number) if judge(row) != QueryVector.BySpark => number -> row
      }
    }
  }

  private[search] def prepare(
      source: Source,
      types: Array[DataType],
      queryVector: Expression,
      searcher: NearestByInput.Searcher
  ): Prepared = {
    val judgement = searcher.judge
    val judge: InternalRow => QueryVector = row =>
      judgement(queryVector.eval(row))
    def dimension(lengths: Map[Int, Long]): Int =
      if (lengths.isEmpty) 0 else searcher.dimension(lengths)
    def local(rows: Array[InternalRow]): Local = {
      val judged = rows.map(judge)
      val width = dimension(
        judged
          .collect { case QueryVector.Searched(vector) => vector.length }
          .groupBy(identity)
          .map { case (length, all) => length -> all.length.toLong }
      )
      new Local(rows, judged.map(_.at(width)), width)
    }
    source match {
      case Held(rows)     => local(rows)
      case Computed(rows) =>
        // Classification and both sides of the final join share owned rows.
        val kept = rows.map(_.copy()).persist(StorageLevel.MEMORY_AND_DISK)
        try {
          val judged = countKinds(kept, types, judge, searcher.packedBytes)
          val width = dimension(judged.lengths)
          val counts = judged.at(width)
          if (counts.heldBytes <= searcher.limits.queriesMaxBytes) {
            val fetched = kept.collect()
            kept.unpersist(blocking = false)
            local(fetched)
          } else {
            val kind: InternalRow => QueryVector = row => judge(row).at(width)
            new Distributed(kept, kind, counts, width)
          }
        } catch {
          case failure: Throwable =>
            try kept.unpersist(blocking = false)
            catch { case later: Throwable => failure.addSuppressed(later) }
            throw failure
        }
    }
  }

  /** The query rows counted in one job: how they divide, and the bytes the
    * driver would hold if it took them all.
    */
  private def countKinds(
      rows: RDD[InternalRow],
      types: Array[DataType],
      judge: InternalRow => QueryVector,
      packedBytes: Int => Long
  ): Counts = rows
    .mapPartitions { rows =>
      val toUnsafe = UnsafeProjection.create(types)
      var counts = Counts.Zero
      rows.foreach { row =>
        val bytes = toUnsafe(row).getSizeInBytes.toLong
        counts = counts + Counts.of(judge(row), bytes, packedBytes)
      }
      Iterator.single(counts)
    }
    .fold(Counts.Zero)(_ + _)

}

/** What a query row's vector is to the search, under the ranking function's
  * rules.
  */
private[search] sealed trait QueryVector {

  /** The kind once the search's dimension is known: a searched vector of
    * another length goes to Spark.
    */
  def at(dimension: Int): QueryVector = this match {
    case QueryVector.Searched(vector) if vector.length != dimension =>
      QueryVector.BySpark
    case other => other
  }
}

private[search] object QueryVector {

  /** NULL against every base row: a NULL vector, or one of the field's
    * dimension with a NULL element. The base vectors all have that dimension,
    * so no pair is compared at another length.
    */
  case object Unranked extends QueryVector

  /** In [[EngineRange]]: searched. */
  final case class Searched(vector: Array[Float]) extends QueryVector

  /** Another length, or a value outside the engine range: joined by Spark. */
  case object BySpark extends QueryVector

  /** A DataFrame input's query vector, before the search's dimension is chosen:
    * a non-empty vector without a NULL element whose values are in
    * [[EngineRange]] at its own length may be searched; one with a NULL element
    * goes to Spark, since whether it fails depends on the base's lengths.
    */
  def ofFrame(value: Any, metric: MetricType): QueryVector = value match {
    case null => Unranked
    case array: ArrayData =>
      val size = array.numElements()
      if (size == 0 || (0 until size).exists(array.isNullAt)) BySpark
      else {
        val vector = array.toFloatArray()
        val layout = VectorLayout(VectorElementType.Float32, size)
        if (EngineRange.query(vector, layout, metric) == EngineRange.Engine)
          Searched(vector)
        else BySpark
      }
    case other =>
      throw new IllegalArgumentException(
        s"A query vector is ARRAY<FLOAT>, not ${other.getClass.getName}"
      )
  }

  def of(value: Any, layout: VectorLayout, metric: MetricType): QueryVector =
    value match {
      case null => Unranked
      case array: ArrayData =>
        val size = array.numElements()
        if (size != layout.dimension) BySpark
        else if ((0 until size).exists(array.isNullAt)) Unranked
        else {
          val vector = array.toFloatArray()
          if (EngineRange.query(vector, layout, metric) == EngineRange.Engine)
            Searched(vector)
          else BySpark
        }
      case other =>
        throw new IllegalArgumentException(
          s"A query vector is ARRAY<FLOAT>, not ${other.getClass.getName}"
        )
    }
}
