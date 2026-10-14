package com.zilliz.spark.connector.search

import scala.jdk.CollectionConverters._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{
  Expression,
  GenericInternalRow,
  JoinedRow,
  UnsafeProjection
}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.{
  InputAdapter,
  LocalTableScanExec,
  SparkPlan,
  WholeStageCodegenExec
}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.{
  ArrayType,
  DataType,
  FloatType,
  LongType,
  StructField,
  StructType
}
import org.apache.spark.storage.StorageLevel

import com.zilliz.milvus.storage.index.{
  EngineRange,
  QueryMatrix,
  RankingFunction
}
import com.zilliz.milvus.storage.read.exec.SegmentIndexHandle
import com.zilliz.milvus.storage.schema.{
  MetricType,
  VectorElementType,
  VectorLayout
}
import com.zilliz.spark.connector.metrics.{NearestByMetrics, SearchMetrics}
import com.zilliz.spark.connector.options.{MilvusOption, SearchLimits}
import com.zilliz.spark.connector.read.{MilvusInputPartition, MilvusScan}
import io.milvus.grpc.schema.{DataType => MilvusDataType, FieldSchema}

/** A nearest-by join run as the two-stage search, over a Milvus table input or
  * a DataFrame input (docs/design/architecture/dataframe-api.html sections 2
  * and 4).
  *
  * Each query row is one of three kinds by its ranking value under Spark's
  * rules (VectorFunctionImplUtils in Spark 4.2). A NULL vector is NULL against
  * every base row and finds nothing. A vector of the search's dimension whose
  * values are in [[EngineRange]] is searched. Any other is joined by Spark
  * itself (`bySpark`), whose rows join the result; whether it fails, and on
  * which pair, is Spark's. A Milvus table input's dimension is its field's, and
  * a vector of that dimension with a NULL element finds nothing, every base
  * vector having that length. A DataFrame input has no field: its dimension is
  * the commonest length among the vectors that could be searched, and a vector
  * with a NULL element goes to Spark, since whether it fails depends on the
  * lengths the base holds.
  *
  * The rows Spark does not join are numbered 0 to n - 1, and the numbers are
  * the query ids the search carries. Where the rows are decides how they meet
  * their hits. Rows the driver holds -- a local relation's, or a query side
  * whose rows fit `milvus.search.queries.max.bytes` and are fetched in one job
  * -- are classified, numbered and packed there, broadcast with the packed
  * vectors, and attached to each hit by number after the last stage; a LEFT
  * OUTER join's rows without a hit come out of the merge stage. Other rows are
  * kept and numbered on the executors, their vectors go by shuffle, and they
  * meet their hits in one join after the last stage, so a query side computed
  * again in another order cannot pair a hit with the wrong row.
  */
private[connector] object NearestBySearch {

  private val QueryId = SearchQueries.IdColumn
  private val Vector = SearchQueries.VectorColumn

  /** The base columns a hit carries that the search itself knows, so they are
    * taken from the candidate rather than read.
    */
  private val FromCandidate = Set("_segment_id", "_row_offset")

  /** The query set of rows kept on the executors: each row's number and its
    * vector.
    */
  private val QuerySetSchema = StructType(
    Seq(
      StructField(QueryId, LongType, nullable = false),
      StructField(
        Vector,
        ArrayType(FloatType, containsNull = false),
        nullable = false
      )
    )
  )

  /** The query side as the node hands it over. */
  sealed trait Queries

  /** Rows the driver already holds. */
  final case class Held(rows: Array[InternalRow]) extends Queries

  /** Rows an RDD computes. */
  final case class Computed(rows: RDD[InternalRow]) extends Queries

  object Queries {

    /** A local table scan's rows, taken on the driver, which runs no job; any
      * other plan's, as its RDD.
      */
    def of(plan: SparkPlan): Queries = unwrapped(plan) match {
      case local: LocalTableScanExec => Held(local.executeCollect())
      case _                         => Computed(plan.execute())
    }

    private def unwrapped(plan: SparkPlan): SparkPlan = plan match {
      case stage: WholeStageCodegenExec => unwrapped(stage.child)
      case input: InputAdapter          => unwrapped(input.child)
      case other                        => other
    }
  }

  /** What the join searches. */
  sealed trait Input

  /** A Milvus table input: the scan, the base output by the scan's column name
    * and type in output order, and the vector field.
    */
  final case class MilvusTable(
      scan: MilvusScan,
      columns: Seq[(String, DataType)],
      vectorColumn: String
  ) extends Input

  /** A DataFrame input: the base as Spark computes it. Its search options are
    * the defaults (section 5); a test passes others.
    */
  final case class Frame(
      base: FrameSearch.Base,
      limits: SearchLimits = SearchLimits.from(Map.empty)
  ) extends Input

  /** How the query rows divide, the bytes the driver holds if it takes them all
    * -- the rows themselves and the packed vectors of the ones that could be
    * searched -- and, of those, how many have each length.
    */
  private final case class Counts(
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

  private object Counts {
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

  /** What differs between the two inputs: how a query vector is first judged,
    * the search's dimension and layout, the search itself, where a hit's base
    * columns are, and what the node reports about segments. `judge` and
    * `packedBytes` run in tasks, so they are functions that hold no more than
    * they read.
    */
  private trait Searcher {
    def limits: SearchLimits

    /** The kind before the dimension is known: a `Searched` vector of another
      * length than the one chosen goes to Spark.
      */
    def judge: Any => QueryVector

    def dimension(lengths: Map[Int, Long]): Int

    /** How the searched vectors are packed. */
    def layoutAt(dimension: Int): VectorLayout

    /** The bytes a packed vector of this length takes. */
    def packedBytes: Int => Long

    def search(
        input: SearchQueries.Input,
        dimension: Int,
        required: Option[Array[Long]]
    ): SearchMerge.Hits

    /** Where in a hit row each base column is, and its type. */
    def positions: Seq[Int]
    def baseTypes: Seq[DataType]

    def segments(searched: Long): Map[String, Long]
  }

  /** @param queryTypes
    *   the query side's column types, in its output order
    * @param queryVector
    *   bound to the query rows
    * @param outputTypes
    *   the join's output: the query columns, then the base columns
    * @param function
    *   what the base rows outside [[EngineRange]] are scored by
    * @param bySpark
    *   Spark's own execution of the join for the query rows it is given, in the
    *   join's output columns; called on the driver
    * @param metrics
    *   the SQL metrics of the node that runs the join ([[NearestByMetrics]])
    */
  def run(
      spark: SparkSession,
      queries: Queries,
      queryTypes: Seq[DataType],
      queryVector: Expression,
      input: Input,
      outer: Boolean,
      approx: Boolean,
      k: Int,
      metric: MetricType,
      outputTypes: Seq[DataType],
      function: RankingFunction,
      bySpark: RDD[InternalRow] => RDD[InternalRow],
      metrics: Map[String, SQLMetric]
  ): RDD[InternalRow] = {
    val searcher = input match {
      case table: MilvusTable =>
        milvusTable(spark, table, approx, k, metric, function, metrics)
      case frame: Frame => dataFrame(spark, frame, k, metric, function, metrics)
    }
    val join = new Join(
      spark,
      searcher,
      queryVector,
      outer,
      outputTypes.toArray,
      outputTypes.size - queryTypes.size,
      bySpark,
      metrics
    )
    val result = queries match {
      case Held(rows)     => join.held(rows)
      case Computed(rows) => join.computed(rows, queryTypes.toArray)
    }
    val outputRows = metrics(NearestByMetrics.OutputRows)
    result.mapPartitions { rows =>
      rows.map { row =>
        outputRows.add(1L)
        row
      }
    }
  }

  /** One join on the driver: it classifies the query rows, has the searcher
    * search the ones it can, and joins each to its hits -- by number from a
    * broadcast when the driver holds the rows ([[held]]), by a join after the
    * last stage when they stay on the executors ([[numbered]]).
    *
    * A join is never sent to a task. What its tasks run is built by
    * [[judging]], [[kindAt]], [[baseValues]] and [[joiners]] from values copied
    * out of it, so a closure holds those values and not the join.
    *
    * @param width
    *   the base columns of the join's output
    */
  private final class Join(
      spark: SparkSession,
      searcher: Searcher,
      queryVector: Expression,
      outer: Boolean,
      output: Array[DataType],
      width: Int,
      bySpark: RDD[InternalRow] => RDD[InternalRow],
      metrics: Map[String, SQLMetric]
  ) {

    /** Rows the driver holds: classified, numbered and packed here, and
      * attached to the hits from a broadcast.
      */
    def held(rows: Array[InternalRow]): RDD[InternalRow] = {
      val judged = rows.map(judging)
      val dimension = commonest(
        judged
          .collect { case QueryVector.Searched(vector) => vector.length }
          .groupBy(identity)
          .map { case (length, all) => length -> all.length.toLong }
      )
      val kinds = judged.map(_.at(dimension))
      val toSpark = rows.indices.filter(kinds(_) == QueryVector.BySpark)
      val joinedBySpark =
        if (toSpark.isEmpty) None
        else
          Some(
            bySpark(
              spark.sparkContext.parallelize(
                toSpark.map(rows(_).copy()),
                math.max(1, math.min(toSpark.size, 64))
              )
            )
          )
      val kept = rows.indices.filter(kinds(_) != QueryVector.BySpark)
      val keptRows = kept.map(rows(_).copy()).toArray
      val searched = kept.map(kinds(_)).zipWithIndex.collect {
        case (QueryVector.Searched(vector), number) => number.toLong -> vector
      }
      val counts = Counts(
        unranked = keptRows.length.toLong - searched.size,
        searched = searched.size.toLong,
        bySpark = toSpark.size.toLong,
        heldBytes = 0L,
        lengths = Map.empty
      )
      val payload = spark.sparkContext.broadcast(keptRows)
      val newJoiner = joiners
      val connector: RDD[InternalRow] =
        if (searched.isEmpty) {
          post(counts)
          if (!outer) spark.sparkContext.emptyRDD[InternalRow]
          else
            spark.sparkContext
              .parallelize(
                keptRows.indices,
                math.max(1, math.min(keptRows.length, 64))
              )
              .mapPartitions { numbers =>
                val joiner = newJoiner()
                numbers.map(number =>
                  joiner(payload.value(number), None).copy()
                )
              }
        } else {
          val hits = searcher.search(
            SearchQueries.Packed(
              searched.map(_._1).toArray,
              QueryMatrix.packFloats(
                searched.map(_._2),
                searcher.layoutAt(dimension)
              )
            ),
            dimension,
            if (outer) Some(Array.tabulate(keptRows.length)(_.toLong))
            else None
          )
          post(counts)
          val baseOf = baseValues
          hits.rows.mapPartitions { found =>
            val joiner = newJoiner()
            found.map(hit =>
              joiner(payload.value(hit.getLong(0).toInt), Some(baseOf(hit)))
            )
          }
        }
      joinedBySpark.fold(connector)(connector.union(_))
    }

    /** Rows an RDD computes, counted in one job that also says whether the
      * driver can hold them: fetched and [[held]] when it can, kept on the
      * executors and [[numbered]] when it cannot.
      */
    def computed(
        rows: RDD[InternalRow],
        types: Array[DataType]
    ): RDD[InternalRow] = {
      // Kept before numbering: zipWithIndex counts the partitions in a job of
      // its own, which then fills the cache instead of computing the rows
      // twice.
      val kept = rows.map(_.copy()).persist(StorageLevel.MEMORY_AND_DISK)
      val judged = countKinds(kept, types, judging, searcher.packedBytes)
      val dimension = commonest(judged.lengths)
      val counts = judged.at(dimension)
      if (counts.heldBytes <= searcher.limits.queriesMaxBytes) {
        val fetched = kept.collect()
        kept.unpersist(blocking = false)
        held(fetched)
      } else numbered(kept, counts, dimension)
    }

    /** Rows kept and numbered on the executors, joined to their hits after the
      * last stage, so that a query side computed again in another order cannot
      * pair a hit with the wrong row.
      */
    private def numbered(
        kept: RDD[InternalRow],
        counts: Counts,
        dimension: Int
    ): RDD[InternalRow] = {
      val kind = kindAt(dimension)
      val toSpark = kept.filter(kind(_) == QueryVector.BySpark)
      val joinedBySpark =
        if (counts.bySpark == 0L) None else Some(bySpark(toSpark))
      val numbered = kept.zipWithIndex()
      val vectors = numbered.flatMap { case (row, number) =>
        kind(row) match {
          case QueryVector.Searched(vector) => Some(Row(number, vector))
          case _                            => None
        }
      }
      val querySet = spark.createDataFrame(vectors, QuerySetSchema)
      val hits =
        if (counts.searched == 0L) None
        else
          Some(
            searcher.search(
              SearchQueries.Frame(querySet, counts.searched),
              dimension,
              None
            )
          )
      post(counts)
      val baseOf = baseValues
      val hitsByQuery = hits match {
        case Some(found) => found.rows.map(hit => hit.getLong(0) -> baseOf(hit))
        case None =>
          spark.sparkContext.emptyRDD[(Long, InternalRow)]
      }
      // The rows Spark joins are not this side's to keep or drop.
      val byNumber = numbered.collect {
        case (row, number) if kind(row) != QueryVector.BySpark =>
          number -> row
      }
      val paired: RDD[(InternalRow, Option[InternalRow])] =
        if (outer)
          byNumber.leftOuterJoin(hitsByQuery).values
        else
          byNumber.join(hitsByQuery).values.map { case (row, hit) =>
            row -> Some(hit)
          }
      val newJoiner = joiners
      val searched: RDD[InternalRow] = paired.mapPartitions { pairs =>
        val joiner = newJoiner()
        pairs.map { case (row, hit) => joiner(row, hit) }
      }
      joinedBySpark.fold(searched)(searched.union(_))
    }

    // What the driver settled, on the node.
    private def post(counts: Counts): Unit =
      NearestByMetrics.postDriverValues(
        spark.sparkContext,
        metrics,
        Map(
          NearestByMetrics.SearchedQueries -> counts.searched,
          NearestByMetrics.SparkQueries -> counts.bySpark,
          NearestByMetrics.UnrankedQueries -> counts.unranked
        ) ++ searcher.segments(counts.searched)
      )

    private def commonest(lengths: Map[Int, Long]): Int =
      if (lengths.isEmpty) 0 else searcher.dimension(lengths)

    /** A query row's kind before the search's dimension is known. */
    private def judging: InternalRow => QueryVector = {
      val judgement = searcher.judge
      val vector = queryVector
      row => judgement(vector.eval(row))
    }

    /** A query row's kind once the search's dimension is known. */
    private def kindAt(dimension: Int): InternalRow => QueryVector = {
      val judge = judging
      row => judge(row).at(dimension)
    }

    /** A hit's base values, NULL where the hit is. */
    private def baseValues: InternalRow => InternalRow = {
      val positions = searcher.positions.toArray
      val types = searcher.baseTypes.toArray
      val columns = width
      hit => {
        val values = new GenericInternalRow(columns)
        var i = 0
        while (i < columns) {
          values.update(i, hit.get(positions(i), types(i)))
          i += 1
        }
        values
      }
    }

    /** What makes a [[Joiner]] in a task. */
    private def joiners: () => Joiner = {
      val types = output
      val columns = width
      () => new Joiner(types, columns)
    }
  }

  /** A query row joined to its hit's base values, or to NULLs where it has no
    * hit, as a row of the join's output. Each task makes its own: a projection
    * is not shared.
    */
  private final class Joiner(output: Array[DataType], width: Int) {
    private val project = UnsafeProjection.create(output)
    private val joined = new JoinedRow()
    private val absent = new GenericInternalRow(width)

    def apply(row: InternalRow, base: Option[InternalRow]): InternalRow =
      project(joined(row, base.getOrElse(absent)))
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

  /** A Milvus table input: the field's layout and dimension, the two-stage
    * search over the scan's segments, and the take stage for the base columns.
    */
  private final class MilvusSearcher(
      spark: SparkSession,
      input: MilvusTable,
      approx: Boolean,
      k: Int,
      metric: MetricType,
      function: RankingFunction,
      metrics: Map[String, SQLMetric]
  ) extends Searcher {
    private val scan = input.scan
    // Planning made this a Milvus table input for a field `vectorField` takes.
    private val field = vectorField(scan, input.vectorColumn).fold(
      reason => throw new IllegalStateException(reason),
      identity
    )
    private val layout: VectorLayout =
      VectorLayout.of(field.dataType, MilvusSearch.dimensionOf(field))
    MilvusSearch.checkSearchable(metric, layout)
    private val options =
      Map(scan.options.asCaseSensitiveMap().asScala.toSeq: _*)
    override val limits: SearchLimits = SearchLimits.from(options)
    private val partitions = scan
      .planInputPartitions()
      .toSeq
      .map(_.asInstanceOf[MilvusInputPartition])
    private val read = scan.readSchema()
    private val taken = StructType(
      input.columns
        .map(_._1)
        .filterNot(FromCandidate)
        .distinct
        .map(name => read(name))
    )

    override val judge: Any => QueryVector = {
      val field = layout
      val ranking = metric
      value => QueryVector.of(value, field, ranking)
    }

    override def dimension(lengths: Map[Int, Long]): Int = layout.dimension

    override def layoutAt(dimension: Int): VectorLayout = layout

    override val packedBytes: Int => Long = {
      val bytes = layout.rowBytes.toLong
      _ => bytes
    }

    override def search(
        queries: SearchQueries.Input,
        dimension: Int,
        required: Option[Array[Long]]
    ): SearchMerge.Hits =
      MilvusSearch.execute(
        spark,
        options,
        queries,
        partitions,
        field,
        layout,
        k,
        metric,
        if (approx) SearchMode.Index else SearchMode.Exact,
        MilvusOption.searchParameters(options),
        MilvusOption(scan.options).milvusFilter,
        scan.pushedExpression,
        taken,
        SearchMetrics.of(metrics),
        function,
        required
      )

    // A hit row is the candidate's columns, then `taken` in order. Positions,
    // not names: a base column may share a name with a candidate column.
    override val positions: Seq[Int] = {
      val takenNames = taken.fieldNames.toSeq
      input.columns.map { case (name, _) =>
        if (FromCandidate(name)) SearchMerge.HitSchema.fieldIndex(name)
        else SearchMerge.HitSchema.size + takenNames.indexOf(name)
      }
    }

    override val baseTypes: Seq[DataType] = input.columns.map(_._2)

    // The search has planned and checked every segment by now, so the
    // selection here cannot fail differently.
    override def segments(searched: Long): Map[String, Long] = {
      val indexSegments =
        if (!approx || searched == 0L) 0L
        else
          partitions.count(partition =>
            SegmentIndexHandle
              .select(partition.task, field.fieldID, metric)
              .nonEmpty
          )
      Map(
        NearestByMetrics.IndexSegments -> indexSegments,
        NearestByMetrics.ExactSegments ->
          (if (searched == 0L) 0L else partitions.size.toLong - indexSegments)
      )
    }
  }

  private def milvusTable(
      spark: SparkSession,
      input: MilvusTable,
      approx: Boolean,
      k: Int,
      metric: MetricType,
      function: RankingFunction,
      metrics: Map[String, SQLMetric]
  ): Searcher =
    new MilvusSearcher(spark, input, approx, k, metric, function, metrics)

  /** A DataFrame input: the dimension the queries make, an exact scan of the
    * base's rows, and the hit rows carrying the base row after the query id.
    */
  private def dataFrame(
      spark: SparkSession,
      input: Frame,
      k: Int,
      metric: MetricType,
      function: RankingFunction,
      metrics: Map[String, SQLMetric]
  ): Searcher = new Searcher {
    override val limits: SearchLimits = input.limits

    override val judge: Any => QueryVector = {
      val ranking = metric
      value => QueryVector.ofFrame(value, ranking)
    }

    // The commonest length; of two as common, the shorter, so the choice does
    // not depend on the order the rows were counted in.
    override def dimension(lengths: Map[Int, Long]): Int =
      lengths.toSeq.minBy { case (length, count) => (-count, length) }._1

    override def layoutAt(dimension: Int): VectorLayout =
      VectorLayout(VectorElementType.Float32, dimension)

    override val packedBytes: Int => Long = length => 4L * length

    override def search(
        queries: SearchQueries.Input,
        dimension: Int,
        required: Option[Array[Long]]
    ): SearchMerge.Hits =
      FrameSearch.execute(
        spark,
        queries,
        input.base,
        dimension,
        k,
        metric,
        function,
        limits,
        SearchMetrics.of(metrics),
        required
      )

    override val positions: Seq[Int] = input.base.types.indices.map(_ + 1)

    override val baseTypes: Seq[DataType] = input.base.types

    override def segments(searched: Long): Map[String, Long] = Map.empty
  }

  private val SearchedTypes: Set[MilvusDataType] = Set(
    MilvusDataType.FloatVector,
    MilvusDataType.Float16Vector,
    MilvusDataType.BFloat16Vector
  )

  /** The vector field a Milvus table input searches, named by the scan column
    * the base vector reads, or why that column is not one: the search takes a
    * FloatVector, Float16Vector or BFloat16Vector field. An Array of Float
    * reads as ARRAY<FLOAT> as those do, but has neither a dimension nor an
    * index; an Int8Vector field reaches a ranking only cast to ARRAY<FLOAT>,
    * which is not a column.
    */
  def vectorField(
      scan: MilvusScan,
      column: String
  ): Either[String, FieldSchema] =
    scan.snapshot.schema.fields.find(_.name == column) match {
      case Some(field) if SearchedTypes(field.dataType) => Right(field)
      case Some(field) =>
        Left(
          s"the base vector $column is a Milvus ${field.dataType} field, not a vector field"
        )
      case None =>
        Left(s"the base vector $column is not a field of the collection")
    }

  /** Which segments the search takes through an index and which it scans, from
    * the snapshot's metadata alone, for explain: no file is read. A row count
    * the snapshot does not state is checked when the search plans.
    */
  def describeSegments(
      scan: MilvusScan,
      vectorColumn: String,
      metric: MetricType,
      approx: Boolean
  ): String = {
    val segments = scan.snapshot.dataSegments
    if (!approx) s"${segments.size} segments scanned exactly"
    else {
      val fieldId = scan.snapshot.schema.fields
        .find(_.name == vectorColumn)
        .map(_.fieldID)
        .getOrElse(-1L)
      val reasons = segments.flatMap(segment =>
        SegmentIndexHandle
          .usable(segment.id, segment.indexes, fieldId, metric)
          .left
          .toOption
          .map(_.label)
      )
      val exact =
        if (reasons.isEmpty) ""
        else
          reasons
            .groupBy(identity)
            .toSeq
            .sortBy(_._1)
            .map { case (reason, all) => s"$reason: ${all.size}" }
            .mkString(" (", ", ", ")")
      s"${segments.size - reasons.size} of ${segments.size} segments by " +
        s"index, ${reasons.size} scanned exactly$exact"
    }
  }
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
