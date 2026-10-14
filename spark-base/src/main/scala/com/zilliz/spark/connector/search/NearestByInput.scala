package com.zilliz.spark.connector.search

import scala.jdk.CollectionConverters._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{
  BoundReference,
  UnsafeProjection
}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.SparkSession

import com.zilliz.milvus.storage.read.exec.SegmentIndexHandle
import com.zilliz.milvus.storage.schema.{
  MetricType,
  VectorElementType,
  VectorLayout
}
import com.zilliz.milvus.storage.vector.RankingFunction
import com.zilliz.spark.connector.metrics.{NearestByMetrics, SearchMetrics}
import com.zilliz.spark.connector.options.{MilvusOption, SearchLimits}
import com.zilliz.spark.connector.read.{MilvusInputPartition, MilvusScan}
import io.milvus.grpc.schema.{DataType => MilvusDataType, FieldSchema}

/** The base a nearest-by join searches. Its adapter supplies the vector rules,
  * search execution and requested output columns; query preparation and joining
  * the hits belong to [[NearestByQueryRows]] and [[NearestBySearch]].
  */
private[connector] sealed trait NearestByInput

private[connector] object NearestByInput {

  /** Base columns already present in each candidate, requiring no take. */
  private val FromCandidate = Set("_segment_id", "_row_offset")

  /** A Milvus table input: the scan, the base output by the scan's column name
    * and type in output order, and the vector field.
    */
  final case class MilvusTable(
      scan: MilvusScan,
      columns: Seq[(String, DataType)],
      vectorColumn: String
  ) extends NearestByInput

  /** A DataFrame input: the base as Spark computes it. Its search options are
    * the defaults (section 5); a test passes others.
    */
  final case class Frame(
      base: FrameSearch.Base,
      limits: SearchLimits = SearchLimits.from(Map.empty)
  ) extends NearestByInput

  /** What differs between the two inputs: how a query vector is first judged,
    * the search's dimension and layout, the search itself, and what the node
    * reports about segments. `judge` and `packedBytes` run in tasks, so they
    * are functions that hold no more than they read.
    */
  private[search] trait Searcher {
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

    /** Each query id with its requested base columns, in output order. */
    def search(
        input: SearchQueries.Input,
        dimension: Int,
        required: Option[Array[Long]]
    ): RDD[(Long, InternalRow)]

    def segments(searched: Long): Map[String, Long]
  }

  /** Creates the driver-side adapter before any query rows are classified. A
    * Milvus input resolves its field and plans its scan here, including when
    * the queries later turn out to be empty or all unranked.
    */
  private[search] def searcher(
      spark: SparkSession,
      input: NearestByInput,
      approx: Boolean,
      k: Int,
      metric: MetricType,
      function: RankingFunction,
      metrics: Map[String, SQLMetric]
  ): Searcher = input match {
    case table: MilvusTable =>
      new MilvusSearcher(spark, table, approx, k, metric, function, metrics)
    case frame: Frame => dataFrame(spark, frame, k, metric, function, metrics)
  }

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
      VectorLayout.of(field.dataType, dimensionOf(field))
    checkSearchable(metric, layout)
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
    ): RDD[(Long, InternalRow)] = {
      val hits = MilvusSearch.execute(
        spark,
        options,
        queries,
        partitions,
        field,
        layout,
        k,
        metric,
        if (approx) SearchMode.PreferIndex else SearchMode.Exact,
        MilvusOption.searchParameters(options),
        MilvusOption(scan.options).milvusFilter,
        scan.pushedExpression,
        taken,
        SearchMetrics.of(metrics),
        function,
        required
      )
      outputRows(hits, positions)
    }

    // A hit row is the candidate's columns, then `taken` in order. Positions,
    // not names: a base column may share a name with a candidate column.
    private val positions: Seq[Int] = {
      val takenNames = taken.fieldNames.toSeq
      input.columns.map { case (name, _) =>
        if (FromCandidate(name)) SearchMerge.HitSchema.fieldIndex(name)
        else SearchMerge.HitSchema.size + takenNames.indexOf(name)
      }
    }

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
    ): RDD[(Long, InternalRow)] = {
      val hits = FrameSearch.execute(
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
      outputRows(hits, input.base.types.indices.map(_ + 1))
    }

    override def segments(searched: Long): Map[String, Long] = Map.empty
  }

  /** Input-specific candidate columns stop here. Rows may enter a shuffle or be
    * retained by the join, so each projected output owns its bytes.
    */
  private def outputRows(
      hits: SearchMerge.Hits,
      positions: Seq[Int]
  ): RDD[(Long, InternalRow)] = {
    val columns = positions.map { at =>
      val field = hits.schema(at)
      // Outer searches can return NULL even for a non-nullable base field.
      BoundReference(at, field.dataType, nullable = true)
    }
    hits.rows.mapPartitions { rows =>
      val project = UnsafeProjection.create(columns)
      rows.map(row => row.getLong(0) -> project(row).copy())
    }
  }

  private def dimensionOf(field: FieldSchema): Int = field.typeParams
    .find(_.key == "dim")
    .map(_.value.toInt)
    .getOrElse(
      throw new IllegalArgumentException(
        s"Field '${field.name}' declares no dimension"
      )
    )

  /** The field's element type determines the supported metrics. */
  private[search] def checkSearchable(
      metric: MetricType,
      layout: VectorLayout
  ): Unit = {
    require(
      layout.elementType != VectorElementType.Bit,
      "A binary vector field is not searched; the connector searches FloatVector, Float16Vector, BFloat16Vector and Int8Vector fields"
    )
    val supported = MetricType.forElementType(layout.elementType)
    require(
      supported.contains(metric),
      s"A ${layout.elementType} field takes ${supported.map(_.name).sorted.mkString(" or ")}, not $metric"
    )
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
