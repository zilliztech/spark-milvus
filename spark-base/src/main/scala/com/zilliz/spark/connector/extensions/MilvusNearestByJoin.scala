package com.zilliz.spark.connector.extensions

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  Attribute,
  AttributeReference,
  BindReferences,
  Expression,
  NamedExpression
}
import org.apache.spark.sql.catalyst.plans.{JoinType, LeftOuter}
import org.apache.spark.sql.catalyst.plans.logical.{
  BinaryNode,
  LogicalPlan,
  Project
}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.{
  BinaryExecNode,
  SparkPlan,
  SparkStrategy,
  UnaryExecNode
}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.SparkSession

import com.zilliz.milvus.storage.schema.MetricType
import com.zilliz.spark.connector.metrics.NearestByMetrics
import com.zilliz.spark.connector.read.{
  FrameSearch,
  MilvusScan,
  NearestBySearch
}

/** A nearest-by join the connector executes: for each query row, the k base
  * rows nearest to it by one Milvus metric (docs/design/architecture/
  * dataframe-api.html sections 3 and 4).
  *
  * It keeps Spark's NearestByJoin contract: the output is the query columns and
  * the base columns, both nullable, with the column ids unchanged, and no score
  * column. Its output comes from its children, so Spark's optimizer prunes both
  * sides and pushes the base's filters into the Milvus scan as it does for any
  * such node; a filter above it stays above it. `queryVector` is a query column
  * or a constant, `baseVector` the base's vector column, `rankingExpression`
  * the ranking as the user wrote it, and `spark` Spark's own computation of the
  * join for what the connector leaves to it (section 2).
  */
final case class MilvusNearestByJoin(
    left: LogicalPlan,
    right: LogicalPlan,
    joinType: JoinType,
    approx: Boolean,
    k: Int,
    metric: MetricType,
    queryVector: Expression,
    baseVector: Expression,
    rankingExpression: Expression,
    spark: SparkNearestBy
) extends BinaryNode {

  override def output: Seq[Attribute] =
    left.output.map(_.withNullability(true)) ++
      right.output.map(_.withNullability(true))

  override def simpleString(maxFields: Int): String =
    s"MilvusNearestByJoin ${if (approx) "APPROX" else "EXACT"} $metric k=$k " +
      s"${joinType.sql} ${rankingExpression.sql}"

  override protected def withNewChildrenInternal(
      newLeft: LogicalPlan,
      newRight: LogicalPlan
  ): MilvusNearestByJoin = copy(left = newLeft, right = newRight)
}

/** The base as the optimizer left it, when it is a Milvus table input: the
  * Milvus scan, with at most a projection of its columns above it, so every
  * base column is a scan column under its own name or an alias of one.
  */
private[extensions] final case class MilvusTableInput(
    scan: MilvusScan,
    columns: Map[Long, String]
) {

  /** The scan column a base attribute reads. */
  def columnOf(attribute: Attribute): String = columns.getOrElse(
    attribute.exprId.id,
    throw new IllegalStateException(
      s"${attribute.sql} is not a column of the Milvus scan"
    )
  )
}

private[extensions] object MilvusTableInput {

  def of(base: LogicalPlan): Either[String, MilvusTableInput] = base match {
    case relation: DataSourceV2ScanRelation =>
      milvusScan(relation).map(scan =>
        MilvusTableInput(
          scan,
          relation.output.map(a => a.exprId.id -> a.name).toMap
        )
      )
    case Project(list, relation: DataSourceV2ScanRelation) =>
      for {
        scan <- milvusScan(relation)
        columns <- projected(list)
      } yield MilvusTableInput(scan, columns)
    case other =>
      Left(s"the base is ${other.nodeName} above the scan, not the Milvus scan")
  }

  private def milvusScan(
      relation: DataSourceV2ScanRelation
  ): Either[String, MilvusScan] = relation.scan match {
    case scan: MilvusScan => Right(scan)
    case other            => Left(s"the scan is ${other.getClass.getName}")
  }

  private def projected(
      list: Seq[NamedExpression]
  ): Either[String, Map[Long, String]] = {
    val columns = list.collect {
      case a: AttributeReference => a.exprId.id -> a.name
      case alias @ Alias(source: AttributeReference, _) =>
        alias.exprId.id -> source.name
    }
    if (columns.size == list.size) Right(columns.toMap)
    else Left("the projection above the scan computes a column")
  }
}

/** Plans a [[MilvusNearestByJoin]] by its optimized base (docs/design/
  * architecture/dataframe-api.html section 4): a Milvus table input when the
  * base is the Milvus scan with at most a projection of its columns above it
  * and the base vector is one of them, a DataFrame input otherwise, with the
  * reason kept for explain.
  */
object MilvusNearestByJoinStrategy extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case join: MilvusNearestByJoin =>
      val table = for {
        input <- MilvusTableInput.of(join.right)
        vector <- join.baseVector match {
          case attribute: Attribute => Right(input.columnOf(attribute))
          case other =>
            Left(s"the base vector ${other.sql} is computed, not a field")
        }
      } yield (input, vector)
      table match {
        case Right((input, vector)) =>
          MilvusNearestByJoinExec(
            planLater(join.left),
            input.scan,
            join.right.output,
            join.right.output.map(input.columnOf),
            join.joinType,
            join.approx,
            join.k,
            join.metric,
            join.queryVector,
            vector,
            join.spark
          ) :: Nil
        case Left(reason) =>
          MilvusNearestByJoinFrameExec(
            planLater(join.left),
            planLater(join.right),
            join.joinType,
            join.approx,
            join.k,
            join.metric,
            join.queryVector,
            join.baseVector,
            reason,
            join.spark
          ) :: Nil
      }
    case _ => Nil
  }
}

/** Runs a [[MilvusNearestByJoin]] over a Milvus table input: the query rows are
  * its child, and the base is searched through `scan` rather than read
  * (docs/design/architecture/dataframe-api.html section 4); the query rows the
  * connector does not search are joined by `spark`.
  *
  * `scan` and `spark` are the driver's: a task whose closure reaches the plan
  * above this node, a window's for one, carries the node without them, as
  * Spark's `BatchScanExec` carries no scan.
  *
  * Explain names the input, the mode, the metric, k, and how many of the
  * snapshot's segments an APPROX search takes through an index and why the
  * others are scanned; the search's counters, how the query rows divided and
  * how many segments each way took are the node's SQL metrics.
  */
final case class MilvusNearestByJoinExec(
    left: SparkPlan,
    @transient scan: MilvusScan,
    baseOutput: Seq[Attribute],
    baseColumns: Seq[String],
    joinType: JoinType,
    approx: Boolean,
    k: Int,
    metric: MetricType,
    queryVector: Expression,
    vectorColumn: String,
    @transient spark: SparkNearestBy
) extends UnaryExecNode {

  override def child: SparkPlan = left

  override def output: Seq[Attribute] =
    left.output.map(_.withNullability(true)) ++
      baseOutput.map(_.withNullability(true))

  override lazy val metrics: Map[String, SQLMetric] =
    NearestByMetrics.create(sparkContext)

  /** Read from the snapshot's metadata, so explain opens no file. */
  @transient private lazy val segments: String =
    NearestBySearch.describeSegments(scan, vectorColumn, metric, approx)

  override def simpleString(maxFields: Int): String =
    s"MilvusNearestByJoin ${if (approx) "APPROX" else "EXACT"} $metric k=$k " +
      s"${joinType.sql} input=Milvus table, vector=$vectorColumn, " +
      s"base=${baseColumns.mkString("[", ", ", "]")}, $segments"

  override protected def doExecute(): RDD[InternalRow] = {
    val active = SparkSession.active
    NearestBySearch.run(
      active,
      NearestBySearch.Queries.of(left),
      left.output.map(_.dataType),
      BindReferences.bindReference(queryVector, left.output),
      NearestBySearch.MilvusTable(
        scan,
        baseColumns.zip(baseOutput.map(_.dataType)),
        vectorColumn
      ),
      outer = joinType == LeftOuter,
      approx = approx,
      k = k,
      metric = metric,
      outputTypes = output.map(_.dataType),
      function = spark.function,
      bySpark = spark.execute(active, _, left.output, output),
      metrics = metrics
    )
  }

  override protected def withNewChildInternal(
      newChild: SparkPlan
  ): MilvusNearestByJoinExec = copy(left = newChild)
}

/** Runs a [[MilvusNearestByJoin]] over a DataFrame input: the query rows are
  * its left child, the base rows its right child, read and scanned exactly
  * however the join was written, APPROX too (docs/design/architecture/
  * dataframe-api.html sections 3 and 4); the query rows the connector does not
  * search are joined by `spark`.
  *
  * Explain names the input and why the base is not a Milvus table input, the
  * mode, the metric, k and the base vector; the search's counters and how the
  * query rows divided are the node's SQL metrics. `spark` is the driver's, as
  * in [[MilvusNearestByJoinExec]].
  */
final case class MilvusNearestByJoinFrameExec(
    left: SparkPlan,
    right: SparkPlan,
    joinType: JoinType,
    approx: Boolean,
    k: Int,
    metric: MetricType,
    queryVector: Expression,
    baseVector: Expression,
    reason: String,
    @transient spark: SparkNearestBy
) extends BinaryExecNode {

  override def output: Seq[Attribute] =
    left.output.map(_.withNullability(true)) ++
      right.output.map(_.withNullability(true))

  override lazy val metrics: Map[String, SQLMetric] =
    NearestByMetrics.create(sparkContext)

  override def simpleString(maxFields: Int): String =
    s"MilvusNearestByJoin ${if (approx) "APPROX" else "EXACT"} $metric k=$k " +
      s"${joinType.sql} input=DataFrame ($reason), vector=${baseVector.sql}, " +
      s"base=${right.output.map(_.name).mkString("[", ", ", "]")}, " +
      (if (approx) "computed exactly" else "scanned exactly")

  override protected def doExecute(): RDD[InternalRow] = {
    val active = SparkSession.active
    NearestBySearch.run(
      active,
      NearestBySearch.Queries.of(left),
      left.output.map(_.dataType),
      BindReferences.bindReference(queryVector, left.output),
      NearestBySearch.Frame(
        FrameSearch.Base(
          right.execute(),
          right.output.map(_.dataType),
          BindReferences.bindReference(baseVector, right.output)
        )
      ),
      outer = joinType == LeftOuter,
      approx = approx,
      k = k,
      metric = metric,
      outputTypes = output.map(_.dataType),
      function = spark.function,
      bySpark = spark.execute(active, _, left.output, output),
      metrics = metrics
    )
  }

  override protected def withNewChildrenInternal(
      newLeft: SparkPlan,
      newRight: SparkPlan
  ): MilvusNearestByJoinFrameExec = copy(left = newLeft, right = newRight)
}
