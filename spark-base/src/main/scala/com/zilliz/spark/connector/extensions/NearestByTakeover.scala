package com.zilliz.spark.connector.extensions

import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  Attribute,
  AttributeReference,
  ExprId,
  Expression,
  PredicateHelper
}
import org.apache.spark.sql.catalyst.plans.logical.{
  Filter,
  LogicalPlan,
  Project,
  SubqueryAlias,
  View
}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.util.V2ExpressionBuilder
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.SparkSession

import com.zilliz.milvus.storage.schema.MetricType
import com.zilliz.spark.connector.expr.SparkPredicateTranslator
import com.zilliz.spark.connector.table.MilvusTable
import io.milvus.grpc.schema.{DataType => MilvusDataType}

/** Whether the connector executes a nearest-by join, and the node that does
  * (docs/design/architecture/dataframe-api.html section 3). Spark 4.2's rule
  * asks this of its `NearestByJoin`, the older lines' rule of a
  * [[NearestByJoinRequest]]; each line says which of its expressions are the
  * three vector functions.
  *
  * A join is replaced when its ranking is one of them in the direction its
  * metric ranks, one argument is computed from the base's columns and the other
  * from the query's columns or from constants, and the base is one the
  * connector reads: a base containing a Milvus table always, in either mode,
  * since the table is the connector's whichever input it becomes; a base
  * without one for EXACT, which uses no index; and for APPROX only on the lines
  * before 4.2 (`approxEverywhere`), where no other implementation could take
  * it. On 4.2 APPROX over another base is left to Spark or to that base's own
  * data source, whose index it may have. The prerequisites besides: the join is
  * resolved, cross joins are enabled, neither side is a stream, and
  * `MilvusSparkPlugin` is in `spark.plugins`, so every executor has the native
  * library. Which input the join becomes is the planner's, from the optimized
  * base; `milvusBase` says why a base is not certain to become a Milvus table
  * input.
  */
private[connector] object NearestByTakeover extends PredicateHelper {

  /** A ranking the connector computes: the metric of the vector function, its
    * name, and its two arguments in the order they are written.
    */
  final case class Ranking(
      metric: MetricType,
      name: String,
      first: Expression,
      second: Expression
  )

  private val PluginClass = classOf[MilvusSparkPlugin].getName

  /** @param ranking
    *   the ranking expression as one of the three functions, or None
    * @param spark
    *   Spark's own computation of this join, given the ranking and whether the
    *   query is its first argument
    */
  def replacement(
      session: SparkSession,
      left: LogicalPlan,
      right: LogicalPlan,
      joinType: JoinType,
      approx: Boolean,
      numResults: Int,
      rankingExpression: Expression,
      direction: RankingDirection,
      resolved: Boolean,
      ranking: Option[Ranking],
      approxEverywhere: Boolean,
      spark: (Ranking, Boolean) => SparkNearestBy
  ): Either[String, MilvusNearestByJoin] =
    if (!resolved) Left("the join is not resolved")
    else if (!SQLConf.get.crossJoinEnabled)
      Left(s"${SQLConf.CROSS_JOINS_ENABLED.key} is false")
    else if (left.isStreaming || right.isStreaming)
      Left("a side of the join is a stream")
    else if (!pluginLoaded(session))
      Left(s"spark.plugins does not name $PluginClass")
    else
      for {
        known <- ranking.toRight(
          s"the ranking ${rankingExpression.sql} is not a vector function the connector computes"
        )
        _ <- ranks(known, direction)
        arguments <- sides(known.first, known.second, left, right)
        _ <- reads(right, arguments._2, approx, approxEverywhere)
      } yield MilvusNearestByJoin(
        left,
        right,
        joinType,
        approx,
        numResults,
        known.metric,
        arguments._1,
        arguments._2,
        rankingExpression,
        spark(known, arguments._3)
      )

  private def pluginLoaded(session: SparkSession): Boolean =
    session.sparkContext.getConf
      .get("spark.plugins", "")
      .split(',')
      .map(_.trim)
      .contains(PluginClass)

  /** A direction the metric does not rank by asks for the farthest rows, which
    * no Milvus search returns.
    */
  private def ranks(
      ranking: Ranking,
      direction: RankingDirection
  ): Either[String, Unit] = {
    val expected =
      if (ranking.metric == MetricType.L2) RankingDirection.Distance
      else RankingDirection.Similarity
    if (direction == expected) Right(())
    else
      Left(
        s"${ranking.name} ranks by ${expected.name}, and the join asks for ${direction.name}"
      )
  }

  /** Which argument is the query and which the base vector, in either order,
    * and whether the query is the first. The base vector reads base columns
    * only, at least one; the query vector reads query columns or none.
    */
  private def sides(
      a: Expression,
      b: Expression,
      left: LogicalPlan,
      right: LogicalPlan
  ): Either[String, (Expression, Expression, Boolean)] = {
    def query(e: Expression) =
      e.deterministic && e.references.subsetOf(left.outputSet)
    def base(e: Expression) =
      e.deterministic && e.references.nonEmpty &&
        e.references.subsetOf(right.outputSet)
    if (base(b) && query(a)) Right((a, b, true))
    else if (base(a) && query(b)) Right((b, a, false))
    else
      Left(
        "the ranking does not take one vector from the base and one from the query or constants"
      )
  }

  /** Whether the connector reads this base in this mode (the boundary table of
    * section 3). A base that is certain to become a Milvus table input is read
    * in both; so is any other base with a Milvus table in it, as a DataFrame
    * input; a base without one is read for EXACT, and for APPROX only where
    * nothing else could execute the join.
    */
  private def reads(
      base: LogicalPlan,
      vector: Expression,
      approx: Boolean,
      approxEverywhere: Boolean
  ): Either[String, Unit] = {
    val table = vector match {
      case attribute: AttributeReference => milvusBase(base, attribute)
      case other =>
        Left(s"the base vector ${other.sql} is computed, not a field")
    }
    table match {
      case Right(()) => Right(())
      case Left(_)
          if containsMilvusTable(base) || !approx || approxEverywhere =>
        Right(())
      case Left(reason) =>
        Left(
          s"APPROX over a base without a Milvus table is left to Spark or to the base's data source ($reason)"
        )
    }
  }

  /** A view's plan is its child, so the walk looks inside views too. */
  private def containsMilvusTable(plan: LogicalPlan): Boolean =
    plan.find {
      case relation: DataSourceV2Relation =>
        relation.table.isInstanceOf[MilvusTable]
      case _ => false
    }.nonEmpty

  /** The base is the Milvus table, seen through aliases, views, column
    * projections and filters the scan takes whole, and `vector` is one of its
    * float vector fields.
    */
  private def milvusBase(
      base: LogicalPlan,
      vector: Attribute
  ): Either[String, Unit] = {
    def walk(
        plan: LogicalPlan,
        column: ExprId,
        filters: Seq[Expression]
    ): Either[String, Unit] = plan match {
      case SubqueryAlias(_, child) => walk(child, column, filters)
      case view: View              => walk(view.child, column, filters)
      case Project(list, child) =>
        val source = list.collectFirst {
          case a: AttributeReference if a.exprId == column => a.exprId
          case alias @ Alias(a: AttributeReference, _)
              if alias.exprId == column =>
            a.exprId
        }
        if (
          list.exists(e => !e.isInstanceOf[AttributeReference] && !isRename(e))
        )
          Left("a projection in the base computes a column")
        else
          source match {
            case Some(id) => walk(child, id, filters)
            case None =>
              Left("the base vector column is not projected from the table")
          }
      case Filter(condition, child) =>
        walk(child, column, filters ++ splitConjunctivePredicates(condition))
      case relation: DataSourceV2Relation =>
        relation.table match {
          case table: MilvusTable =>
            milvusTable(
              table,
              relation,
              relation.output.find(_.exprId == column),
              filters
            )
          case other =>
            Left(s"the base reads ${other.name()}, not a Milvus table")
        }
      case other =>
        Left(
          s"the base holds ${other.nodeName}, which the Milvus scan cannot take"
        )
    }
    walk(base, vector.exprId, Seq.empty)
  }

  private def isRename(e: Expression): Boolean = e match {
    case Alias(_: AttributeReference, _) => true
    case _                               => false
  }

  /** The vector column is a float vector field of the table, and every filter
    * reads only the table's columns and translates whole into a predicate the
    * Milvus scan evaluates, the translation the scan itself applies.
    */
  private def milvusTable(
      table: MilvusTable,
      relation: DataSourceV2Relation,
      column: Option[Attribute],
      filters: Seq[Expression]
  ): Either[String, Unit] = {
    val schema = table.schema()
    column.flatMap(c =>
      table.snapshot.schema.fields.find(_.name == c.name)
    ) match {
      case None =>
        Left("the base vector column is not a field of the Milvus table")
      case Some(field)
          if field.dataType != MilvusDataType.FloatVector &&
            field.dataType != MilvusDataType.Float16Vector &&
            field.dataType != MilvusDataType.BFloat16Vector =>
        Left(
          s"the base vector field '${field.name}' is ${field.dataType}, not a float vector"
        )
      case Some(_) =>
        filters
          .find(filter =>
            !filter.references.subsetOf(relation.outputSet) ||
              new V2ExpressionBuilder(filter, isPredicate = true)
                .build()
                .collect { case predicate: Predicate => predicate }
                .flatMap(SparkPredicateTranslator.translate(_, schema))
                .isEmpty
          )
          .map(filter =>
            s"the filter ${filter.sql} is not one the Milvus scan takes whole"
          )
          .toLeft(())
    }
  }
}
