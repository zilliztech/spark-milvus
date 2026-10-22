package com.zilliz.spark.connector.extensions

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

import com.zilliz.milvus.storage.schema.MetricType
import com.zilliz.spark.connector.table.MilvusTable

/** Whether the connector executes a nearest-by join, and the node that does.
  * Spark 4.2's rule asks this of its `NearestByJoin`, the older lines' rule of
  * a [[NearestByJoinRequest]]; each line says which of its expressions are the
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
  * base.
  */
private[connector] object NearestByTakeover {

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
    else if (!pluginLoaded(session.sparkContext.getConf))
      Left(
        s"neither spark.plugins nor spark.plugins.defaultList names $PluginClass"
      )
    else
      for {
        known <- ranking.toRight(
          s"the ranking ${rankingExpression.sql} is not a vector function the connector computes"
        )
        _ <- ranks(known, direction)
        arguments <- sides(known.first, known.second, left, right)
        _ <- reads(right, approx, approxEverywhere)
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

  /** Spark loads the plugins `spark.plugins.defaultList` names as well as those
    * `spark.plugins` names, so a cluster's default list counts too.
    */
  private[extensions] def pluginLoaded(conf: SparkConf): Boolean =
    Seq("spark.plugins.defaultList", "spark.plugins")
      .flatMap(key => conf.get(key, "").split(','))
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
    * section 3): a base with a Milvus table in it in both, whichever input the
    * planner makes of it; a base without one for EXACT, and for APPROX only
    * where nothing else could execute the join.
    */
  private def reads(
      base: LogicalPlan,
      approx: Boolean,
      approxEverywhere: Boolean
  ): Either[String, Unit] =
    if (!approx || approxEverywhere || containsMilvusTable(base)) Right(())
    else
      Left(
        "APPROX over a base without a Milvus table is left to Spark or to the base's data source"
      )

  /** A view's plan is its child, so the walk looks inside views too. */
  private def containsMilvusTable(plan: LogicalPlan): Boolean =
    plan.find {
      case relation: DataSourceV2Relation =>
        relation.table.isInstanceOf[MilvusTable]
      case _ => false
    }.nonEmpty
}
