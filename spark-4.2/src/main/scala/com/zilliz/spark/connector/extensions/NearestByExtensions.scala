package com.zilliz.spark.connector.extensions

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{
  Attribute,
  Expression,
  VectorCosineSimilarity,
  VectorFunctionImplUtils,
  VectorInnerProduct,
  VectorL2Distance
}
import org.apache.spark.sql.catalyst.optimizer.RewriteNearestByJoin
import org.apache.spark.sql.catalyst.plans.{
  NearestByDirection,
  NearestByDistance,
  NearestBySimilarity
}
import org.apache.spark.sql.catalyst.plans.logical.{
  LogicalPlan,
  NearestByJoin,
  Project
}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.NEAREST_BY_JOIN
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.unsafe.types.UTF8String

import com.zilliz.milvus.storage.schema.MetricType

/** Spark 4.2's NEAREST BY join, taken over where the connector executes it
  * (docs/design/architecture/dataframe-api.html section 3).
  */
object NearestByExtensions {
  def apply(extensions: SparkSessionExtensions): Unit =
    extensions.injectPostHocResolutionRule(new ReplaceNearestByJoin(_))
}

/** Replaces a `NearestByJoin` with [[MilvusNearestByJoin]] in the analyzer's
  * Post-Hoc Resolution batch, the last place the node exists: the optimizer's
  * first batch rewrites it into a cross join (`RewriteNearestByJoin`). Which
  * joins are replaced is [[NearestByTakeover]]'s; anything else is left to
  * Spark, with the reason in the driver log, and Spark's CheckAnalysis reports
  * what it reports about a nearest-by join.
  */
final class ReplaceNearestByJoin(session: SparkSession)
    extends Rule[LogicalPlan]
    with Logging {

  override def apply(plan: LogicalPlan): LogicalPlan =
    plan.transformUpWithPruning(_.containsPattern(NEAREST_BY_JOIN)) {
      case join: NearestByJoin =>
        NearestByTakeover.replacement(
          session,
          join.left,
          join.right,
          join.joinType,
          join.approx,
          join.numResults,
          join.rankingExpression,
          directionOf(join.direction),
          join.resolved,
          rankingOf(join.rankingExpression),
          approxEverywhere = false,
          (known, queryFirst) =>
            RewrittenNearestBy(
              join,
              SparkVectorFunction(known.metric, known.name, queryFirst)
            )
        ) match {
          case Right(replaced) => replaced
          case Left(reason) =>
            logInfo(s"NEAREST BY is left to Spark: $reason")
            join
        }
    }

  private def directionOf(direction: NearestByDirection): RankingDirection =
    direction match {
      case NearestByDistance   => RankingDirection.Distance
      case NearestBySimilarity => RankingDirection.Similarity
    }

  /** Spark's three vector functions, as the connector computes them. */
  private def rankingOf(
      ranking: Expression
  ): Option[NearestByTakeover.Ranking] = ranking match {
    case function @ VectorL2Distance(a, b) =>
      Some(NearestByTakeover.Ranking(MetricType.L2, function.prettyName, a, b))
    case function @ VectorCosineSimilarity(a, b) =>
      Some(
        NearestByTakeover.Ranking(MetricType.Cosine, function.prettyName, a, b)
      )
    case function @ VectorInnerProduct(a, b) =>
      Some(NearestByTakeover.Ranking(MetricType.IP, function.prettyName, a, b))
    case _ => None
  }
}

/** Spark's own computation of one NEAREST BY the rule replaced
  * (docs/design/architecture/dataframe-api.html sections 2 and 4).
  *
  * `execute` puts the query rows it is given in place of the join's query side,
  * as a relation over the same column ids, and runs the join as Spark does:
  * `RewriteNearestByJoin`'s cross join and top k, over the base as the analyzer
  * left it. The table in that base is the one the connector searches, holding
  * the same snapshot.
  */
final case class RewrittenNearestBy(
    join: NearestByJoin,
    function: SparkVectorFunction
) extends SparkNearestBy {

  override def execute(
      spark: SparkSession,
      queries: RDD[InternalRow],
      queryOutput: Seq[Attribute],
      output: Seq[Attribute]
  ): RDD[InternalRow] = {
    val queryRows = Datasets.relation(spark, queryOutput, queries)
    val plan =
      Project(output, RewriteNearestByJoin(join.copy(left = queryRows)))
    spark.sessionState.executePlan(plan).toRdd
  }
}

/** The vector function a NEAREST BY ranks by, called as Spark calls it:
  * `VectorFunctionImplUtils`, with the arguments in the order the ranking names
  * them.
  */
final case class SparkVectorFunction(
    metric: MetricType,
    name: String,
    queryFirst: Boolean
) extends VectorRanking(metric, name, queryFirst) {

  override protected def value(
      first: ArrayData,
      second: ArrayData,
      functionName: UTF8String
  ): java.lang.Float = metric match {
    case MetricType.L2 =>
      VectorFunctionImplUtils.vectorL2Distance(first, second, functionName)
    case MetricType.Cosine =>
      VectorFunctionImplUtils.vectorCosineSimilarity(
        first,
        second,
        functionName
      )
    case MetricType.IP =>
      VectorFunctionImplUtils.vectorInnerProduct(first, second, functionName)
    case other =>
      throw new IllegalStateException(s"No vector function ranks by $other")
  }
}
