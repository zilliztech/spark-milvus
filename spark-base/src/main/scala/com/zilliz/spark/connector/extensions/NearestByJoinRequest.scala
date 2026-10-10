package com.zilliz.spark.connector.extensions

import java.util.Locale

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType, LeftOuter}
import org.apache.spark.sql.catalyst.plans.logical.{BinaryNode, LogicalPlan}

/** The order a nearest-by join ranks in: the smallest value first, or the
  * largest.
  */
sealed abstract class RankingDirection(val name: String) extends Serializable

object RankingDirection {
  case object Distance extends RankingDirection("distance")
  case object Similarity extends RankingDirection("similarity")
}

/** A nearest-by join as written on a Spark line without Spark's
  * `NearestByJoin`, 3.5 to 4.1: the same fields, built by `nearestByJoin` and
  * by the table function `nearest_by_join`
  * (docs/design/architecture/dataframe-api.html section 6).
  *
  * The analyzer resolves its ranking expression against the two sides, and
  * `ReplaceNearestByJoinRequest` then replaces it, with [[MilvusNearestByJoin]]
  * when the connector executes it and with the general plan otherwise. Its
  * output is Spark 4.2's: the query columns and the base columns, both
  * nullable.
  */
final case class NearestByJoinRequest(
    left: LogicalPlan,
    right: LogicalPlan,
    joinType: JoinType,
    approx: Boolean,
    numResults: Int,
    rankingExpression: Expression,
    direction: RankingDirection
) extends BinaryNode {

  override def output: Seq[Attribute] =
    left.output.map(_.withNullability(true)) ++
      right.output.map(_.withNullability(true))

  override protected def withNewChildrenInternal(
      newLeft: LogicalPlan,
      newRight: LogicalPlan
  ): NearestByJoinRequest = copy(left = newLeft, right = newRight)
}

/** Spark 4.2's checks of a nearest-by join's arguments, with its messages:
  * `NEAREST_BY_JOIN.*` does not exist before 4.2, so they fail with
  * `IllegalArgumentException` (docs/design/architecture/dataframe-api.html
  * section 6).
  */
object NearestByArguments {

  /** Spark 4.2's `NearestByJoin.MaxNumResults`. */
  val MaxNumResults = 100000

  private def failure(subclass: String, message: String) =
    new IllegalArgumentException(
      s"[NEAREST_BY_JOIN.$subclass] Invalid nearest-by join. $message SQLSTATE: 42604"
    )

  def numResults(k: Int): Int = {
    if (k < 1 || k > MaxNumResults)
      throw failure(
        "NUM_RESULTS_OUT_OF_RANGE",
        s"The number of results $k must be between 1 and $MaxNumResults. " +
          s"Update the literal in `APPROX NEAREST $k BY ...` (or `EXACT NEAREST $k BY ...`) " +
          "to fall within that range."
      )
    k
  }

  /** True for APPROX, false for EXACT. */
  def approx(mode: String): Boolean =
    String.valueOf(mode).toLowerCase(Locale.ROOT) match {
      case "approx" => true
      case "exact"  => false
      case _ =>
        throw failure(
          "UNSUPPORTED_MODE",
          s"Unsupported nearest-by join mode '$mode'. Supported modes include: 'approx', 'exact'."
        )
    }

  def direction(direction: String): RankingDirection =
    String.valueOf(direction).toLowerCase(Locale.ROOT) match {
      case "distance"   => RankingDirection.Distance
      case "similarity" => RankingDirection.Similarity
      case _ =>
        throw failure(
          "UNSUPPORTED_DIRECTION",
          s"Unsupported nearest-by join direction '$direction'. Supported nearest-by join " +
            "directions include: 'distance', 'similarity'."
        )
    }

  def joinType(joinType: String): JoinType =
    String.valueOf(joinType).toLowerCase(Locale.ROOT).replace("_", "") match {
      case "inner"              => Inner
      case "leftouter" | "left" => LeftOuter
      case _ =>
        throw failure(
          "UNSUPPORTED_JOIN_TYPE",
          s"Unsupported nearest-by join type $joinType. Supported types: 'INNER', 'LEFT OUTER'."
        )
    }

  /** What Spark 4.2's CheckAnalysis reports about a resolved nearest-by join.
    */
  def checkResolved(
      request: NearestByJoinRequest,
      crossJoinEnabled: Boolean,
      orderable: Boolean
  ): Unit = {
    if (!crossJoinEnabled)
      throw failure(
        "CROSS_JOIN_NOT_ENABLED",
        "Nearest-by join is implemented as a bounded cross-product internally and is " +
          "therefore rejected when `spark.sql.crossJoin.enabled = false`. Set " +
          "`spark.sql.crossJoin.enabled = true` to permit it, or rewrite the query " +
          "without nearest-by."
      )
    if (request.left.isStreaming || request.right.isStreaming)
      throw failure(
        "STREAMING_NOT_SUPPORTED",
        "Nearest-by join is not supported with streaming DataFrames/Datasets."
      )
    if (!orderable)
      throw failure(
        "NON_ORDERABLE_RANKING_EXPRESSION",
        s"The ranking expression \"${request.rankingExpression.sql}\" of type " +
          s"\"${request.rankingExpression.dataType.sql}\" is not orderable. Provide an " +
          "expression that returns an orderable type, such as a numeric distance like " +
          "abs(a.col - b.col) or a numeric similarity score."
      )
  }
}
