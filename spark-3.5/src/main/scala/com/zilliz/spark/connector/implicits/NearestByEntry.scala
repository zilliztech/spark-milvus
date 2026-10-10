package com.zilliz.spark.connector.implicits

import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.plans.logical.{Join, Project}

import com.zilliz.spark.connector.extensions.{
  Datasets,
  NearestByArguments,
  NearestByJoinRequest
}

/** `nearestByJoin` on Spark 3.5, which has no NearestByJoin: the arguments
  * checked as Spark 4.2 checks them, and a [[NearestByJoinRequest]] for the
  * analyzer (docs/design/architecture/dataframe-api.html section 6).
  *
  * The ranking is resolved by selecting it over the cross join of the two
  * frames, which is also where a query side and a base read from the same table
  * get their own column ids; the join's two sides are the request's.
  */
private[implicits] object NearestByEntry {

  def nearestByJoin(
      frame: DataFrame,
      right: Dataset[_],
      rankingExpression: Column,
      numResults: Int,
      mode: String,
      direction: String,
      joinType: String
  ): DataFrame = {
    val join = NearestByArguments.joinType(joinType)
    val approx = NearestByArguments.approx(mode)
    val ranking = NearestByArguments.direction(direction)
    val k = NearestByArguments.numResults(numResults)
    frame
      .crossJoin(right)
      .select(rankingExpression.as("__ranking"))
      .queryExecution
      .analyzed match {
      case Project(Seq(Alias(expression, _)), Join(left, base, _, _, _)) =>
        Datasets.ofRows(
          frame.sparkSession,
          NearestByJoinRequest(left, base, join, approx, k, expression, ranking)
        )
      case other =>
        throw new IllegalStateException(
          s"The ranking was resolved over ${other.nodeName}, not a cross join"
        )
    }
  }
}
