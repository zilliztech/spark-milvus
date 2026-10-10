package com.zilliz.spark.connector.implicits

import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.plans.logical.{Join, Project}

import com.zilliz.spark.connector.extensions.{
  Datasets,
  NearestByArguments,
  NearestByJoinRequest
}

/** `nearestByJoin` on Spark 4.1, which has no NearestByJoin
  * (docs/design/architecture/dataframe-api.html section 6). A Spark Connect
  * DataFrame is not a plan this JVM can build on; its SQL form is the table
  * function `nearest_by_join`. The session is told by its class name, and the
  * plan is built in [[ClassicNearestByJoin]]: a Connect client has neither the
  * classic nor the catalyst classes, and the JVM loads some of the classes a
  * class refers to when it checks that class, so this object refers to none.
  */
private[implicits] object NearestByEntry {

  private val ClassicDataset = "org.apache.spark.sql.classic.Dataset"

  def nearestByJoin(
      frame: DataFrame,
      right: Dataset[_],
      rankingExpression: Column,
      numResults: Int,
      mode: String,
      direction: String,
      joinType: String
  ): DataFrame =
    if (frame.getClass.getName != ClassicDataset)
      throw new IllegalArgumentException(
        "nearestByJoin runs on a classic Spark session; on Spark Connect write " +
          "it in SQL as nearest_by_join(TABLE(query), TABLE(base), 'ranking', k, " +
          "mode, direction[, join_type])"
      )
    else
      ClassicNearestByJoin(
        frame,
        right,
        rankingExpression,
        numResults,
        mode,
        direction,
        joinType
      )
}

/** `nearestByJoin` on a classic session: the arguments checked as Spark 4.2
  * checks them, and a [[NearestByJoinRequest]] for the analyzer.
  *
  * The ranking is resolved by selecting it over the cross join of the two
  * frames, which is also where a query side and a base read from the same table
  * get their own column ids; the join's two sides are the request's.
  */
private[implicits] object ClassicNearestByJoin {

  def apply(
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
