package com.zilliz.spark.connector.implicits

import org.apache.spark.sql.{Column, DataFrame, Dataset}

/** Search methods available on a DataFrame, with version-specific entry
  * adaptation isolated from Spark Connect clients.
  */
final class MilvusDataFrame(frame: DataFrame) {

  /** Spark 4.2's `Dataset.nearestByJoin`, INNER: for each row of this frame,
    * the `numResults` rows of `right` the ranking puts first.
    */
  def nearestByJoin(
      right: Dataset[_],
      rankingExpression: Column,
      numResults: Int,
      mode: String,
      direction: String
  ): DataFrame =
    nearestByJoin(
      right,
      rankingExpression,
      numResults,
      mode,
      direction,
      "inner"
    )

  /** Spark 4.2's `Dataset.nearestByJoin`, INNER or LEFT OUTER. */
  def nearestByJoin(
      right: Dataset[_],
      rankingExpression: Column,
      numResults: Int,
      mode: String,
      direction: String,
      joinType: String
  ): DataFrame = NearestByEntry.nearestByJoin(
    frame,
    right,
    rankingExpression,
    numResults,
    mode,
    direction,
    joinType
  )

}
