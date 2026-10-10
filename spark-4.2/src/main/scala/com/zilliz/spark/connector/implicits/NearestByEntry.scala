package com.zilliz.spark.connector.implicits

import org.apache.spark.sql.{Column, DataFrame, Dataset}

/** On Spark 4.2 `nearestByJoin` is Spark's own: the connector's rule takes over
  * what it executes (docs/design/architecture/dataframe-api.html section 3).
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
  ): DataFrame = frame.nearestByJoin(
    right,
    rankingExpression,
    numResults,
    mode,
    direction,
    joinType
  )
}
