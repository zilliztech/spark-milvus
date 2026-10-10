package com.zilliz.spark.connector.extensions

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.SparkSession

import com.zilliz.milvus.storage.index.RankingFunction

/** Spark's own computation of one NEAREST BY, which the Spark line that has the
  * syntax gives a [[MilvusNearestByJoin]] when it replaces the join
  * (docs/design/architecture/dataframe-api.html sections 2 and 4).
  *
  * The connector computes what Knowhere computes as Spark would. The rest is
  * Spark's: `function` scores the base rows Knowhere is not given, and
  * `execute` runs the join, as Spark runs it, for the query rows the connector
  * does not search.
  */
trait SparkNearestBy {

  /** The ranking function, as the executors call it. */
  def function: RankingFunction

  /** The join of `queries`, rows of `queryOutput`, with the join's base, as
    * Spark executes it, in the columns `output`. Called on the driver.
    */
  def execute(
      spark: SparkSession,
      queries: RDD[InternalRow],
      queryOutput: Seq[Attribute],
      output: Seq[Attribute]
  ): RDD[InternalRow]
}
