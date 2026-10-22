package com.zilliz.spark.connector.extensions

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.SparkSession

import com.zilliz.milvus.storage.vector.RankingFunction

/** Spark's own computation of one NEAREST BY, which the Spark line that has the
  * syntax gives a [[MilvusNearestByJoin]] when it replaces the join.
  *
  * The connector ranks candidates by Float engine-scale scores, including Float
  * squared distances for L2. `function` scores base rows Knowhere is not given,
  * and `execute` runs the join, as Spark runs it, for the query rows the
  * connector does not search.
  */
trait SparkNearestBy {

  /** The ranking function, as the executors call it. */
  def function: RankingFunction

  /** The same join over `base` and `ranking`, the join's base and ranking as
    * the optimizer left them, which [[MilvusNearestByJoinStrategy]] gives the
    * node it plans. The node's output columns are the optimized plan's, and the
    * analyzed base may not produce them: the optimizer gives a base column the
    * id of the column below it when it removes the alias a view put over it.
    */
  def over(base: LogicalPlan, ranking: Expression): SparkNearestBy

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
