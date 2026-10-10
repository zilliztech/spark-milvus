package com.zilliz.spark.connector.extensions

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.classic.{SparkSession => ClassicSession}
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.SparkSession

/** A relation over rows on this line, which `RewrittenNearestBy` puts in place
  * of a join's query side (docs/design/architecture/dataframe-api.html section
  * 4). It needs the classic session, which a plan only ever runs in; Spark
  * 4.2's own `nearestByJoin` builds the DataFrames.
  */
private[connector] object Datasets {

  def relation(
      spark: SparkSession,
      output: Seq[Attribute],
      rows: RDD[InternalRow]
  ): LogicalPlan = LogicalRDD(output, rows)(classic(spark))

  private def classic(spark: SparkSession): ClassicSession = spark match {
    case session: ClassicSession => session
    case other =>
      throw new IllegalStateException(
        s"A plan runs on a classic session, not ${other.getClass.getName}"
      )
  }
}
