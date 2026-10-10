package com.zilliz.spark.connector.extensions

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.classic.{
  Dataset => ClassicDataset,
  SparkSession => ClassicSession
}
import org.apache.spark.sql.execution.LogicalRDD

/** A DataFrame from a logical plan, and a relation over rows, on this line:
  * Spark's own `Dataset.ofRows` is internal, so the plan is analyzed first and
  * the DataFrame made with the public constructor and a row encoder of its
  * schema (docs/design/architecture/dataframe-api.html section 6). Both need
  * the classic session, which a plan only ever runs in.
  */
private[connector] object Datasets {

  def ofRows(spark: SparkSession, plan: LogicalPlan): DataFrame = {
    val session = classic(spark)
    val execution = session.sessionState.executePlan(plan)
    execution.assertAnalyzed()
    val analyzed = execution.analyzed
    new ClassicDataset[Row](session, analyzed, Encoders.row(analyzed.schema))
  }

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
