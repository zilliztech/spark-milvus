package com.zilliz.spark.connector.metrics

import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.SparkContext

/** The SQL metrics of the plan node that runs a nearest-by join: the search's
  * counters ([[SearchMetrics]]), its output rows, and what the driver settles
  * before the search runs, namely how the query rows divide and how the
  * segments are searched (docs/design/architecture/dataframe-api.html sections
  * 2 and 4).
  */
object NearestByMetrics {

  val OutputRows = "numOutputRows"

  /** Query rows whose vector Knowhere scores as Spark's function does. */
  val SearchedQueries = "searchedQueries"

  /** Query rows Spark's own execution of the join answers. */
  val SparkQueries = "sparkQueries"

  /** Query rows whose ranking value is NULL against every base row. */
  val UnrankedQueries = "unrankedQueries"

  val IndexSegments = "indexSegments"
  val ExactSegments = "exactSegments"

  def create(context: SparkContext): Map[String, SQLMetric] =
    SearchMetrics.sqlMetrics(context) ++ Map(
      OutputRows -> SQLMetrics.createMetric(context, "number of output rows"),
      SearchedQueries -> SQLMetrics
        .createMetric(context, "query rows searched"),
      SparkQueries ->
        SQLMetrics.createMetric(context, "query rows joined by Spark"),
      UnrankedQueries ->
        SQLMetrics.createMetric(context, "query rows without a ranking value"),
      IndexSegments ->
        SQLMetrics.createMetric(context, "segments searched by index"),
      ExactSegments ->
        SQLMetrics.createMetric(context, "segments scanned exactly")
    )

  /** Sets numbers the driver counted and sends them to the SQL execution
    * running on this thread, which shows them on the node.
    */
  def postDriverValues(
      context: SparkContext,
      metrics: Map[String, SQLMetric],
      values: Map[String, Long]
  ): Unit = {
    values.foreach { case (name, value) => metrics(name).set(value) }
    SQLMetrics.postDriverMetricUpdates(
      context,
      context.getLocalProperty(SQLExecution.EXECUTION_ID_KEY),
      values.keys.toSeq.map(metrics)
    )
  }
}
