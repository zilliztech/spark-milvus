package com.zilliz.spark.connector.write

import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.write.Write

/** Spark 4 reports driver write outcomes after both successful and failed
  * writes.
  */
trait DriverMeasuredWrite extends Write {
  protected def driverMetricsValues: Array[CustomTaskMetric]

  override def reportDriverMetrics(): Array[CustomTaskMetric] =
    driverMetricsValues
}
