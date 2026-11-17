package com.zilliz.spark.connector.write

import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.write.Write

/** Spark 3.5 has task write metrics but no driver write metric callback. The
  * operational plugin exports driver outcomes on this profile.
  */
trait DriverMeasuredWrite extends Write {
  protected def driverMetricsValues: Array[CustomTaskMetric]
}
