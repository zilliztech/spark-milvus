package com.zilliz.spark.connector.extensions

import org.apache.spark.sql.SparkSessionExtensions

/** Offline vector functions, NEAREST BY and its execution strategies. Reading
  * fixed snapshots and searching them does not install management SQL.
  */
class MilvusReadSparkSessionExtensions
    extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    NearestByExtensions(extensions)
    extensions.injectPlannerStrategy(_ => MilvusNearestByJoinStrategy)
  }
}
