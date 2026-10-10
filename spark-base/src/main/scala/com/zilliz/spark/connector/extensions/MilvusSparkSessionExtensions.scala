package com.zilliz.spark.connector.extensions

import org.apache.spark.sql.SparkSessionExtensions

/** What
  * `spark.sql.extensions=com.zilliz.spark.connector.extensions.MilvusSparkSessionExtensions`
  * installs: the parser that recognises `CALL milvus.system.<name>(...)` and
  * the strategy that runs it, and the nearest-by join over a Milvus table: this
  * line's way in (`NearestByExtensions`) and the strategy that plans
  * [[MilvusNearestByJoin]] (docs/design/architecture/dataframe-api.html).
  * Everything else in the session is untouched.
  */
class MilvusSparkSessionExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectParser((_, delegate) => new MilvusSqlParser(delegate))
    extensions.injectPlannerStrategy(_ => CallProcedureStrategy)
    NearestByExtensions(extensions)
    extensions.injectPlannerStrategy(_ => MilvusNearestByJoinStrategy)
  }
}
