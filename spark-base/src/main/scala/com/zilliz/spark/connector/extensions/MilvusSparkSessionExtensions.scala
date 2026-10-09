package com.zilliz.spark.connector.extensions

import org.apache.spark.sql.SparkSessionExtensions

/** What
  * `spark.sql.extensions=com.zilliz.spark.connector.extensions.MilvusSparkSessionExtensions`
  * installs: the parser that recognises `CALL milvus.system.<name>(...)` and
  * the strategy that runs it. Everything else in the session is untouched.
  */
class MilvusSparkSessionExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectParser((_, delegate) => new MilvusSqlParser(delegate))
    extensions.injectPlannerStrategy(_ => CallProcedureStrategy)
  }
}
