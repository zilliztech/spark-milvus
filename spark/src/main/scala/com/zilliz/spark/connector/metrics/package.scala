package com.zilliz.spark.connector

/** DataSource read and write metrics and their task aggregation. SQL metrics
  * describe operation progress and the Arrow batch, JNI, copy and allocation
  * costs of snapshot scans. The optional Spark plugin reports cumulative
  * attempt work and Driver-confirmed outcomes through Spark's metric registry,
  * using a bounded set of metric names. Operation and task identities are
  * internal correlation keys, never metric labels. Driver state joins task
  * snapshots with Spark's accepted task results, tracks each active operation's
  * progress independently, and retires state at execution end.
  */
package object metrics
