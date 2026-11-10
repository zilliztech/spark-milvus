package com.zilliz.spark.connector.connect

import org.apache.spark.sql.SparkSession

/** Read-only client for fixed V3 snapshots in [[ConnectReadSessionTest]]. It
  * runs in a JVM whose classpath is the Spark Connect client and this
  * connector's classes, with no classic Spark, connects to the server the suite
  * started, and prints what it gets, one fact a line, fields separated by tabs.
  *
  * Arguments: the server's `sc://` URL, the Spark line, then the read options
  * of the collection as `key=value`.
  */
object ConnectReadClientProgram {

  def main(args: Array[String]): Unit = {
    val url = args(0)
    val line = args(1)
    val options = args
      .drop(2)
      .map { option =>
        val at = option.indexOf('=')
        option.substring(0, at) -> option.substring(at + 1)
      }
      .toMap
    val spark = SparkSession.builder().remote(url).getOrCreate()
    try run(spark, line, options)
    finally spark.stop()
  }

  private def say(fields: Any*): Unit = println(fields.mkString("\t"))

  private def run(
      spark: SparkSession,
      line: String,
      options: Map[String, String]
  ): Unit = {
    val delivered = spark.read.format("milvus").options(options).load()
    say("rows", delivered.count())
    say(
      "repeat",
      delivered.select("id").collect().map(_.getLong(0)).sorted.mkString(",")
    )
    say(
      "views",
      spark.catalog
        .listTables()
        .collect()
        .count(_.name.startsWith("milvus_input_"))
    )
  }
}
