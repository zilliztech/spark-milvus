package com.zilliz.spark.connector.connect

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.call_function
import org.apache.spark.sql.types.{
  ArrayType,
  FloatType,
  StringType,
  StructField,
  StructType
}

import com.zilliz.spark.connector.implicits._

/** Read-only client for fixed persisted indexes in [[ConnectSessionTest]]. It
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

  /** Every message in a failure's cause chain, on one line. */
  private def failure(run: => Any): String =
    try {
      run
      "no failure"
    } catch {
      case thrown: Exception =>
        Iterator
          .iterate[Throwable](thrown)(_.getCause)
          .takeWhile(_ != null)
          .map(e => String.valueOf(e.getMessage))
          .mkString(" ")
          .replaceAll("\\s+", " ")
    }

  private def hits(label: String, frame: DataFrame): Unit =
    frame
      .collect()
      .map(row => (row.getString(0), row.getLong(1)))
      .sorted
      .foreach { case (qid, id) => say(label, qid, id) }

  private def run(
      spark: SparkSession,
      line: String,
      options: Map[String, String]
  ): Unit = {
    val delivered = spark.read.format("milvus").options(options).load()
    say("rows", delivered.count())
    val queries = spark.createDataFrame(
      Seq(Row("q1", Seq(1f, 3f))).asJava,
      StructType(
        Seq(
          StructField("qid", StringType),
          StructField("qv", ArrayType(FloatType))
        )
      )
    )
    val ranking =
      call_function("vector_l2_distance", queries("qv"), delivered("vector"))
    if (line == "4.2")
      hits(
        "method",
        queries
          .nearestByJoin(delivered, ranking, 3, "approx", "distance")
          .select("qid", "id")
      )
    else
      say(
        "method",
        failure(
          queries.nearestByJoin(delivered, ranking, 3, "approx", "distance")
        )
      )

    queries.createOrReplaceTempView("connect_queries")
    delivered.createOrReplaceTempView("connect_docs")
    hits(
      "sql",
      spark.sql(
        if (line == "4.2")
          "SELECT q.qid, d.id FROM connect_queries q JOIN connect_docs d " +
            "APPROX NEAREST 3 BY DISTANCE vector_l2_distance(q.qv, d.vector)"
        else
          "SELECT query.qid, base.id FROM nearest_by_join(TABLE(connect_queries), " +
            "TABLE(connect_docs), 'vector_l2_distance(query.qv, base.vector)', 3, " +
            "'approx', 'distance')"
      )
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
