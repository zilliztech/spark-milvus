package com.zilliz.spark.connector.connect

import com.zilliz.spark.connector.extensions.MilvusSparkSessionExtensions
import com.zilliz.spark.connector.implicits._

/** Transitional write coverage retained until the public write APIs retire. */
class ConnectSessionTest extends ConnectSessionTestBase {
  override protected def sessionExtensions: String =
    classOf[MilvusSparkSessionExtensions].getName

  test("a Connect client builds, delivers and searches as a classic one does") {
    assume(available, "the native libraries are not on this machine")
    assume(
      clientClasspath.nonEmpty,
      "the build passes the Connect client's classpath in milvus.test.connect.client.classpath"
    )
    val printed = client(ConnectClientProgram.getClass.getName.stripSuffix("$"))

    val classic = spark.read
      .format("milvus")
      .options(options)
      .load()
      .buildIndex("vector", "built/classic", metric = "IP", buildId = 7101L)
      .collect()
      .map(row =>
        Seq(
          row.getAs[Long]("segment_id"),
          row.getAs[Long]("row_count"),
          row.getAs[Long]("build_id")
        ).map(_.toString)
      )
      .toSeq
      .sortBy(_.head)
    printed("built") shouldBe classic
    printed("written").map(_.tail) shouldBe Seq(Seq("2", "2"))
    printed("filtered").head.head should include("its plan holds Filter")

    val nearest = Seq(Seq("q1", "3105"), Seq("q1", "3106"), Seq("q1", "3107"))
    if (line == "4.2") printed("method") shouldBe nearest
    else
      printed("method").head.head should include(
        "nearestByJoin runs on a classic Spark session; on Spark Connect write " +
          "it in SQL as nearest_by_join"
      )
    printed("sql") shouldBe nearest
    printed("views") shouldBe Seq(Seq("0"))
  }
}
