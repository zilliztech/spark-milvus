package com.zilliz.spark.connector.connect

import com.zilliz.spark.connector.extensions.MilvusReadSparkSessionExtensions

/** Fixed V3 reads and searches without Catalog or management CALL registration.
  */
class ConnectReadSessionTest extends ConnectSessionTestBase {
  override protected def sessionExtensions: String =
    classOf[MilvusReadSparkSessionExtensions].getName

  test("a Connect client reads fixed indexes without building them") {
    assume(available, "the native libraries are not on this machine")
    assume(
      clientClasspath.nonEmpty,
      "the build passes the Connect client's classpath in milvus.test.connect.client.classpath"
    )
    val printed =
      client(ConnectReadClientProgram.getClass.getName.stripSuffix("$"))
    printed("rows") shouldBe Seq(Seq("16"))
    printed("repeat") shouldBe Seq(
      Seq(((3000L until 3008L) ++ (3100L until 3108L)).mkString(","))
    )
    val nearest = Seq(Seq("q1", "3000"), Seq("q1", "3100"), Seq("q1", "3101"))
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
