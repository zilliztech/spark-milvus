package com.zilliz.spark.connector.connect

/** Fixed V3 reads without Catalog or management CALL registration.
  */
class ConnectReadSessionTest extends ConnectSessionTestBase {
  test("a Connect client reads fixed V3 data") {
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
    printed("views") shouldBe Seq(Seq("0"))
  }

}
