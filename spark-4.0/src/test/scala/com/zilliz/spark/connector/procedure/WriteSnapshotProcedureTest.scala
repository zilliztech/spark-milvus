package com.zilliz.spark.connector.procedure

import org.apache.spark.sql.types.{LongType, StringType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** What `CALL milvus.system.write_snapshot(...)` takes and answers with
  * (docs/design/architecture/vector-search.html section 2.7).
  */
class WriteSnapshotProcedureTest extends AnyFunSuite with Matchers {

  test("the call names the collection, the build job and where it wrote") {
    WriteSnapshotProcedure.parameters
      .filter(_.required)
      .map(_.name) shouldBe Seq("collection", "job", "input")
    WriteSnapshotProcedure.parameters
      .filterNot(_.required)
      .map(_.name) shouldBe Seq("output", "snapshot_id", "snapshot_name")
    WriteSnapshotProcedure.parameters
      .find(_.name == "snapshot_id")
      .map(_.dataType) shouldBe Some(LongType)
    WriteSnapshotProcedure.parameters
      .find(_.name == "job")
      .map(_.dataType) shouldBe Some(StringType)
  }

  test("the row says where the snapshot is and what it covers") {
    WriteSnapshotProcedure.outputSchema.fieldNames.toSeq shouldBe Seq(
      "snapshot",
      "snapshot_id",
      "snapshot_name",
      "segments",
      "indexes",
      "bytes"
    )
  }

  test("a job id is one key component and a prefix is not a URI") {
    val args = Map[String, Any](
      "collection" -> "c",
      "job" -> "index-1/../..",
      "input" -> "built"
    )
    the[IllegalArgumentException] thrownBy WriteSnapshotProcedure.run(
      ProcedureArgs(args, Map.empty)
    ) should have message
      "requirement failed: 'job' is one storage key component, not 'index-1/../..'"

    the[IllegalArgumentException] thrownBy WriteSnapshotProcedure.run(
      ProcedureArgs(
        args + ("job" -> "index-1") + ("input" -> "s3://bucket/built"),
        Map.empty
      )
    ) should have message
      "requirement failed: 'input' is a prefix inside the bucket, not a URI: 's3://bucket/built'"
  }
}
