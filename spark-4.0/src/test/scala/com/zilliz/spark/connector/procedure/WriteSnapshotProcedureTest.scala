package com.zilliz.spark.connector.procedure

import org.apache.spark.sql.types.{LongType, StringType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.write.commit.SourceSnapshot

/** What `CALL milvus.system.write_snapshot(...)` takes and answers with, and
  * how it holds to the snapshot the build job recorded
  * (docs/design/architecture/vector-search.html section 2.7,
  * docs/design/architecture/dataframe-api.html section 9).
  */
class WriteSnapshotProcedureTest extends AnyFunSuite with Matchers {

  private val source = SourceSnapshot(
    key = "files/snapshots/10/metadata/5000.json",
    bucket = "milvus-bucket",
    collectionId = 10L,
    name = "s-5000"
  )

  test("the call names the table, the build job and where it wrote") {
    WriteSnapshotProcedure.parameters
      .filter(_.required)
      .map(_.name) shouldBe Seq("job", "input")
    WriteSnapshotProcedure.parameters
      .filterNot(_.required)
      .map(_.name) shouldBe Seq(
      "collection",
      "output",
      "snapshot_id",
      "snapshot_name",
      "restorable",
      "table"
    )
    WriteSnapshotProcedure.parameters.head.name shouldBe "collection"
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

  test("the call says how to check the collection before any store is opened") {
    val args = Map[String, Any](
      "collection" -> "c",
      "job" -> "index-1",
      "input" -> "built"
    )

    the[IllegalArgumentException] thrownBy WriteSnapshotProcedure.run(
      ProcedureArgs(args, Map.empty)
    ) should have message
      "requirement failed: write_snapshot checks the collection through 'milvus.uri', or through " +
      "'milvus.snapshot.path' naming the snapshot the job was built from; give one"
    val backup = the[IllegalArgumentException] thrownBy WriteSnapshotProcedure
      .run(ProcedureArgs(args, Map("milvus.backup.dir" -> "backup/b1")))
    backup.getMessage should include("a backup export is not one")
  }

  test("a snapshot path the call still gives has to name the recorded one") {
    Seq(
      "files/snapshots/10/metadata/5000.json",
      "s3a://milvus-bucket/files/snapshots/10/metadata/5000.json"
    ).foreach(
      WriteSnapshotProcedure.checkPath(
        _,
        "milvus-bucket",
        "",
        local = false,
        source,
        "index-1"
      )
    )

    val newer = the[IllegalArgumentException] thrownBy WriteSnapshotProcedure
      .checkPath(
        "files/snapshots/10/metadata/6000.json",
        "milvus-bucket",
        "",
        local = false,
        source,
        "index-1"
      )
    newer.getMessage should include(
      "job index-1 was built from 'files/snapshots/10/metadata/5000.json'"
    )
    the[IllegalArgumentException] thrownBy WriteSnapshotProcedure.checkPath(
      "s3a://other-bucket/files/snapshots/10/metadata/5000.json",
      "milvus-bucket",
      "",
      local = false,
      source,
      "index-1"
    )
  }

  test(
    "the collection the call names has to be the one the job was built over"
  ) {
    WriteSnapshotProcedure.checkCollectionId(10L, source, "index-1")

    val failure = the[IllegalArgumentException] thrownBy WriteSnapshotProcedure
      .checkCollectionId(11L, source, "index-1")
    failure.getMessage should include(
      "names collection 11; job index-1 was built over collection 10"
    )
  }
}
