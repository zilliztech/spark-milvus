package com.zilliz.milvus.storage.write.commit

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class RegistrationTest extends AnyFunSuite with Matchers {

  private val backfilled =
    CommittedSegment(0, "files/insert_log/1/2/30", 3L, 100L, Some(30L))
  private val appended =
    CommittedSegment(1, "files/staging/j/1/task_1_2", 1L, 50L)

  test(
    "a backfill job's segments become (segment id, manifest version) items"
  ) {
    Registration.backfillItems(JobManifest("j", 1L, Seq(backfilled))) shouldBe
      Right(Seq(Registration.Item(30L, 3L)))
  }

  test("a job that created a segment is refused as a whole") {
    val result = Registration.backfillItems(
      JobManifest("j", 1L, Seq(backfilled, appended))
    )
    result.isLeft shouldBe true
    result.left.toOption.get should include("1 new segment(s)")
    result.left.toOption.get should include("RegisterSegments")
  }

  test("a job with no segment is refused") {
    Registration
      .backfillItems(JobManifest("j", 1L, Seq.empty))
      .isLeft shouldBe true
  }

  test("the segment id is in the JSON when known and absent otherwise") {
    val json = JobManifest("j", 1L, Seq(backfilled, appended)).toJson
    json should include("\"segment_id\" : 30")
    json.split("segment_id").length shouldBe 2
    JobManifest.fromJson(json) shouldBe Right(
      JobManifest("j", 1L, Seq(backfilled, appended))
    )
  }
}
