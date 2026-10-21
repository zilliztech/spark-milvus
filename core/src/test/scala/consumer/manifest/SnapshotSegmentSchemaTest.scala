package consumer.manifest

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.manifest.{
  SnapshotSegmentFixture,
  SnapshotSegmentReader,
  SnapshotSegmentSchema
}

/** Shared full schemas are available without the production snapshot writer. */
class SnapshotSegmentSchemaTest extends AnyFunSuite with Matchers {
  test("every supported schema includes fields beyond the read projection") {
    SnapshotSegmentSchema.supportedVersions shouldBe Seq(1, 2, 3, 4, 5)
    SnapshotSegmentSchema.supportedVersions.foreach { version =>
      val schema = SnapshotSegmentSchema.schemaFor(version)
      schema.getField("json_key_index_files").pos() should be >
        schema.getField("index_files").pos()
      Option(schema.getField("commit_timestamp")).isDefined shouldBe
        (version >= 3)
      Option(schema.getField("manifest_has_index")).isDefined shouldBe
        (version >= 5)
    }
    the[IllegalArgumentException] thrownBy
      SnapshotSegmentSchema.schemaFor(6)
  }

  test("a consumer's schema mutations cannot change another consumer's view") {
    val first = SnapshotSegmentSchema.schemaFor(4)
    first.getField("segment_id").schema().addProp("consumer", "private-writer")
    val second = SnapshotSegmentSchema.schemaFor(4)
    second.getField("segment_id").schema().getProp("consumer") shouldBe null
    SnapshotSegmentReader
      .parse(SnapshotSegmentFixture.encode(), 4)
      .fold(throw _, identity)
      .segmentId shouldBe 30L
  }

}
