package com.zilliz.spark.connector.write

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.milvus.storage.snapshot.SnapshotLayout

class WritableManifestPathsTest extends AnyFunSuite with Matchers {
  private val remote = Map(
    StorageProperties.BucketName -> "bucket",
    StorageProperties.Address -> "store:9000",
    StorageProperties.RootPath -> "files"
  )
  private def source(path: String): SourceSnapshot =
    SourceSnapshot(path, 7L, "source", 10L, 0)
  private val segment = "insert_log/10/20/30"
  private def plan(base: String, id: Long = 30L): PlannedSegment =
    PlannedSegment(id, 20L, base, sourceVersion = 1L, rows = 2L)

  test(
    "native and exported snapshots keep writable segments in the selected prefix"
  ) {
    Seq(
      "scoped/snapshots/10/metadata/7.json",
      "s3://bucket/scoped/snapshots/10/metadata/7.json",
      "minio://store:9000/bucket/scoped/snapshots/10/metadata/7.json",
      "scoped/snapshot.json",
      "scoped/snapshots/before_update.json"
    ).foreach { path =>
      val targets = WritableManifestPaths(source(path), remote)
      Seq(s"scoped/$segment", s"scoped/files/$segment").foreach { base =>
        withClue(path) { targets.validate(Seq(plan(base)), None) }
      }
    }
  }

  test("a writable segment must match every ID and the entire canonical base") {
    val targets = WritableManifestPaths(source("scoped/snapshot.json"), remote)
    Seq(
      "scoped/files/insert_log/11/20/30",
      "scoped/files/insert_log/10/21/30",
      "scoped/files/insert_log/10/20/31",
      s"files/$segment",
      s"scoped/archive/$segment",
      s"scoped/files/$segment/extra",
      s"scoped/./files/$segment",
      s"scoped/sub/../files/$segment"
    ).foreach { base =>
      withClue(base) {
        intercept[IllegalArgumentException](
          targets.validate(Seq(plan(base)), None)
        ).getMessage should include("writable location")
      }
    }
  }

  test("explicit layouts select exactly one data root") {
    val paths = WritableManifestPaths(source("scoped/snapshot.json"), remote)
    val referenced = Seq(plan(s"scoped/$segment"))
    val exported = Seq(plan(s"scoped/files/$segment"))
    paths.validate(referenced, Some(SnapshotLayout.Referenced))
    paths.validate(exported, Some(SnapshotLayout.SelfContained))
    intercept[IllegalArgumentException] {
      paths.validate(exported, Some(SnapshotLayout.Referenced))
    }.getMessage should include("writable location")
    intercept[IllegalArgumentException] {
      paths.validate(referenced, Some(SnapshotLayout.SelfContained))
    }.getMessage should include("writable location")
    intercept[IllegalArgumentException] {
      paths.validate(exported, Some(SnapshotLayout.Unsupported("3")))
    }.getMessage should include("does not support snapshot layout")
  }

  test("a legacy snapshot cannot mix referenced and exported segment roots") {
    val paths = WritableManifestPaths(source("scoped/snapshot.json"), remote)
    val segments = Seq(
      plan(s"scoped/$segment"),
      plan("scoped/files/insert_log/10/20/31", 31L)
    )
    intercept[IllegalArgumentException](
      paths.validate(segments, None)
    ).getMessage should include("single data root")
  }

  test("the DataCoord metadata path must identify the declared snapshot") {
    Seq(
      "scoped/snapshots/11/metadata/7.json",
      "scoped/snapshots/10/metadata/8.json"
    ).foreach { path =>
      intercept[IllegalArgumentException](
        WritableManifestPaths(source(path), remote)
      ).getMessage should include("does not match collection 10 and snapshot 7")
    }
  }

  test("local segment paths may be rooted or relative") {
    val local = Map(
      StorageProperties.StorageType -> StorageProperties.StorageTypeLocal,
      StorageProperties.RootPath -> "/data"
    )
    Seq("snapshot.json", "/data/snapshot.json", "file:///data/snapshot.json")
      .foreach { path =>
        val targets = WritableManifestPaths(source(path), local)
        Seq(segment, s"files/$segment", s"/data/files/$segment").foreach {
          base =>
            targets.validate(Seq(plan(base)), None)
        }
      }
  }
}
