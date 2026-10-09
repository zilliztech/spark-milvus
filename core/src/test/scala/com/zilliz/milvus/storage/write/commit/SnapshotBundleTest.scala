package com.zilliz.milvus.storage.write.commit

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** The rule Milvus's external restore applies to a snapshot's paths, stated
  * once so that a write and a restore refuse the same snapshots
  * (docs/design/architecture/vector-search.html section 2.7).
  */
class SnapshotBundleTest extends AnyFunSuite with Matchers {

  test(
    "the restore root is the prefix above snapshots/<collection>/metadata/<id>.json"
  ) {
    SnapshotBundle.rootOf("files/snapshots/10/metadata/5.json") shouldBe Some(
      "files"
    )
    SnapshotBundle.rootOf("a/b/snapshots/10/metadata/5.json") shouldBe Some(
      "a/b"
    )
    SnapshotBundle.rootOf("snapshots/10/metadata/5.json") shouldBe Some("")
    SnapshotBundle.rootOf("/files/snapshots/10/metadata/5.json") shouldBe Some(
      "files"
    )
    SnapshotBundle.rootOf(
      "files/snapshots/10/manifests/5/30.avro"
    ) shouldBe None
    SnapshotBundle.rootOf("files/built/5.json") shouldBe None
    SnapshotBundle.rootOf("") shouldBe None
  }

  test(
    "a path is outside the root unless the root is a prefix of its key in the same bucket"
  ) {
    SnapshotBundle.outsideRoot(
      "files",
      Seq(
        "files/insert_log/10/20/30",
        "files",
        "s3://b/files/index_files/1",
        "built/index_files/1",
        "s3://other/files/x",
        "filesystem/x"
      ),
      "b"
    ) shouldBe Seq("built/index_files/1", "s3://other/files/x", "filesystem/x")

    SnapshotBundle.outsideRoot("", Seq("anything/at/all"), "b") shouldBe empty
    SnapshotBundle.outsideRoot(
      "/files/",
      Seq("files/a", "other/a"),
      ""
    ) shouldBe
      Seq("other/a")
    SnapshotBundle.outsideRoot("files", Seq("files/a", "files/a"), "") shouldBe
      empty
  }

  test("what is outside is described with the root and the first few paths") {
    SnapshotBundle.describeOutside(
      "built/",
      Seq("a", "b", "c", "d", "e", "f", "g")
    ) shouldBe "7 path(s) are outside the root 'built': a, b, c, d, e and 2 more"
    SnapshotBundle.describeOutside("", Seq("x")) shouldBe
      "1 path(s) are outside the root '': x"
  }
}
