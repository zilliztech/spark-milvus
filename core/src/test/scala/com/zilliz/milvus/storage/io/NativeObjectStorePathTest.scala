package com.zilliz.milvus.storage.io

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.credential.StorageProperties

/** The filesystem path a local key names, without a native library. */
class NativeObjectStorePathTest extends AnyFunSuite with Matchers {
  private def local(root: String): Map[String, String] =
    Map(
      StorageProperties.StorageType -> StorageProperties.StorageTypeLocal,
      StorageProperties.RootPath -> root
    )

  test("a local key names one path whether or not it carries its root") {
    val cases = Seq(
      ("files", "files/results/run", "files/results/run"),
      ("files", "results/run", "files/results/run"),
      ("files/", "files/results/run", "files/results/run"),
      ("files", "files", "files"),
      // Only a whole leading segment is the root.
      ("files", "filesystem/run", "files/filesystem/run"),
      ("/data/root", "/data/root/results/run", "/data/root/results/run"),
      ("/data/root", "results/run", "/data/root/results/run"),
      ("/", "data/results/run", "/data/results/run")
    )
    cases.foreach { case (root, key, path) =>
      withClue(s"root=$root key=$key: ") {
        NativeObjectStore.localPath(local(root), key) shouldBe Some(path)
      }
    }
  }

  test("object storage has no local path") {
    NativeObjectStore.localPath(
      Map(
        StorageProperties.StorageType -> "remote",
        StorageProperties.BucketName -> "bucket",
        StorageProperties.RootPath -> "files"
      ),
      "files/results/run"
    ) shouldBe None
  }
}
