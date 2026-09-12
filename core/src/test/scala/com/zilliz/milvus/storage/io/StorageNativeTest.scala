package com.zilliz.milvus.storage.io

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import scala.collection.JavaConverters._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

import com.zilliz.milvus.jni.storage.{StorageNative, StorageNativeException}

/** Exercises the C filesystem through JNI on the local backend, which needs no
  * credentials and no object store.
  *
  * Needs libnative-storage-jni, so it is skipped when the library is absent —
  * the same treatment the other native suites get.
  */
class StorageNativeTest
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterAll {

  private var root: Path = _
  private var available = true

  override def beforeAll(): Unit = {
    root = Files.createTempDirectory("native-storage-test")
    try StorageNative.filesystemDestroy(open())
    catch {
      case _: UnsatisfiedLinkError | _: NoClassDefFoundError =>
        available = false
      case _: RuntimeException => available = false
    }
  }

  private def open(): Long =
    StorageNative.filesystemGet(
      Map(
        "fs.storage_type" -> "local",
        "fs.root_path" -> root.toAbsolutePath.toString
      ).asJava,
      ""
    )

  private def withFs(body: Long => Unit): Unit = {
    assume(available, "libnative-storage-jni is not on this machine")
    val fs = open()
    fs should not be 0L
    try body(fs)
    finally StorageNative.filesystemDestroy(fs)
  }

  // Paths are keys relative to fs.root_path. The C layer wraps the backend in a
  // subtree, so an absolute path is appended to the root and lands nowhere —
  // the same doubling that produces a-bucket/a-bucket/files/... on S3.
  test("write, read, size, list and delete go through the C filesystem") {
    withFs { fs =>
      val key = "seg/data.bin"
      val payload = "loon".getBytes(StandardCharsets.UTF_8)

      // writeFile does not create parents. Object storage has no directories so
      // this only shows on the local backend, but core.io has to be explicit
      // about it rather than let the difference leak upward.
      StorageNative.createDir(fs, "seg", true)
      StorageNative.writeFile(fs, key, payload)
      StorageNative.readFileAll(fs, key) shouldBe payload
      StorageNative.fileSize(fs, key) shouldBe payload.length.toLong
      Files.exists(root.resolve(key)) shouldBe true

      val entries = StorageNative.listDir(fs, "seg", false)
      entries.map(_.path) should not be empty

      StorageNative.deleteFile(fs, key)
      Files.exists(root.resolve(key)) shouldBe false
    }
  }

  test("an absolute path is appended to fs.root_path, not honoured") {
    withFs { fs =>
      an[StorageNativeException] should be thrownBy StorageNative.writeFile(
        fs,
        root.resolve("absolute.bin").toString,
        Array[Byte](1)
      )
    }
  }

  test("a ranged read does not fetch the whole file") {
    withFs { fs =>
      val key = "ranged.bin"
      val payload = "0123456789".getBytes(StandardCharsets.UTF_8)
      StorageNative.writeFile(fs, key, payload)

      val reader = StorageNative.openReader(fs, key, payload.length.toLong)
      try {
        StorageNative.readerReadAt(reader, 3L, 3L) shouldBe
          "345".getBytes(StandardCharsets.UTF_8)
      } finally StorageNative.readerDestroy(reader)
    }
  }

  test("a failed C call arrives as an exception, not a return code") {
    withFs { fs =>
      an[StorageNativeException] should be thrownBy
        StorageNative.readFileAll(fs, "absent")
    }
  }
}
