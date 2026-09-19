package com.zilliz.milvus.storage.io

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.nio.file.attribute.{PosixFilePermission, PosixFilePermissions}
import java.util.Collections

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

import com.zilliz.milvus.jni.storage.NativeStorageLibrary
import io.milvus.storage.{
  MilvusStorageException,
  MilvusStorageFileSystem,
  MilvusStorageProperties
}

/** Exercises the C filesystem through JNI on the local backend, which needs no
  * credentials and no object store.
  *
  * Needs libmilvus-storage-jni, so it is skipped when the library is absent —
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
    try NativeStorageLibrary.load()
    catch {
      case _: UnsatisfiedLinkError | _: NoClassDefFoundError =>
        available = false
      case _: RuntimeException => available = false
    }
  }

  private def withFs(body: MilvusStorageFileSystem => Unit): Unit = {
    assume(available, "libmilvus-storage-jni is not on this machine")
    val properties = new MilvusStorageProperties()
    var fs: MilvusStorageFileSystem = null
    try {
      properties.create(
        Map(
          "fs.storage_type" -> "local",
          "fs.root_path" -> root.toAbsolutePath.toString
        )
      )
      fs = new MilvusStorageFileSystem(properties, "")
      body(fs)
    } finally {
      try if (fs != null) fs.close()
      finally properties.free()
    }
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
      fs.createDir("seg", true)
      fs.writeFile(key, payload)
      fs.readFileAll(key) shouldBe payload
      fs.fileSize(key) shouldBe payload.length.toLong
      fs.exists(key) shouldBe true
      Files.exists(root.resolve(key)) shouldBe true

      val entries = fs.list("seg", false)
      entries.map(_.path) should not be empty

      fs.deleteFile(key)
      fs.exists(key) shouldBe false
      Files.exists(root.resolve(key)) shouldBe false
    }
  }

  test("an absolute path is appended to fs.root_path, not honoured") {
    withFs { fs =>
      an[MilvusStorageException] should be thrownBy fs.writeFile(
        root.resolve("absolute.bin").toString,
        Array[Byte](1)
      )
    }
  }

  test("a ranged read does not fetch the whole file") {
    withFs { fs =>
      val key = "ranged.bin"
      val payload = "0123456789".getBytes(StandardCharsets.UTF_8)
      fs.writeFile(key, payload)

      val reader = fs.openReader(key, payload.length.toLong)
      try {
        reader.readAt(3L, 3L) shouldBe
          "345".getBytes(StandardCharsets.UTF_8)
      } finally reader.close()
    }
  }

  test("a store key means the same thing on every backend") {
    assume(available, "libmilvus-storage-jni is not on this machine")
    // JobManifest states the contract: a key is relative to the bucket, so it
    // carries fs.root_path. The C layer roots a remote backend at the bucket
    // and a local one at fs.root_path, one level deeper, and the store hides
    // that difference rather than letting callers carry two conventions.
    // The key is built the way a writer builds it: fs.root_path, then the
    // job's own path under it.
    val rootPath = root.resolve("files").toAbsolutePath.toString
    val store = NativeObjectStore
      .Factory(
        Map("fs.storage_type" -> "local", "fs.root_path" -> rootPath)
      )
      .open()
    try {
      val key = s"$rootPath/staging/job-1/segment/data.bin"
      val payload = "loon".getBytes(StandardCharsets.UTF_8)

      // The parents do not exist: object storage has none, a filesystem does,
      // and which of the two is behind the store is not the caller's business.
      store.write(key, payload)

      store.exists(key) shouldBe true
      store.readAll(key) shouldBe payload
      store.size(key) shouldBe payload.length.toLong
      store.list(s"$rootPath/staging", recursive = true).map(_.path) should
        contain(key)
      // Written once, at the key's own place: a root counted twice would put
      // a second copy somewhere below it.
      Files.exists(
        root.resolve("files").resolve("staging/job-1/segment/data.bin")
      ) shouldBe true
      val written = Files.walk(root)
      try
        written
          .filter(path => path.getFileName.toString == "data.bin")
          .count() shouldBe 1L
      finally written.close()
    } finally store.close()
  }

  test("a failed C call arrives as an exception, not a return code") {
    withFs { fs =>
      an[MilvusStorageException] should be thrownBy
        fs.readFileAll("absent")
    }
  }

  // Only a confirmed absence is "does not exist"; a denied or failed lookup has
  // to reach the caller, or a commit marker that is there but unreadable reads
  // as missing and the job manifest is written over.
  test("exists is false only for a missing key; a denied lookup throws") {
    assume(available, "libmilvus-storage-jni is not on this machine")
    // LOON_FILE_NOT_FOUND in milvus-storage's ffi_error_code.h.
    val fileNotFound = 12
    assume(
      System.getProperty("user.name") != "root",
      "root ignores directory permissions"
    )
    val store = NativeObjectStore
      .Factory(
        Map(
          "fs.storage_type" -> "local",
          "fs.root_path" -> root.toAbsolutePath.toString
        )
      )
      .open()
    val locked = root.resolve("locked")
    try {
      store.createDir("locked", recursive = true)
      store.write("locked/_committed", "job-1".getBytes(StandardCharsets.UTF_8))
      store.exists("locked/_committed") shouldBe true
      store.exists("absent") shouldBe false
      intercept[MilvusStorageException](
        store.size("absent")
      ).errorCode() shouldBe fileNotFound

      Files.setPosixFilePermissions(
        locked,
        Collections.emptySet[PosixFilePermission]()
      )
      val denied = intercept[MilvusStorageException](
        store.exists("locked/_committed")
      )
      denied.errorCode() should not be fileNotFound
    } finally {
      Files.setPosixFilePermissions(
        locked,
        PosixFilePermissions.fromString("rwx------")
      )
      store.close()
    }
  }
}
