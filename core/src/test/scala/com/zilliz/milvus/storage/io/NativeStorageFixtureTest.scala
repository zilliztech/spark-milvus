package com.zilliz.milvus.storage.io

import java.nio.file.Files
import java.util.Comparator

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.jni.storage.NativeStorageLibrary
import io.milvus.storage.{MilvusStorageFileSystem, MilvusStorageProperties}

/** Exercises upstream native filesystem reads against checksum-verified frozen
  * bytes, without an external file or object-store credentials.
  */
class NativeStorageFixtureTest extends AnyFunSuite with Matchers {

  private def loadLibrary(): Unit =
    try NativeStorageLibrary.load()
    catch {
      case missing: UnsatisfiedLinkError
          if Option(missing.getMessage).exists(
            _.startsWith("no milvus-storage-jni in java.library.path")
          ) =>
        cancel("libmilvus-storage-jni is not on this machine")
    }

  test("local native storage preserves frozen bytes, size and ranged reads") {
    loadLibrary()
    val directory = Files.createTempDirectory("native-storage-fixture")
    try {
      ReadFixtures.copyTo("segments/batch-20480", directory)
      val key = "segment.parquet"
      val expected = Files.readAllBytes(directory.resolve(key))
      val properties = new MilvusStorageProperties()
      var fs: MilvusStorageFileSystem = null
      try {
        properties.create(
          Map(
            "fs.storage_type" -> "local",
            "fs.root_path" -> directory.toAbsolutePath.toString
          )
        )
        fs = new MilvusStorageFileSystem(properties, "")
        fs.readFileAll(key) shouldBe expected
        fs.fileSize(key) shouldBe expected.length.toLong

        val reader = fs.openReader(key, expected.length.toLong)
        try {
          reader.readAt(1024L, 4096L) shouldBe expected.slice(1024, 5120)
          // The final range ends exactly at EOF, including the parquet footer.
          val tail = expected.length - 64
          reader.readAt(tail.toLong, 64L) shouldBe expected.slice(
            tail,
            expected.length
          )
        } finally reader.close()
      } finally {
        try if (fs != null) fs.close()
        finally properties.free()
      }
    } finally {
      val paths = Files.walk(directory)
      try
        paths.sorted(Comparator.reverseOrder()).forEach(Files.deleteIfExists(_))
      finally paths.close()
    }
  }
}
