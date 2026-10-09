package com.zilliz.spark.connector.write

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.Comparator
import scala.collection.mutable

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.io.LocalObjectStore
import com.zilliz.milvus.storage.write.commit.{CommittedSegment, Committer}
import com.zilliz.milvus.storage.write.exec.StagingLayout
import com.zilliz.spark.connector.uat.{UatResources, UatWriteScope}

class UatResourcesTest extends AnyFunSuite with Matchers {
  private def withDirectory(body: Path => Unit): Unit = {
    val directory = Files.createTempDirectory("uat-resources")
    try body(directory)
    finally {
      val entries = Files.walk(directory)
      try
        entries.sorted(Comparator.reverseOrder[Path]()).forEach(Files.delete(_))
      finally entries.close()
    }
  }

  test(
    "a failure before any write closes the store without requiring a directory"
  ) {
    withDirectory { directory =>
      val scope = new UatWriteScope(directory.toString, "before-write")
      val store = new LocalObjectStore()
      intercept[IllegalArgumentException] {
        scope.run(store)(throw new IllegalArgumentException("invalid input"))
      }
      store.isClosed shouldBe true
      Files.exists(directory.resolve(scope.root)) shouldBe false
    }
  }

  test(
    "a post-commit assertion failure cleans its files and a retry gets a fresh directory"
  ) {
    withDirectory { directory =>
      val base = directory.toString
      val first = new UatWriteScope(base, "write-read")
      val other = new UatWriteScope(base, "write-read")
      val observer = new LocalObjectStore()
      val sentinel = s"${other.root}/staging/other/_committed"
      observer.write(sentinel, "other".getBytes(StandardCharsets.UTF_8))
      val store = new LocalObjectStore()
      val layout = StagingLayout(first.root, "job")
      val failure = new AssertionError("readback failed after commit")
      val thrown = intercept[AssertionError] {
        first.run(store) {
          new Committer(store, layout).commit(
            Seq(CommittedSegment(0, s"${layout.prefix}/segment", 1L, 1L)),
            nowMillis = 1L
          )
          store.exists(layout.marker) shouldBe true
          throw failure
        }
      }
      thrown should be theSameInstanceAs failure
      store.isClosed shouldBe true
      observer
        .list(first.root, recursive = true)
        .filterNot(_.isDirectory) shouldBe empty
      observer.exists(sentinel) shouldBe true

      val retry = new UatWriteScope(base, "write-read")
      retry.root should not be first.root
      retry.root should not be other.root
      val retryStore = new LocalObjectStore()
      val retryLayout = StagingLayout(retry.root, "retry")
      retry.run(retryStore) {
        new Committer(retryStore, retryLayout).commit(Seq.empty, nowMillis = 2L)
        retryStore.list(s"${retry.root}/staging").size shouldBe 1
      }
      observer.exists(retryLayout.marker) shouldBe false
      observer.exists(sentinel) shouldBe true
      observer.close()
    }
  }

  test("overlapping scopes list and clean only their own files") {
    withDirectory { directory =>
      val first = new UatWriteScope(directory.toString, "scenario")
      val second = new UatWriteScope(directory.toString, "scenario")
      val a = new LocalObjectStore()
      val b = new LocalObjectStore()
      val keyA = s"${first.root}/staging/a/_committed"
      val keyB = s"${second.root}/staging/b/_committed"
      first.run(a) {
        a.write(keyA, Array[Byte](1))
        second.run(b) {
          b.write(keyB, Array[Byte](2))
          a.list(s"${first.root}/staging").size shouldBe 1
          b.list(s"${second.root}/staging").size shouldBe 1
        }
        a.exists(keyA) shouldBe true
        a.exists(keyB) shouldBe false
      }
      a.isClosed shouldBe true
      b.isClosed shouldBe true
    }
  }

  test(
    "explicitly kept artifacts survive an assertion failure and the store still closes"
  ) {
    withDirectory { directory =>
      val scope = new UatWriteScope(directory.toString, "debug")
      val store = new LocalObjectStore()
      val key = s"${scope.root}/staging/job/_committed"
      intercept[AssertionError] {
        scope.run(store, keep = true) {
          store.write(key, Array[Byte](1))
          throw new AssertionError("inspect this output")
        }
      }
      Files.exists(directory.resolve(key)) shouldBe true
      store.isClosed shouldBe true
    }
  }

  test(
    "cleanup failures fail a success and preserve an earlier assertion as primary"
  ) {
    val cleanup = new IllegalStateException("cleanup failed")
    intercept[IllegalStateException] {
      UatResources.withCleanup(())(throw cleanup)
    } should be theSameInstanceAs cleanup
    val assertion = new AssertionError("assertion failed")
    intercept[AssertionError] {
      UatResources.withCleanup(throw assertion)(throw cleanup)
    } should be theSameInstanceAs assertion
    assertion.getSuppressed.toSeq shouldBe Seq(cleanup)
  }

  test("collection cleanup preserves overlapping runs and old collections") {
    val sentinel = "spark_milvus_ct_preexisting"
    val collections = mutable.Set(sentinel)
    val create: String => Unit = name => { collections.add(name); () }
    val drop: String => Unit = name => { collections.remove(name); () }
    UatResources.withCollection(create, drop) { first =>
      intercept[AssertionError] {
        UatResources.withCollection(create, drop) { second =>
          second should not be first
          collections.toSet shouldBe Set(sentinel, first, second)
          throw new AssertionError("second run failed")
        }
      }
      collections.toSet shouldBe Set(sentinel, first)
    }
    collections.toSet shouldBe Set(sentinel)
  }

  test("partially created collections are cleaned when creation throws") {
    val collections = mutable.Set.empty[String]
    intercept[IllegalStateException] {
      UatResources.withCollection(
        name => {
          collections.add(name);
          throw new IllegalStateException("create timed out")
        },
        name => { collections.remove(name); () }
      )(_ => fail("creation failed"))
    }
    collections shouldBe empty
  }
}
