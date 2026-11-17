package com.zilliz.milvus.storage.io

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.concurrent.{Callable, CountDownLatch, Executors, TimeUnit}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

import com.zilliz.milvus.jni.storage.NativeStorageLibrary
import io.milvus.storage.MilvusStorageException

/** Conditional creation through the native writer on the local backend, which
  * needs no credentials and no object store.
  *
  * Bundle-backed CI requires this suite to run, so it holds no case that
  * depends on the user running it; StorageNativeTest keeps those.
  */
class NativeConditionalCreateTest
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterAll {

  private var root: Path = _
  private var available = true

  override def beforeAll(): Unit = {
    root = Files.createTempDirectory("native-conditional-create-test")
    try NativeStorageLibrary.load()
    catch {
      case _: UnsatisfiedLinkError | _: NoClassDefFoundError =>
        available = false
      case _: RuntimeException => available = false
    }
  }

  private def withWritableStore[A](
      body: NativeWritableObjectStore => A
  ): A = {
    assume(available, "libmilvus-storage-jni is not on this machine")
    val store = NativeWritableObjectStore.open(
      Map("fs.storage_type" -> "local", "fs.root_path" -> root.toString)
    )
    try body(store)
    finally store.close()
  }

  test("conditional creation keeps the original payload and native conflict") {
    withWritableStore { store =>
      val key = s"${root.toAbsolutePath}/conditional/nested/claim.json"
      val original = "first-operation".getBytes(StandardCharsets.UTF_8)
      store.create(key, original)
      store.readAll(key) shouldBe original

      val conflict = intercept[MilvusStorageException] {
        store.create(key, "second-operation".getBytes(StandardCharsets.UTF_8))
      }
      // The local backend reports LOON_AWS_ERROR_CONFLICT, not a failed
      // precondition. Keep that distinction instead of reclassifying it.
      conflict.errorCode() shouldBe 102
      store.readAll(key) shouldBe original
      Files.readAllBytes(
        root.resolve("conditional/nested/claim.json")
      ) shouldBe original
    }
  }

  test("conditional creation finalizes an empty payload") {
    withWritableStore { store =>
      val key = "conditional/empty"
      store.create(key, Array.emptyByteArray)
      store.exists(key) shouldBe true
      store.size(key) shouldBe 0L
      Files.readAllBytes(root.resolve(key)) shouldBe Array.emptyByteArray
      intercept[MilvusStorageException](store.create(key, Array[Byte](1)))
        .errorCode() shouldBe 102
      store.size(key) shouldBe 0L
    }
  }

  test("concurrent conditional creators publish exactly one complete payload") {
    assume(available, "libmilvus-storage-jni is not on this machine")
    val contenders = 8
    val ready = new CountDownLatch(contenders)
    val start = new CountDownLatch(1)
    val pool = Executors.newFixedThreadPool(contenders)
    val key = "conditional/concurrent/claim.json"
    def payload(id: Int): Array[Byte] =
      s"operation-$id".getBytes(StandardCharsets.UTF_8)
    try {
      val attempts = (0 until contenders).map { id =>
        pool.submit(new Callable[Either[MilvusStorageException, Int]] {
          override def call(): Either[MilvusStorageException, Int] =
            withWritableStore { store =>
              ready.countDown()
              assert(start.await(10, TimeUnit.SECONDS))
              try {
                store.create(key, payload(id))
                Right(id)
              } catch {
                case conflict: MilvusStorageException => Left(conflict)
              }
            }
        })
      }
      assert(ready.await(10, TimeUnit.SECONDS))
      start.countDown()
      val results = attempts.map(_.get(30, TimeUnit.SECONDS))
      val winners = results.collect { case Right(id) => id }
      val failures = results.collect { case Left(error) => error }
      winners.size shouldBe 1
      failures.size shouldBe contenders - 1
      failures.foreach(_.errorCode() shouldBe 102)
      withWritableStore { store =>
        store.readAll(key) shouldBe payload(winners.head)
      }
    } finally {
      start.countDown()
      pool.shutdownNow()
      pool.awaitTermination(10, TimeUnit.SECONDS)
    }
  }
}
