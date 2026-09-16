package com.zilliz.spark.connector.procedure

import java.nio.file.{Files, Path}
import java.util.Comparator
import scala.util.{Failure, Success}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.io.LocalObjectStore
import com.zilliz.milvus.storage.write.commit.{
  CommittedSegment,
  Committer,
  Registration
}
import com.zilliz.milvus.storage.write.exec.StagingLayout
import io.milvus.grpc.common.Status

/** The register procedure against a local store and a stand-in for Milvus. */
class RegisterTest extends AnyFunSuite with Matchers {

  private val backfilled = Seq(
    CommittedSegment(0, "files/insert_log/1/2/30", 3L, 100L, Some(30L)),
    CommittedSegment(0, "files/insert_log/1/2/31", 2L, 80L, Some(31L))
  )

  private def withStore(
      f: (LocalObjectStore, StagingLayout) => Unit
  ): Unit = {
    val dir = Files.createTempDirectory("register")
    try f(new LocalObjectStore(dir.toString), StagingLayout("files", "job-1"))
    finally
      Files
        .walk(dir)
        .sorted(Comparator.reverseOrder[Path]())
        .forEach(p => Files.delete(p))
  }

  test("a committed backfill job is handed to Milvus once") {
    withStore { (store, layout) =>
      new Committer(store, layout).commit(backfilled)
      var calls = Seq.empty[Seq[Registration.Item]]
      val register = (items: Seq[Registration.Item]) => {
        calls = calls :+ items
        Success(Status())
      }
      val first = Register.run(store, layout, register)
      first.alreadyRegistered shouldBe false
      first.items shouldBe Seq(
        Registration.Item(30L, 3L),
        Registration.Item(31L, 2L)
      )
      store.exists(layout.registered) shouldBe true

      val second = Register.run(store, layout, register)
      second.alreadyRegistered shouldBe true
      calls.size shouldBe 1
    }
  }

  test("an uncommitted job is not registered") {
    withStore { (store, layout) =>
      val e = intercept[IllegalStateException](
        Register.run(store, layout, _ => Success(Status()))
      )
      e.getMessage should include("not committed")
    }
  }

  test("Milvus refusing the segments fails the call and leaves no marker") {
    withStore { (store, layout) =>
      new Committer(store, layout).commit(backfilled)
      val e = intercept[IllegalStateException](
        Register.run(
          store,
          layout,
          _ => Failure(new Exception("segment not found"))
        )
      )
      e.getMessage should include("segment not found")
      store.exists(layout.registered) shouldBe false
    }
  }

  test("a job with a new segment is refused before Milvus is called") {
    withStore { (store, layout) =>
      new Committer(store, layout)
        .commit(
          backfilled :+ CommittedSegment(
            1,
            "files/staging/job-1/1/task_1_2",
            1L,
            5L
          )
        )
      var called = false
      val e = intercept[IllegalStateException](
        Register.run(store, layout, _ => { called = true; Success(Status()) })
      )
      e.getMessage should include("RegisterSegments")
      called shouldBe false
    }
  }

  test("the staging prefix names the layout") {
    Register.layoutOf("files/staging/job-1") shouldBe StagingLayout(
      "files",
      "job-1"
    )
    Register.layoutOf("staging/job-1/") shouldBe StagingLayout("", "job-1")
    Register.layoutOf("c48b088d035ecd5/x/staging/j") shouldBe StagingLayout(
      "c48b088d035ecd5/x",
      "j"
    )
    an[IllegalArgumentException] should be thrownBy Register.layoutOf(
      "files/job-1"
    )
    Register.layoutOf("files/staging/job.1") shouldBe StagingLayout(
      "files",
      "job.1"
    )
    Seq(".", "..", "a\\b").foreach { jobId =>
      an[IllegalArgumentException] should be thrownBy Register.layoutOf(
        s"files/staging/$jobId"
      )
    }
  }
}
