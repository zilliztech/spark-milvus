package com.zilliz.milvus.storage.write.commit

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.Comparator

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.io.LocalObjectStore
import com.zilliz.milvus.storage.write.exec.StagingLayout

class CommitterTest extends AnyFunSuite with Matchers {

  private val segments = Seq(
    CommittedSegment(0, "files/staging/job-1/0/task_0_1", 1L, 1500L),
    CommittedSegment(1, "files/staging/job-1/1/task_1_2", 1L, 1490L)
  )

  private def withStore(
      f: (LocalObjectStore, StagingLayout, Path) => Unit
  ): Unit = {
    val dir = Files.createTempDirectory("committer")
    try
      f(
        new LocalObjectStore(dir.toString),
        StagingLayout("files", "job-1"),
        dir
      )
    finally
      Files
        .walk(dir)
        .sorted(Comparator.reverseOrder[Path]())
        .forEach(p => Files.delete(p))
  }

  private def text(dir: Path, key: String): String =
    new String(Files.readAllBytes(dir.resolve(key)), StandardCharsets.UTF_8)

  test("commit writes the job manifest, then the marker") {
    withStore { (store, layout, dir) =>
      val committer = new Committer(store, layout)
      committer.isCommitted shouldBe false

      committer.commit(
        segments,
        nowMillis = 1700000000000L
      ) shouldBe CommitOutcome.Committed

      committer.isCommitted shouldBe true
      text(dir, "files/staging/job-1/_committed") shouldBe "job-1"
      val json = text(dir, "files/staging/job-1/manifest.json")
      json should include("\"job_id\" : \"job-1\"")
      json should include("\"created_at\" : 1700000000000")
      json should include("\"partition_id\" : 1")
      json should include("\"base_path\" : \"files/staging/job-1/1/task_1_2\"")
      json should include("\"manifest_version\" : 1")
      json should include("\"row_count\" : 1490")
      val manifest = JobManifest.fromJson(json).fold(e => throw e, identity)
      manifest shouldBe JobManifest("job-1", 1700000000000L, segments)
      manifest.rowCount shouldBe 2990L
    }
  }

  test("a second commit of the same job writes nothing") {
    withStore { (store, layout, dir) =>
      val committer = new Committer(store, layout)
      committer.commit(
        segments,
        nowMillis = 1L
      ) shouldBe CommitOutcome.Committed
      val first = text(dir, "files/staging/job-1/manifest.json")

      committer.commit(
        segments.take(1),
        nowMillis = 2L
      ) shouldBe CommitOutcome.AlreadyCommitted

      text(dir, "files/staging/job-1/manifest.json") shouldBe first
    }
  }

  test("a marker of another job is an error, not a silent skip") {
    withStore { (store, layout, _) =>
      store.write(layout.marker, "job-0".getBytes(StandardCharsets.UTF_8))
      val e = intercept[IllegalStateException](
        new Committer(store, layout).commit(segments)
      )
      e.getMessage should include("marks job 'job-0', not job 'job-1'")
    }
  }

  test("abort deletes every file under the staging prefix") {
    withStore { (store, layout, dir) =>
      store.write(layout.segment(0, 1) + "/0.parquet", Array[Byte](1, 2, 3))
      store.write(layout.segment(0, 1) + "/manifest-1.avro", Array[Byte](4))
      store.write(layout.segment(1, 2) + "/0.parquet", Array[Byte](5))
      val committer = new Committer(store, layout)
      committer.commit(segments)
      // A neighbouring job is not touched.
      store.write("files/staging/job-2/0/task_0_1/0.parquet", Array[Byte](6))

      committer.abort() shouldBe 5

      store
        .list(layout.prefix, recursive = true)
        .filterNot(_.isDirectory) shouldBe empty
      committer.isCommitted shouldBe false
      Files.exists(
        dir.resolve("files/staging/job-2/0/task_0_1/0.parquet")
      ) shouldBe true
    }
  }

  test("abort of a job that wrote nothing deletes nothing") {
    withStore { (store, layout, _) =>
      new Committer(store, layout).abort() shouldBe 0
    }
  }

  test("registration is recorded once the segments are handed over") {
    withStore { (store, layout, dir) =>
      val committer = new Committer(store, layout)
      committer.commit(segments)
      committer.isRegistered shouldBe false
      committer.manifest().segments shouldBe segments
      committer.markRegistered(nowMillis = 5L)
      committer.isRegistered shouldBe true
      text(dir, "files/staging/job-1/_registered") shouldBe "5"
    }
  }

  test("JobManifest round-trips through JSON") {
    val manifest = JobManifest("j", 42L, segments)
    JobManifest.fromJson(manifest.toJson) shouldBe Right(manifest)
    JobManifest.fromJson("{") should matchPattern { case Left(_) => }
  }
}
