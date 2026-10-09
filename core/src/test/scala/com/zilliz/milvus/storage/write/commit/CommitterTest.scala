package com.zilliz.milvus.storage.write.commit

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.Comparator

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.io.{FileInfo, LocalObjectStore, ObjectStore}
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

  test("owned jobs publish immutable ownership, heartbeat and manifest v2") {
    withStore { (store, layout, _) =>
      val descriptor =
        JobDescriptor(JobOwner("analytics", "events"), JobWriteMode.Append)
      val committer = new Committer(store, layout)

      committer.start(descriptor, nowMillis = 100L)
      committer.heartbeat(nowMillis = 150L)
      committer.commit(
        segments,
        nowMillis = 200L,
        descriptor = Some(descriptor)
      ) shouldBe CommitOutcome.Committed

      committer.ownerManifest() shouldBe JobOwnerManifest(
        JobOwnerManifest.CurrentVersion,
        "job-1",
        100L,
        descriptor.owner,
        JobWriteMode.Append.name
      )
      JobHeartbeat
        .fromJson(text(storeRoot = store, path = layout.heartbeat))
        .fold(e => throw e, identity) shouldBe JobHeartbeat(
        JobHeartbeat.CurrentVersion,
        "job-1",
        200L
      )
      committer.manifest() shouldBe JobManifest(
        "job-1",
        100L,
        segments,
        formatVersion = Some(JobManifest.CurrentVersion),
        owner = Some(descriptor.owner),
        writeMode = Some(JobWriteMode.Append.name)
      )

      val changed =
        JobDescriptor(JobOwner("analytics", "other"), JobWriteMode.Append)
      val error = intercept[IllegalStateException](
        committer.start(changed, nowMillis = 250L)
      )
      error.getMessage should include("does not describe")
    }
  }

  test("start creates the staging directory before writing ownership") {
    withStore { (store, layout, _) =>
      val strict = new ParentCheckingStore(store)
      new Committer(strict, layout).start(
        JobDescriptor(JobOwner("default", "events"), JobWriteMode.Append),
        nowMillis = 10L
      )

      strict.created should contain(layout.prefix)
      store.exists(layout.owner) shouldBe true
      store.exists(layout.heartbeat) shouldBe true
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

  // A marker lookup that fails is not an absent marker.
  test("a failed marker lookup stops the commit before anything is written") {
    withStore { (store, layout, _) =>
      val written = scala.collection.mutable.ListBuffer.empty[String]
      val failing = new com.zilliz.milvus.storage.io.ObjectStore {
        def readAll(key: String): Array[Byte] = store.readAll(key)
        def size(key: String): Long = store.size(key)
        def list(key: String, recursive: Boolean) = store.list(key, recursive)
        def exists(key: String): Boolean =
          throw new RuntimeException(s"access denied: $key")
        def readAt(key: String, offset: Long, length: Long, fileSize: Long) =
          store.readAt(key, offset, length, fileSize)
        def write(key: String, data: Array[Byte]): Unit = {
          written += key
          store.write(key, data)
        }
        def createDir(key: String, recursive: Boolean): Unit =
          store.createDir(key, recursive)
        def delete(key: String): Unit = store.delete(key)
        def close(): Unit = store.close()
      }
      val committer = new Committer(failing, layout)
      intercept[RuntimeException](committer.commit(segments)).getMessage should
        include("access denied")
      intercept[RuntimeException](committer.isRegistered).getMessage should
        include("access denied")
      written shouldBe empty
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

  private def text(storeRoot: LocalObjectStore, path: String): String =
    new String(storeRoot.readAll(path), StandardCharsets.UTF_8)

  private final class ParentCheckingStore(delegate: ObjectStore)
      extends ObjectStore {
    var created = Set.empty[String]

    override def readAll(key: String): Array[Byte] = delegate.readAll(key)
    override def size(key: String): Long = delegate.size(key)
    override def list(key: String, recursive: Boolean): Seq[FileInfo] =
      delegate.list(key, recursive)
    override def exists(key: String): Boolean = delegate.exists(key)
    override def readAt(
        key: String,
        offset: Long,
        length: Long,
        fileSize: Long
    ): Array[Byte] = delegate.readAt(key, offset, length, fileSize)
    override def write(key: String, data: Array[Byte]): Unit = {
      val parent = key.split("/").dropRight(1).mkString("/")
      if (!delegate.exists(parent)) {
        throw new IllegalStateException(s"parent $parent was not created")
      }
      delegate.write(key, data)
    }
    override def createDir(key: String, recursive: Boolean): Unit = {
      created += key
      delegate.createDir(key, recursive)
    }
    override def delete(key: String): Unit = delegate.delete(key)
    override def close(): Unit = ()
  }
}
