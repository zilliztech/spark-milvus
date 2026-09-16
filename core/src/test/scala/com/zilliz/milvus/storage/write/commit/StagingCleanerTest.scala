package com.zilliz.milvus.storage.write.commit

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.nio.file.attribute.FileTime
import java.util.Comparator

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.io.{FileInfo, LocalObjectStore, ObjectStore}
import com.zilliz.milvus.storage.write.exec.StagingLayout

class StagingCleanerTest extends AnyFunSuite with Matchers {
  private val target = JobOwner("analytics", "events")
  private val nowMillis = 1000000L
  private val retentionMillis = 100000L
  private val oldMillis = 2000L

  private def withStore(f: (LocalObjectStore, Path) => Unit): Unit = {
    val dir = Files.createTempDirectory("staging-cleaner")
    val store = new LocalObjectStore(dir.toString)
    try f(store, dir)
    finally {
      store.close()
      Files
        .walk(dir)
        .sorted(Comparator.reverseOrder[Path]())
        .forEach(path => Files.delete(path))
    }
  }

  private def layout(jobId: String): StagingLayout =
    StagingLayout("files", jobId)

  private def descriptor(
      mode: JobWriteMode = JobWriteMode.Append,
      owner: JobOwner = target
  ) =
    JobDescriptor(owner, mode)

  private def start(
      store: ObjectStore,
      job: StagingLayout,
      mode: JobWriteMode = JobWriteMode.Append,
      createdAt: Long = 1000L,
      heartbeatAt: Long = oldMillis,
      owner: JobOwner = target
  ): Committer = {
    val committer = new Committer(store, job)
    committer.start(descriptor(mode, owner), createdAt)
    committer.heartbeat(heartbeatAt)
    store.write(job.segment(0, 1L) + "/data", Array[Byte](1, 2, 3))
    committer
  }

  private def age(dir: Path, job: StagingLayout, millis: Long): Unit = {
    val stream = Files.walk(dir.resolve(job.prefix))
    try
      stream.forEach(path =>
        if (Files.isRegularFile(path)) {
          Files.setLastModifiedTime(path, FileTime.fromMillis(millis))
        }
      )
    finally stream.close()
  }

  private def clean(
      store: ObjectStore,
      dryRun: Boolean
  ): Seq[StagingCleanupResult] =
    new StagingCleaner(store, "files")
      .clean(target, retentionMillis, dryRun, nowMillis)

  test(
    "dry run audits a killed append job and real cleanup deletes files only"
  ) {
    withStore { (store, dir) =>
      val job = layout("job-killed")
      start(store, job)
      age(dir, job, oldMillis)
      store.write("files/sentinel", Array[Byte](9))

      val preview = clean(store, dryRun = true).head
      preview.action shouldBe CleanupAction.WouldDeleteFiles
      preview.candidateFiles shouldBe 3
      preview.deletedFiles shouldBe 0
      preview.prefixDeleted shouldBe false
      store.exists(job.owner) shouldBe true

      val deleted = clean(store, dryRun = false).head
      deleted.action shouldBe CleanupAction.FilesDeleted
      deleted.deletedFiles shouldBe 3
      deleted.directoriesRemaining should be > 0
      deleted.prefixDeleted shouldBe false
      store
        .list(job.prefix, recursive = true)
        .filterNot(_.isDirectory) shouldBe empty
      Files.isDirectory(dir.resolve(job.prefix)) shouldBe true
      store.exists("files/sentinel") shouldBe true
    }
  }

  test("registered, backfill, fresh and legacy jobs are preserved") {
    withStore { (store, dir) =>
      val registered = layout("job-registered")
      start(store, registered).markRegistered(3000L)
      age(dir, registered, oldMillis)

      val backfill = layout("job-backfill")
      start(store, backfill, JobWriteMode.Backfill)
      age(dir, backfill, oldMillis)

      val fresh = layout("job-fresh")
      start(store, fresh, heartbeatAt = 950000L)
      age(dir, fresh, 950000L)

      val legacy = layout("job-legacy")
      new Committer(store, legacy).commit(Seq.empty, nowMillis = oldMillis)
      age(dir, legacy, oldMillis)

      val foreign = layout("job-foreign")
      start(
        store,
        foreign,
        owner = JobOwner("analytics", "other")
      )
      age(dir, foreign, oldMillis)

      val missingHeartbeat = layout("job-no-heartbeat")
      start(store, missingHeartbeat)
      store.delete(missingHeartbeat.heartbeat)
      age(dir, missingHeartbeat, oldMillis)

      val inconsistentHeartbeat = layout("job-bad-heartbeat")
      start(store, inconsistentHeartbeat)
      store.write(
        inconsistentHeartbeat.heartbeat,
        JobHeartbeat(
          JobHeartbeat.CurrentVersion,
          "another-job",
          oldMillis
        ).toJson
          .getBytes(StandardCharsets.UTF_8)
      )
      age(dir, inconsistentHeartbeat, oldMillis)

      val results = clean(store, dryRun = false).map(r => r.jobId -> r).toMap
      results.keySet shouldBe Set(
        registered.jobId,
        backfill.jobId,
        fresh.jobId,
        legacy.jobId,
        foreign.jobId,
        missingHeartbeat.jobId,
        inconsistentHeartbeat.jobId
      )
      results.values.foreach(_.action shouldBe CleanupAction.Preserved)
      results(registered.jobId).reason should include("_registered")
      results(backfill.jobId).reason should include(
        "never automatically cleaned"
      )
      results(fresh.jobId).reason should include("newer than retention cutoff")
      results(legacy.jobId).reason should include("owner.json is missing")
      results(foreign.jobId).reason should include("not analytics.events")
      results(missingHeartbeat.jobId).reason should include(
        "_heartbeat is missing"
      )
      results(inconsistentHeartbeat.jobId).reason should include(
        "heartbeat metadata is inconsistent"
      )
      results.keys.foreach(jobId =>
        store.list(layout(jobId).prefix, recursive = true) should not be empty
      )
    }
  }

  test("a committed but unregistered append job remains eligible") {
    withStore { (store, dir) =>
      val job = layout("job-committed")
      start(store, job).commit(
        Seq.empty,
        nowMillis = 3000L,
        descriptor = Some(descriptor())
      )
      age(dir, job, oldMillis)

      val result = clean(store, dryRun = false).head
      result.action shouldBe CleanupAction.FilesDeleted
      result.candidateFiles shouldBe 5
      result.deletedFiles shouldBe 5
      store.exists(job.marker) shouldBe false
      store.exists(job.manifest) shouldBe false
    }
  }

  test("a recently modified file closes an otherwise stale lease") {
    withStore { (store, dir) =>
      val job = layout("job-changing")
      start(store, job)
      age(dir, job, oldMillis)
      Files.setLastModifiedTime(
        dir.resolve(job.segment(0, 1L) + "/data"),
        FileTime.fromMillis(950000L)
      )

      val result = clean(store, dryRun = false).head
      result.action shouldBe CleanupAction.Preserved
      result.reason should include("modified after retention cutoff")
      store.exists(job.owner) shouldBe true
    }
  }

  test("one corrupt or mismatched job does not block an eligible sibling") {
    withStore { (store, dir) =>
      val corrupt = layout("job-corrupt")
      store.write(corrupt.owner, "{".getBytes(StandardCharsets.UTF_8))
      store.write(
        corrupt.heartbeat,
        JobHeartbeat(
          JobHeartbeat.CurrentVersion,
          corrupt.jobId,
          oldMillis
        ).toJson
          .getBytes(StandardCharsets.UTF_8)
      )
      age(dir, corrupt, oldMillis)

      val mismatched = layout("job-marker")
      start(store, mismatched)
        .commit(
          Seq.empty,
          nowMillis = 3000L,
          descriptor = Some(descriptor())
        )
      store.write(
        mismatched.marker,
        "another-job".getBytes(StandardCharsets.UTF_8)
      )
      age(dir, mismatched, oldMillis)

      val eligible = layout("job-eligible")
      start(store, eligible)
      age(dir, eligible, oldMillis)

      val results = clean(store, dryRun = false).map(r => r.jobId -> r).toMap
      results(corrupt.jobId).action shouldBe CleanupAction.Preserved
      results(corrupt.jobId).reason should include(
        "cannot validate job metadata"
      )
      results(mismatched.jobId).action shouldBe CleanupAction.Preserved
      results(mismatched.jobId).reason should include("marks job 'another-job'")
      results(eligible.jobId).action shouldBe CleanupAction.FilesDeleted
      store.exists(corrupt.owner) shouldBe true
      store.exists(mismatched.owner) shouldBe true
      store.exists(eligible.owner) shouldBe false
    }
  }

  test("a storage listing outside the observed job directory fails closed") {
    withStore { (store, dir) =>
      val job = layout("job-contained")
      start(store, job)
      age(dir, job, oldMillis)
      val outside = dir.resolve("outside-sentinel")
      Files.write(outside, Array[Byte](7))
      Files.setLastModifiedTime(outside, FileTime.fromMillis(oldMillis))

      val listing = new DelegatingStore(store) {
        override def list(key: String, recursive: Boolean): Seq[FileInfo] = {
          val result = super.list(key, recursive)
          if (key == job.prefix && recursive)
            result :+ FileInfo(
              outside.toString,
              isDirectory = false,
              size = 1L,
              modifiedNanos = oldMillis * 1000000L
            )
          else result
        }
      }

      val result = clean(listing, dryRun = false).head
      result.action shouldBe CleanupAction.Preserved
      result.reason should include("outside the exact job prefix")
      Files.exists(outside) shouldBe true
      store.exists(job.owner) shouldBe true
    }
  }

  test("a fingerprint change between the two checks prevents deletion") {
    withStore { (store, dir) =>
      val job = layout("job-racing")
      start(store, job)
      age(dir, job, oldMillis)
      var recursiveLists = 0
      val changing = new DelegatingStore(store) {
        override def list(key: String, recursive: Boolean): Seq[FileInfo] = {
          val result = super.list(key, recursive)
          if (key == job.prefix && recursive) {
            recursiveLists += 1
            if (recursiveLists == 2)
              result.map { info =>
                if (!info.isDirectory && info.path.endsWith("/data"))
                  info.copy(size = info.size + 1L)
                else info
              }
            else result
          } else result
        }
      }

      val result = clean(changing, dryRun = false).head
      result.action shouldBe CleanupAction.Preserved
      result.reason should include("changed between cleanup checks")
      recursiveLists shouldBe 2
      store.exists(job.owner) shouldBe true
      store.exists(job.segment(0, 1L) + "/data") shouldBe true
    }
  }

  private class DelegatingStore(delegate: ObjectStore) extends ObjectStore {
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
    override def write(key: String, data: Array[Byte]): Unit =
      delegate.write(key, data)
    override def createDir(key: String, recursive: Boolean): Unit =
      delegate.createDir(key, recursive)
    override def delete(key: String): Unit = delegate.delete(key)
    override def close(): Unit = ()
  }
}
