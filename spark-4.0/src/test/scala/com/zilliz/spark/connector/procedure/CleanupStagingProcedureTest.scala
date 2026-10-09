package com.zilliz.spark.connector.procedure

import java.nio.file.{Files, Path}
import java.nio.file.attribute.FileTime
import java.util.Comparator

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.io.LocalObjectStore
import com.zilliz.milvus.storage.write.commit.{
  CleanupAction,
  Committer,
  JobDescriptor,
  JobOwner,
  JobWriteMode
}
import com.zilliz.milvus.storage.write.exec.StagingLayout

class CleanupStagingProcedureTest extends AnyFunSuite with Matchers {

  test("cleanup_staging defaults to seven-day dry run and closes its store") {
    val dir = Files.createTempDirectory("cleanup-staging-defaults")
    val store = new LocalObjectStore(dir.toString)
    val layout = StagingLayout("files", "job-defaults")
    try {
      val owner = JobOwner("default", "events")
      val committer = new Committer(store, layout)
      committer.start(
        JobDescriptor(owner, JobWriteMode.Append),
        nowMillis = 1000L
      )
      committer.heartbeat(nowMillis = 2000L)
      store.write(layout.segment(0, 1L) + "/data", Array[Byte](1))
      age(dir.resolve(layout.prefix), 2000L)

      val rows = CleanupStagingProcedure.run(
        ProcedureArgs(
          Map("collection" -> "events"),
          Map("fs.storage_type" -> "local", "fs.root_path" -> "files")
        ),
        _ => store,
        nowMillis = 700000000L
      )

      rows.size shouldBe 1
      rows.head.getString(4) shouldBe CleanupAction.WouldDeleteFiles
      rows.head.length shouldBe CleanupStagingProcedure.outputSchema.length
      store.exists(layout.owner) shouldBe true
      store.isClosed shouldBe true
    } finally {
      Files
        .walk(dir)
        .sorted(Comparator.reverseOrder[Path]())
        .forEach(path => Files.delete(path))
    }
  }

  test(
    "cleanup_staging defaults to an auditable dry run and reports file-only deletion"
  ) {
    val dir = Files.createTempDirectory("cleanup-staging-procedure")
    val store = new LocalObjectStore(dir.toString)
    val layout = StagingLayout("files", "job-1")
    try {
      val owner = JobOwner("analytics", "events")
      val committer = new Committer(store, layout)
      committer.start(
        JobDescriptor(owner, JobWriteMode.Append),
        nowMillis = 1000L
      )
      committer.heartbeat(nowMillis = 2000L)
      store.write(layout.segment(0, 1L) + "/data", Array[Byte](1))
      age(dir.resolve(layout.prefix), 2000L)

      val preview = CleanupStagingProcedure.run(
        store,
        "files",
        owner,
        retentionSeconds = 300L,
        dryRun = true,
        nowMillis = 1000000L
      )
      preview.size shouldBe 1
      preview.head.getString(0) shouldBe layout.jobId
      preview.head.getString(1) shouldBe "analytics"
      preview.head.getString(2) shouldBe "events"
      preview.head.getString(3) shouldBe JobWriteMode.Append.name
      preview.head.getString(4) shouldBe CleanupAction.WouldDeleteFiles
      preview.head.getLong(7) shouldBe 3L
      preview.head.getLong(8) shouldBe 0L
      preview.head.getBoolean(10) shouldBe false
      store.exists(layout.owner) shouldBe true

      val deleted = CleanupStagingProcedure.run(
        store,
        "files",
        owner,
        retentionSeconds = 300L,
        dryRun = false,
        nowMillis = 1000000L
      )
      deleted.head.getString(4) shouldBe CleanupAction.FilesDeleted
      deleted.head.getLong(8) shouldBe 3L
      deleted.head.getLong(9) should be > 0L
      deleted.head.getBoolean(10) shouldBe false
      Files.isDirectory(dir.resolve(layout.prefix)) shouldBe true
    } finally {
      store.close()
      Files
        .walk(dir)
        .sorted(Comparator.reverseOrder[Path]())
        .forEach(path => Files.delete(path))
    }
  }

  test(
    "cleanup_staging enforces a five minute retention floor and overflow bound"
  ) {
    val dir = Files.createTempDirectory("cleanup-staging-retention")
    val store = new LocalObjectStore(dir.toString)
    val owner = JobOwner("default", "events")
    try {
      intercept[IllegalArgumentException](
        CleanupStagingProcedure.run(
          store,
          "files",
          owner,
          retentionSeconds = 299L,
          dryRun = true,
          nowMillis = 1000L
        )
      )
      intercept[IllegalArgumentException](
        CleanupStagingProcedure.run(
          store,
          "files",
          owner,
          retentionSeconds = Long.MaxValue,
          dryRun = true,
          nowMillis = 1000L
        )
      )
    } finally {
      store.close()
      Files.delete(dir)
    }
  }

  private def age(root: Path, millis: Long): Unit = {
    val stream = Files.walk(root)
    try
      stream.forEach(path =>
        if (Files.isRegularFile(path)) {
          Files.setLastModifiedTime(path, FileTime.fromMillis(millis))
        }
      )
    finally stream.close()
  }
}
