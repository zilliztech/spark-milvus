package com.zilliz.spark.connector.apps.backfill

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

import com.zilliz.milvus.storage.io.LocalObjectStore
import com.zilliz.milvus.storage.write.commit.{CommittedSegment, JobManifest}
import com.zilliz.milvus.storage.write.exec.StagingLayout

/** Two backfills in one application shared a job id, and the second one's
  * manifest versions were silently dropped.
  */
class BackfillJobManifestTest
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterAll {

  private lazy val spark = SparkSession
    .builder()
    .master("local[1]")
    .appName("backfill-job-manifest")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  override protected def afterAll(): Unit = spark.stop()

  private val config = BackfillConfig(
    milvusUri = "http://localhost:19530",
    collectionName = "c",
    s3Endpoint = "localhost:9000",
    s3BucketName = "b",
    s3AccessKey = "ak",
    s3SecretKey = "sk",
    s3RootPath = "files"
  )

  private def segment(version: Long) = CommittedSegment(
    partitionId = 0,
    basePath = "files/insert_log/1/2/30",
    manifestVersion = version,
    rowCount = 10L,
    segmentId = Some(30L)
  )

  private def withStore(f: (LocalObjectStore, Path) => Unit): Unit = {
    val dir = Files.createTempDirectory("backfill-job-manifest")
    try f(new LocalObjectStore(dir.toString), dir)
    finally
      Files
        .walk(dir)
        .sorted(java.util.Comparator.reverseOrder[Path]())
        .forEach(p => Files.delete(p))
  }

  test("each run gets its own job id unless the caller names one") {
    MilvusBackfill.jobIdFor(spark, config) should not be
      MilvusBackfill.jobIdFor(spark, config)
    MilvusBackfill.jobIdFor(spark, config.copy(jobId = Some("j"))) shouldBe "j"
  }

  test(
    "a job manifest already committed under the id is never passed off as this run's"
  ) {
    withStore { (store, dir) =>
      val layout = StagingLayout("files", "j")
      MilvusBackfill.commitJobManifest(store, layout, Seq(segment(3L)))
      val e = intercept[IllegalStateException](
        MilvusBackfill.commitJobManifest(store, layout, Seq(segment(4L)))
      )
      e.getMessage should include("files/staging/j")
      JobManifest
        .fromJson(
          new String(
            Files.readAllBytes(dir.resolve(layout.manifest)),
            StandardCharsets.UTF_8
          )
        )
        .fold(e => throw e, identity)
        .segments
        .map(_.manifestVersion) shouldBe Seq(3L)
    }
  }

  test(
    "a job id with a committed manifest is refused before anything is written"
  ) {
    withStore { (store, _) =>
      val layout = StagingLayout("files", "j")
      MilvusBackfill.ensureJobIdUnused(store, layout)
      MilvusBackfill.commitJobManifest(store, layout, Seq(segment(3L)))
      intercept[IllegalStateException](
        MilvusBackfill.ensureJobIdUnused(store, layout)
      ).getMessage should include("cannot be reused")
    }
  }
}
