package com.zilliz.spark.connector.procedure

import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.types.{BooleanType, LongType, StringType}
import org.apache.spark.sql.Row
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.client.api.{
  MilvusClient,
  MilvusConnectionParams,
  MilvusRestoreSnapshotJob
}
import com.zilliz.milvus.storage.credential.StorageProperties
import io.milvus.grpc.milvus.RestoreSnapshotState

/** What `CALL milvus.system.restore_snapshot(...)` takes, asks Milvus, and
  * answers with (docs/design/architecture/vector-search.html section 2.7). The
  * root check before the call is core's, tested with a real snapshot in
  * `SnapshotWriteTest`.
  */
class RestoreSnapshotProcedureTest extends AnyFunSuite with Matchers {

  private val target = ProcedureTarget(
    "db1",
    "restored",
    MilvusConnectionParams("http://localhost:19530")
  )
  private val documentKey = "files/snapshots/10/metadata/5.json"
  private val uri = s"s3://bucket/$documentKey"
  private val baseArgs = ProcedureArgs(
    Map("collection" -> "db1.restored", "snapshot" -> documentKey),
    Map.empty
  )
  private val noWait = WaitOptions(enabled = false, 600L)
  private val waiting = WaitOptions(enabled = true, 600L)

  private def job(
      state: RestoreSnapshotState,
      progress: Int = 100,
      reason: String = ""
  ) = MilvusRestoreSnapshotJob(
    jobId = 42L,
    snapshotName = "c-5",
    dbName = "db1",
    collectionName = "restored",
    state = state,
    progress = progress,
    reason = reason,
    startTimeMillis = 1000L,
    timeCostMillis = 5900L
  )

  test("the call names the target, the snapshot document and how to wait") {
    RestoreSnapshotProcedure.parameters
      .filter(_.required)
      .map(_.name) shouldBe Seq("collection", "snapshot")
    RestoreSnapshotProcedure.parameters
      .filterNot(_.required)
      .map(_.name) shouldBe Seq(
      "snapshot_uri",
      "external_spec",
      "wait",
      "timeout_seconds"
    )
    RestoreSnapshotProcedure.parameters
      .find(_.name == "wait")
      .map(_.dataType) shouldBe Some(BooleanType)
    RestoreSnapshotProcedure.parameters
      .find(_.name == "timeout_seconds")
      .map(_.dataType) shouldBe Some(LongType)
    RestoreSnapshotProcedure.parameters
      .find(_.name == "snapshot")
      .map(_.dataType) shouldBe Some(StringType)
    RestoreSnapshotProcedure.outputSchema.fieldNames.toSeq shouldBe Seq(
      "database",
      "collection",
      "snapshot_uri",
      "job_id",
      "state",
      "progress",
      "reason",
      "time_cost_ms"
    )
    Procedures.byName("RESTORE_SNAPSHOT") shouldBe Some(
      RestoreSnapshotProcedure
    )
  }

  test("without wait the row reports the job Milvus opened") {
    val client = new FakeMilvusClient

    RestoreSnapshotProcedure.restore(
      baseArgs,
      target,
      client,
      uri,
      noWait
    ) shouldBe Seq(
      Row("db1", "restored", uri, 42L, "submitted", null, null, null)
    )
    client.restoreCalls shouldBe Seq(("db1", "restored", uri, ""))
    client.stateCalls shouldBe empty
  }

  test("external_spec goes to Milvus as it is") {
    val client = new FakeMilvusClient
    val spec = """{"extfs":{"provider":"aws","region":"us-west-2"}}"""

    RestoreSnapshotProcedure.restore(
      baseArgs.copy(values = baseArgs.values + ("external_spec" -> spec)),
      target,
      client,
      uri,
      noWait
    )
    client.restoreCalls.map(_._4) shouldBe Seq(spec)
  }

  test("with wait the job is polled until it completes") {
    val client = new FakeMilvusClient
    client.state = Success(job(RestoreSnapshotState.RestoreSnapshotCompleted))

    RestoreSnapshotProcedure.restore(
      baseArgs,
      target,
      client,
      uri,
      waiting
    ) shouldBe Seq(
      Row(
        "db1",
        "restored",
        uri,
        42L,
        "RestoreSnapshotCompleted",
        100,
        "",
        5900L
      )
    )
    client.stateCalls shouldBe Seq(42L)
    client.stateTimeouts should have size 1
    client.stateTimeouts.head should be > 0L
  }

  test("a failed job is a procedure failure that carries Milvus's reason") {
    val client = new FakeMilvusClient
    client.state = Success(
      job(
        RestoreSnapshotState.RestoreSnapshotFailed,
        progress = 30,
        reason = "data path built/index_files/1 is outside source root"
      )
    )

    val error = intercept[IllegalStateException] {
      RestoreSnapshotProcedure.restore(baseArgs, target, client, uri, waiting)
    }
    error.getMessage should include("restore job 42 failed")
    error.getMessage should include("is outside source root")
  }

  test("client failures remain procedure failures") {
    val client = new FakeMilvusClient
    client.restored = Failure(
      new IllegalStateException("collection 'restored' already exists")
    )

    val error = intercept[IllegalStateException] {
      RestoreSnapshotProcedure.restore(baseArgs, target, client, uri, noWait)
    }
    error.getMessage shouldBe "collection 'restored' already exists"
  }

  test("the snapshot is a key inside the bucket; a URI goes in snapshot_uri") {
    RestoreSnapshotProcedure.snapshotKey(s"/$documentKey") shouldBe documentKey

    the[IllegalArgumentException] thrownBy RestoreSnapshotProcedure
      .snapshotKey(uri) should have message
      s"requirement failed: 'snapshot' is the key of the snapshot document inside the bucket, not a URI: '$uri'; " +
      "a URI Milvus should be handed goes in 'snapshot_uri'"

    an[IllegalArgumentException] should be thrownBy RestoreSnapshotProcedure
      .snapshotKey("  ")
  }

  test("the default URI names the bucket, and local storage has to be told") {
    RestoreSnapshotProcedure.defaultUri(
      Map(StorageProperties.BucketName -> "bucket"),
      documentKey
    ) shouldBe uri

    the[IllegalArgumentException] thrownBy RestoreSnapshotProcedure
      .defaultUri(
        Map(StorageProperties.StorageType -> "local"),
        documentKey
      ) should have message
      "requirement failed: On local storage Milvus has to be told where the snapshot is: pass 'snapshot_uri'"

    the[IllegalArgumentException] thrownBy RestoreSnapshotProcedure
      .defaultUri(Map.empty, documentKey) should have message
      s"'${StorageProperties.BucketName}' names the bucket the snapshot URI is built from; set it, or pass 'snapshot_uri'"
  }

  private final class FakeMilvusClient
      extends MilvusClient(target.connectionParams) {
    var restored: Try[Long] = Success(42L)
    var state: Try[MilvusRestoreSnapshotJob] =
      Success(job(RestoreSnapshotState.RestoreSnapshotPending, progress = 0))

    var restoreCalls = Seq.empty[(String, String, String, String)]
    var stateCalls = Seq.empty[Long]
    var stateTimeouts = Seq.empty[Long]

    override def restoreExternalSnapshot(
        dbName: String,
        targetCollectionName: String,
        metadataUri: String,
        externalSpec: String
    ): Try[Long] = {
      restoreCalls :+= ((
        dbName,
        targetCollectionName,
        metadataUri,
        externalSpec
      ))
      restored
    }

    override def getRestoreSnapshotState(
        jobId: Long,
        timeoutMillis: Long
    ): Try[MilvusRestoreSnapshotJob] = {
      stateCalls :+= jobId
      stateTimeouts :+= timeoutMillis
      state
    }
  }
}
