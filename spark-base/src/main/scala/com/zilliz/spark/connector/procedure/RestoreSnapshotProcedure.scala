package com.zilliz.spark.connector.procedure

import org.apache.spark.sql.types.{
  BooleanType,
  IntegerType,
  LongType,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.sql.Row

import com.zilliz.milvus.client.api.MilvusClient
import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.milvus.storage.io.{NativeObjectStore, ObjectStore}
import com.zilliz.milvus.storage.write.commit.SnapshotBundle
import com.zilliz.spark.connector.options.StorageOptions
import io.milvus.grpc.milvus.RestoreSnapshotState

/** `CALL milvus.system.restore_snapshot(...)`: asks Milvus to restore a
  * snapshot that lives on object storage — the one `write_snapshot` wrote,
  * indexes and all — into a collection that does not exist yet. This is the
  * delivery half of W8; the files were written by the call before it.
  *
  * `collection` is the target, `db.collection` or `collection` under the option
  * database. `snapshot` is the key of the snapshot JSON, what `write_snapshot`
  * returned. Milvus is handed a URI for it: `snapshot_uri` when the call gives
  * one, otherwise `s3://{fs.bucket_name}/{snapshot}`. `external_spec` goes to
  * Milvus as it is, for a snapshot on storage other than the instance's own.
  *
  * Before Milvus is asked, the document and its segment manifests are read
  * through the same `fs.*` options a read uses, and every path they name is
  * checked against the root the key derives ([[SnapshotBundle]]): a snapshot a
  * restore would refuse is refused here, with the paths named, instead of after
  * Milvus has opened a job for it. Without `wait` the row reports the job
  * Milvus opened; with `wait` the job is polled with the bounded rule the other
  * management procedures share, and the row reports how it ended
  * (docs/design/architecture/vector-search.html section 2.7).
  */
object RestoreSnapshotProcedure extends Procedure {

  override val name: String = "restore_snapshot"

  override val parameters: Seq[Parameter] = Seq(
    Parameter("collection", StringType),
    Parameter("snapshot", StringType),
    Parameter("snapshot_uri", StringType, required = false),
    Parameter("external_spec", StringType, required = false),
    Parameter("wait", BooleanType, required = false),
    Parameter("timeout_seconds", LongType, required = false)
  )

  override val outputSchema: StructType = StructType(
    Seq(
      StructField("database", StringType, nullable = false),
      StructField("collection", StringType, nullable = false),
      StructField("snapshot_uri", StringType, nullable = false),
      StructField("job_id", LongType, nullable = false),
      StructField("state", StringType, nullable = false),
      StructField("progress", IntegerType, nullable = true),
      StructField("reason", StringType, nullable = true),
      StructField("time_cost_ms", LongType, nullable = true)
    )
  )

  override def run(args: ProcedureArgs): Seq[Row] = {
    val waitOptions = ProcedureSupport.waitOptions(args, name)
    val key = snapshotKey(args.string("snapshot"))
    val (database, collection) =
      ProcedureSupport.parseCollection(args.string("collection"), name)
    val options = ProcedureSupport.collectionOptions(
      args.options,
      database,
      collection,
      name
    )
    // The same resolved `fs.*` bag a write uses; the bucket comes from the
    // options because the target collection does not exist yet.
    val properties = StorageOptions.writeStorageProperties(options, "")
    val bucket = properties.getOrElse(StorageProperties.BucketName, "")
    val store = NativeObjectStore.Factory(properties).open()
    try preflight(store, key, bucket)
    finally store.close()
    val uri = args
      .stringOpt("snapshot_uri")
      .map(_.trim)
      .filter(_.nonEmpty)
      .getOrElse(defaultUri(properties, key))
    ProcedureSupport.withClient(args, name) { (target, client) =>
      restore(args, target, client, uri, waitOptions)
    }
  }

  /** What a restore would refuse, refused before it is asked: the document has
    * to exist, be laid out as a snapshot document, and name no file outside the
    * root its key derives.
    */
  private[procedure] def preflight(
      store: ObjectStore,
      key: String,
      bucket: String
  ): Unit = {
    require(
      store.exists(key),
      s"No snapshot document at '$key'; 'snapshot' is the key write_snapshot returned"
    )
    val outside = SnapshotBundle.outsideRootOf(store, key, bucket)
    require(
      outside.isEmpty,
      "Milvus refuses a restore whose files are outside the root its snapshot document derives; " +
        SnapshotBundle.describeOutside(
          SnapshotBundle.rootOf(key).getOrElse(""),
          outside
        ) +
        ". Write the snapshot under a prefix the data already sits below"
    )
  }

  private[procedure] def restore(
      args: ProcedureArgs,
      target: ProcedureTarget,
      client: MilvusClient,
      uri: String,
      waitOptions: WaitOptions
  ): Seq[Row] = {
    val externalSpec =
      args.stringOpt("external_spec").map(_.trim).getOrElse("")
    val jobId = client
      .restoreExternalSnapshot(
        target.database,
        target.collection,
        uri,
        externalSpec
      )
      .get
    if (!waitOptions.enabled) {
      Seq(
        Row(
          target.database,
          target.collection,
          uri,
          jobId,
          "submitted",
          null,
          null,
          null
        )
      )
    } else {
      val job = ProcedureSupport.await(
        s"procedure $name for ${target.database}.${target.collection}",
        waitOptions.timeoutSeconds
      )(remainingMillis =>
        client.getRestoreSnapshotState(jobId, remainingMillis).get
      ) {
        case current
            if current.state == RestoreSnapshotState.RestoreSnapshotCompleted =>
          WaitDecision.Done
        case current
            if current.state == RestoreSnapshotState.RestoreSnapshotFailed =>
          WaitDecision.Failed(
            s"restore job $jobId failed: ${current.reason}"
          )
        case current
            if current.state == RestoreSnapshotState.RestoreSnapshotPending ||
              current.state == RestoreSnapshotState.RestoreSnapshotExecuting ||
              current.state == RestoreSnapshotState.RestoreSnapshotNone =>
          WaitDecision.Continue
        case current =>
          WaitDecision.Failed(s"unexpected restore state ${current.state}")
      }
      Seq(
        Row(
          target.database,
          target.collection,
          uri,
          jobId,
          job.state.name,
          job.progress,
          job.reason,
          job.timeCostMillis
        )
      )
    }
  }

  /** The key of the snapshot document inside the bucket. A URI is not one; it
    * goes through `snapshot_uri`, and the key still has to be given so the
    * document can be read and checked.
    */
  private[procedure] def snapshotKey(value: String): String = {
    val trimmed = Option(value).getOrElse("").trim.stripPrefix("/")
    require(
      trimmed.nonEmpty && !trimmed.contains("://"),
      s"'snapshot' is the key of the snapshot document inside the bucket, not a URI: '$value'; " +
        "a URI Milvus should be handed goes in 'snapshot_uri'"
    )
    trimmed
  }

  /** `s3://{bucket}/{key}`, the form Milvus writes its own snapshot locations
    * in. Local storage has no bucket Milvus could reach, so there the call has
    * to say where the snapshot is.
    */
  private[procedure] def defaultUri(
      properties: Map[String, String],
      key: String
  ): String = {
    val local = properties
      .get(StorageProperties.StorageType)
      .exists(_.trim.equalsIgnoreCase(StorageProperties.StorageTypeLocal))
    require(
      !local,
      "On local storage Milvus has to be told where the snapshot is: pass 'snapshot_uri'"
    )
    val bucket = properties
      .get(StorageProperties.BucketName)
      .map(_.trim)
      .filter(_.nonEmpty)
      .getOrElse(
        throw new IllegalArgumentException(
          s"'${StorageProperties.BucketName}' names the bucket the snapshot URI is built from; " +
            "set it, or pass 'snapshot_uri'"
        )
      )
    s"s3://$bucket/$key"
  }
}
