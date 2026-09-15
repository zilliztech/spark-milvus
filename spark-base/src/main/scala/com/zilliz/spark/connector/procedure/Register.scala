package com.zilliz.spark.connector.procedure

import scala.util.Try

import org.apache.spark.internal.Logging

import com.zilliz.milvus.client.api.MilvusClient
import com.zilliz.milvus.storage.io.ObjectStore
import com.zilliz.milvus.storage.write.commit.{Committer, Registration}
import com.zilliz.milvus.storage.write.exec.StagingLayout
import com.zilliz.spark.connector.options.{HadoopStorageKeys, MilvusOption}
import io.milvus.grpc.common.Status

/** `register`: hands a committed write job's segments to Milvus (capability
  * A4). Reads `staging/{job}/manifest.json`, calls `BatchUpdateManifest` with
  * every segment's id and new manifest version, and marks the job registered so
  * a second call does nothing. Only a backfill job can be registered today; a
  * job that created new segments is refused, because Milvus has no
  * RegisterSegments yet.
  *
  * This is the procedure's body. `CALL milvus.system.register(...)` in SQL is
  * not wired yet; until it is, a job calls `Register.run` directly.
  */
object Register extends Logging {

  final case class Outcome(
      jobId: String,
      items: Seq[Registration.Item],
      alreadyRegistered: Boolean
  )

  /** @param options
    *   the connection (`milvus.uri`, `milvus.token`, `milvus.database.name`,
    *   `milvus.collection.name`) and the `fs.*` of the bucket the staging
    *   prefix is in
    * @param stagingPrefix
    *   the job's staging prefix as a key relative to the bucket:
    *   `{root}/staging/{job id}`
    */
  def run(options: Map[String, String], stagingPrefix: String): Outcome = {
    val milvusOption = MilvusOption(options)
    val store = HadoopStorageKeys.storeFrom(options)
    try {
      val client = MilvusClient(milvusOption.connectionParams)
      try
        run(
          store,
          layoutOf(stagingPrefix),
          items =>
            client.batchUpdateManifest(
              milvusOption.databaseName,
              milvusOption.collectionName,
              items.map(i => (i.segmentId, i.manifestVersion))
            )
        )
      finally client.close()
    } finally store.close()
  }

  /** The layout `{root}/staging/{job}` names. */
  def layoutOf(stagingPrefix: String): StagingLayout = {
    val prefix = stagingPrefix.trim.stripSuffix("/")
    val parts = prefix.split("/")
    if (parts.length < 2 || parts(parts.length - 2) != "staging") {
      throw new IllegalArgumentException(
        s"'$stagingPrefix' is not a staging prefix; expected {root}/staging/{job id}"
      )
    }
    val root = parts.dropRight(2).mkString("/")
    val layout = StagingLayout(root, parts.last)
    require(
      layout.prefix == prefix,
      s"$prefix does not round-trip as a StagingLayout"
    )
    layout
  }

  private[procedure] def run(
      store: ObjectStore,
      layout: StagingLayout,
      register: Seq[Registration.Item] => Try[Status]
  ): Outcome = {
    val committer = new Committer(store, layout)
    if (!committer.isCommitted) {
      throw new IllegalStateException(
        s"job ${layout.jobId} is not committed: ${layout.marker} is missing"
      )
    }
    val manifest = committer.manifest()
    val items = Registration
      .backfillItems(manifest)
      .fold(reason => throw new IllegalStateException(reason), identity)
    if (committer.isRegistered) {
      logInfo(s"Job ${layout.jobId} is already registered; nothing sent")
      return Outcome(layout.jobId, items, alreadyRegistered = true)
    }
    register(items).fold(
      e =>
        throw new IllegalStateException(
          s"Milvus refused the ${items.size} segment(s) of job ${layout.jobId}: ${e.getMessage}",
          e
        ),
      _ => ()
    )
    committer.markRegistered()
    logInfo(
      s"Job ${layout.jobId} registered: " + items
        .map(i => s"segment ${i.segmentId} at manifest ${i.manifestVersion}")
        .mkString(", ")
    )
    Outcome(layout.jobId, items, alreadyRegistered = false)
  }
}
