package com.zilliz.spark.connector.procedure

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.types.{
  BooleanType,
  LongType,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.sql.Row

import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.milvus.storage.io.ObjectStore
import com.zilliz.milvus.storage.write.commit.{
  JobOwner,
  StagingCleaner,
  StagingCleanupResult
}
import com.zilliz.spark.connector.options.{HadoopStorageKeys, MilvusOption}

/** Audits and, when requested, removes the file objects of stale append jobs.
  * The native filesystem cannot delete directories yet, so every result reports
  * `prefix_deleted = false` and the remaining directory count.
  */
object CleanupStagingProcedure extends Procedure {
  override val name: String = "cleanup_staging"

  private val DefaultRetentionSeconds = TimeUnit.DAYS.toSeconds(7L)
  private val MinimumRetentionSeconds = TimeUnit.MINUTES.toSeconds(5L)

  override val parameters: Seq[Parameter] = Seq(
    Parameter("collection", StringType),
    Parameter("retention_seconds", LongType, required = false),
    Parameter("dry_run", BooleanType, required = false)
  )

  override val outputSchema: StructType = StructType(
    Seq(
      StructField("job_id", StringType, nullable = false),
      StructField("database", StringType, nullable = true),
      StructField("collection", StringType, nullable = true),
      StructField("write_mode", StringType, nullable = true),
      StructField("action", StringType, nullable = false),
      StructField("reason", StringType, nullable = false),
      StructField("last_heartbeat_ms", LongType, nullable = true),
      StructField("candidate_files", LongType, nullable = false),
      StructField("deleted_files", LongType, nullable = false),
      StructField("directories_remaining", LongType, nullable = false),
      StructField("prefix_deleted", BooleanType, nullable = false)
    )
  )

  override def run(args: ProcedureArgs): Seq[Row] =
    run(args, HadoopStorageKeys.storeFrom, System.currentTimeMillis())

  private[procedure] def run(
      args: ProcedureArgs,
      openStore: Map[String, String] => ObjectStore,
      nowMillis: Long
  ): Seq[Row] = {
    val (database, collection) =
      ProcedureSupport.parseCollection(args.string("collection"), name)
    val options = ProcedureSupport.collectionOptions(
      args.options,
      database,
      collection,
      name
    )
    val owner = JobOwner(
      options(MilvusOption.MilvusDatabaseName),
      collection
    )
    val retentionSeconds = args
      .longOpt("retention_seconds")
      .getOrElse(DefaultRetentionSeconds)
    validateRetention(retentionSeconds)
    val dryRun = args.booleanOpt("dry_run").getOrElse(true)
    val properties = HadoopStorageKeys.canonicalProperties(options)
    val rootPath = properties(StorageProperties.RootPath)
    val store = openStore(properties)
    try
      rows(
        new StagingCleaner(store, rootPath).clean(
          owner,
          TimeUnit.SECONDS.toMillis(retentionSeconds),
          dryRun,
          nowMillis
        )
      )
    finally store.close()
  }

  private[procedure] def run(
      store: ObjectStore,
      rootPath: String,
      owner: JobOwner,
      retentionSeconds: Long,
      dryRun: Boolean,
      nowMillis: Long
  ): Seq[Row] = {
    validateRetention(retentionSeconds)
    rows(
      new StagingCleaner(store, rootPath).clean(
        owner,
        TimeUnit.SECONDS.toMillis(retentionSeconds),
        dryRun,
        nowMillis
      )
    )
  }

  private def validateRetention(seconds: Long): Unit = {
    if (seconds < MinimumRetentionSeconds) {
      throw new IllegalArgumentException(
        s"procedure $name: argument 'retention_seconds' must be at least $MinimumRetentionSeconds, got $seconds"
      )
    }
    if (seconds > Long.MaxValue / 1000L) {
      throw new IllegalArgumentException(
        s"procedure $name: argument 'retention_seconds' is too large: $seconds"
      )
    }
  }

  private def rows(results: Seq[StagingCleanupResult]): Seq[Row] =
    results.map { result =>
      Row(
        result.jobId,
        result.owner.map(_.database).orNull,
        result.owner.map(_.collection).orNull,
        result.writeMode.orNull,
        result.action,
        result.reason,
        result.lastHeartbeatMillis.map(java.lang.Long.valueOf).orNull,
        result.candidateFiles.toLong,
        result.deletedFiles.toLong,
        result.directoriesRemaining.toLong,
        result.prefixDeleted
      )
    }
}
