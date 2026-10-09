package com.zilliz.spark.connector.procedure

import org.apache.spark.sql.types.{
  ArrayType,
  LongType,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.sql.Row

import com.zilliz.milvus.client.api.{MilvusClient, MilvusSnapshotInfo}

private[procedure] object SnapshotProcedureResult {
  val metadataSchema: StructType = StructType(
    Seq(
      StructField("database", StringType, nullable = false),
      StructField("collection", StringType, nullable = false),
      StructField("snapshot", StringType, nullable = false),
      StructField("description", StringType, nullable = false),
      StructField(
        "partition_names",
        ArrayType(StringType, containsNull = false),
        nullable = false
      ),
      StructField("create_ts", LongType, nullable = false),
      StructField("s3_location", StringType, nullable = false)
    )
  )

  def metadata(target: ProcedureTarget, snapshot: MilvusSnapshotInfo): Row =
    Row(
      target.database,
      snapshot.collectionName,
      snapshot.name,
      snapshot.description,
      snapshot.partitionNames,
      snapshot.createTs,
      snapshot.s3Location
    )
}

/** Creates an online Milvus snapshot and returns the metadata materialized by
  * Milvus (capability A1).
  */
object CreateSnapshotProcedure extends Procedure {
  override val name: String = "create_snapshot"

  override val parameters: Seq[Parameter] = Seq(
    Parameter("collection", StringType),
    Parameter("name", StringType),
    Parameter("description", StringType, required = false),
    Parameter("compaction_protection_seconds", LongType, required = false)
  )

  override val outputSchema: StructType = SnapshotProcedureResult.metadataSchema

  override def run(args: ProcedureArgs): Seq[Row] =
    ProcedureSupport.withClient(args, name) { (target, client) =>
      run(args, target, client)
    }

  private[procedure] def run(
      args: ProcedureArgs,
      target: ProcedureTarget,
      client: MilvusClient
  ): Seq[Row] = {
    val snapshotName =
      ProcedureSupport.nonBlank(args.string("name"), name, "name")
    val protectionSeconds = ProcedureSupport.nonNegative(
      args.longOpt("compaction_protection_seconds").getOrElse(0L),
      name,
      "compaction_protection_seconds"
    )
    client
      .createSnapshot(
        target.database,
        target.collection,
        snapshotName,
        args.stringOpt("description").getOrElse(""),
        protectionSeconds
      )
      .get
    val snapshot = client
      .describeSnapshotWithRetry(
        target.database,
        target.collection,
        snapshotName
      )
      .fold(
        error =>
          throw new IllegalStateException(
            s"procedure $name: snapshot '$snapshotName' was created, but its metadata could not be read; the snapshot was left intact",
            error
          ),
        identity
      )
    Seq(SnapshotProcedureResult.metadata(target, snapshot))
  }
}

/** Drops one online Milvus snapshot (capability A1). */
object DropSnapshotProcedure extends Procedure {
  override val name: String = "drop_snapshot"

  override val parameters: Seq[Parameter] = Seq(
    Parameter("collection", StringType),
    Parameter("name", StringType)
  )

  override val outputSchema: StructType = StructType(
    Seq(
      StructField("database", StringType, nullable = false),
      StructField("collection", StringType, nullable = false),
      StructField("snapshot", StringType, nullable = false),
      StructField("status", StringType, nullable = false)
    )
  )

  override def run(args: ProcedureArgs): Seq[Row] =
    ProcedureSupport.withClient(args, name) { (target, client) =>
      run(args, target, client)
    }

  private[procedure] def run(
      args: ProcedureArgs,
      target: ProcedureTarget,
      client: MilvusClient
  ): Seq[Row] = {
    val snapshotName =
      ProcedureSupport.nonBlank(args.string("name"), name, "name")
    client
      .dropSnapshot(target.database, target.collection, snapshotName)
      .get
    Seq(Row(target.database, target.collection, snapshotName, "dropped"))
  }
}

/** Lists the names of the online snapshots for one collection (capability A1).
  */
object ListSnapshotsProcedure extends Procedure {
  override val name: String = "list_snapshots"

  override val parameters: Seq[Parameter] = Seq(
    Parameter("collection", StringType)
  )

  override val outputSchema: StructType = StructType(
    Seq(
      StructField("database", StringType, nullable = false),
      StructField("collection", StringType, nullable = false),
      StructField("snapshot", StringType, nullable = false)
    )
  )

  override def run(args: ProcedureArgs): Seq[Row] =
    ProcedureSupport.withClient(args, name) { (target, client) =>
      run(args, target, client)
    }

  private[procedure] def run(
      args: ProcedureArgs,
      target: ProcedureTarget,
      client: MilvusClient
  ): Seq[Row] =
    client
      .listSnapshots(target.database, target.collection)
      .get
      .map(snapshot => Row(target.database, target.collection, snapshot))
}

/** Describes one online Milvus snapshot (capability A1). */
object DescribeSnapshotProcedure extends Procedure {
  override val name: String = "describe_snapshot"

  override val parameters: Seq[Parameter] = Seq(
    Parameter("collection", StringType),
    Parameter("name", StringType)
  )

  override val outputSchema: StructType = SnapshotProcedureResult.metadataSchema

  override def run(args: ProcedureArgs): Seq[Row] =
    ProcedureSupport.withClient(args, name) { (target, client) =>
      run(args, target, client)
    }

  private[procedure] def run(
      args: ProcedureArgs,
      target: ProcedureTarget,
      client: MilvusClient
  ): Seq[Row] = {
    val snapshotName =
      ProcedureSupport.nonBlank(args.string("name"), name, "name")
    val snapshot = client
      .describeSnapshot(target.database, target.collection, snapshotName)
      .get
    Seq(SnapshotProcedureResult.metadata(target, snapshot))
  }
}
