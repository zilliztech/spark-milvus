package com.zilliz.spark.connector.procedure

import org.apache.spark.sql.types.{
  BooleanType,
  LongType,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.sql.Row

import com.zilliz.milvus.client.api.{
  MilvusClient,
  MilvusCompactionState,
  MilvusIndexInfo
}
import io.milvus.grpc.common.{CompactionState, LoadState}
import io.milvus.grpc.schema.FieldSchema

/** Loads a collection, optionally waiting until it is available for search
  * (capability A3).
  */
object LoadProcedure extends Procedure {
  override val name: String = "load"

  override val parameters: Seq[Parameter] = Seq(
    Parameter("collection", StringType),
    Parameter("wait", BooleanType, required = false),
    Parameter("timeout_seconds", LongType, required = false)
  )

  override val outputSchema: StructType = CollectionProcedureResult.stateSchema

  override def run(args: ProcedureArgs): Seq[Row] =
    ProcedureSupport.withClient(args, name) { (target, client) =>
      run(args, target, client)
    }

  private[procedure] def run(
      args: ProcedureArgs,
      target: ProcedureTarget,
      client: MilvusClient
  ): Seq[Row] = {
    val waitOptions = ProcedureSupport.waitOptions(args, name)
    client.loadCollection(target.database, target.collection).get

    val state =
      if (!waitOptions.enabled) "submitted"
      else {
        ProcedureSupport
          .await(
            s"procedure $name for ${target.database}.${target.collection}",
            waitOptions.timeoutSeconds
          )(remainingMillis =>
            client
              .getLoadState(
                target.database,
                target.collection,
                remainingMillis
              )
              .get
          ) {
            case LoadState.LoadStateLoaded => WaitDecision.Done
            case LoadState.LoadStateNotExist =>
              WaitDecision.Failed("collection entered LoadStateNotExist")
            case _ => WaitDecision.Continue
          }
          .toString
      }

    Seq(Row(target.database, target.collection, state))
  }
}

/** Releases a loaded collection (capability A3). */
object ReleaseProcedure extends Procedure {
  override val name: String = "release"
  override val parameters: Seq[Parameter] =
    Seq(Parameter("collection", StringType))
  override val outputSchema: StructType = CollectionProcedureResult.statusSchema

  override def run(args: ProcedureArgs): Seq[Row] =
    ProcedureSupport.withClient(args, name) { (target, client) =>
      run(args, target, client)
    }

  private[procedure] def run(
      args: ProcedureArgs,
      target: ProcedureTarget,
      client: MilvusClient
  ): Seq[Row] = {
    client.releaseCollection(target.database, target.collection).get
    Seq(Row(target.database, target.collection, "released"))
  }
}

/** Submits a flush request for a collection (capability A3). */
object FlushProcedure extends Procedure {
  override val name: String = "flush"
  override val parameters: Seq[Parameter] =
    Seq(Parameter("collection", StringType))
  override val outputSchema: StructType = CollectionProcedureResult.statusSchema

  override def run(args: ProcedureArgs): Seq[Row] =
    ProcedureSupport.withClient(args, name) { (target, client) =>
      run(args, target, client)
    }

  private[procedure] def run(
      args: ProcedureArgs,
      target: ProcedureTarget,
      client: MilvusClient
  ): Seq[Row] = {
    client.flush(target.database, Seq(target.collection)).get
    Seq(Row(target.database, target.collection, "submitted"))
  }
}

/** Starts a manual compaction and optionally waits for all of its plans to
  * complete (capability A3).
  */
object CompactProcedure extends Procedure {
  override val name: String = "compact"

  override val parameters: Seq[Parameter] = Seq(
    Parameter("collection", StringType),
    Parameter("wait", BooleanType, required = false),
    Parameter("timeout_seconds", LongType, required = false)
  )

  override val outputSchema: StructType = StructType(
    Seq(
      StructField("database", StringType, nullable = false),
      StructField("collection", StringType, nullable = false),
      StructField("compaction_id", LongType, nullable = false),
      StructField("plan_count", LongType, nullable = false),
      StructField("state", StringType, nullable = false),
      StructField("executing_plan_count", LongType, nullable = true),
      StructField("completed_plan_count", LongType, nullable = true),
      StructField("failed_plan_count", LongType, nullable = true),
      StructField("timeout_plan_count", LongType, nullable = true)
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
    val waitOptions = ProcedureSupport.waitOptions(args, name)
    val submitted =
      client.manualCompaction(target.database, target.collection).get

    if (!waitOptions.enabled) {
      Seq(
        Row(
          target.database,
          target.collection,
          submitted.compactionID,
          submitted.compactionPlanCount.toLong,
          "submitted",
          null,
          null,
          null,
          null
        )
      )
    } else {
      val state = ProcedureSupport.await(
        s"procedure $name for ${target.database}.${target.collection}",
        waitOptions.timeoutSeconds
      )(remainingMillis =>
        client
          .getCompactionState(submitted.compactionID, remainingMillis)
          .get
      ) {
        case current if current.state == CompactionState.Executing =>
          WaitDecision.Continue
        case current if current.state == CompactionState.Completed =>
          if (current.failedPlanCount == 0L && current.timeoutPlanCount == 0L)
            WaitDecision.Done
          else
            WaitDecision.Failed(
              s"compaction completed with ${current.failedPlanCount} failed and ${current.timeoutPlanCount} timed-out plan(s)"
            )
        case current =>
          WaitDecision.Failed(s"unexpected compaction state ${current.state}")
      }
      Seq(
        compactionRow(
          target,
          submitted.compactionID,
          submitted.compactionPlanCount,
          state
        )
      )
    }
  }

  private def compactionRow(
      target: ProcedureTarget,
      compactionID: Long,
      planCount: Int,
      state: MilvusCompactionState
  ): Row =
    Row(
      target.database,
      target.collection,
      compactionID,
      planCount.toLong,
      state.state.toString,
      state.executingPlanCount,
      state.completedPlanCount,
      state.failedPlanCount,
      state.timeoutPlanCount
    )
}

/** Describes the collection and emits one row for every schema field/index
  * pair. A field without an index still has one row (capability A5).
  */
object DescribeProcedure extends Procedure {
  override val name: String = "describe"
  override val parameters: Seq[Parameter] =
    Seq(Parameter("collection", StringType))

  override val outputSchema: StructType = StructType(
    Seq(
      StructField("database", StringType, nullable = false),
      StructField("collection", StringType, nullable = false),
      StructField("collection_id", LongType, nullable = false),
      StructField("segment_count", LongType, nullable = false),
      StructField("load_state", StringType, nullable = false),
      StructField("field_id", LongType, nullable = false),
      StructField("field_name", StringType, nullable = false),
      StructField("data_type", StringType, nullable = false),
      StructField("nullable", BooleanType, nullable = false),
      StructField("primary_key", BooleanType, nullable = false),
      StructField("partition_key", BooleanType, nullable = false),
      StructField("clustering_key", BooleanType, nullable = false),
      StructField("auto_id", BooleanType, nullable = false),
      StructField("dimension", LongType, nullable = true),
      StructField("index_id", LongType, nullable = true),
      StructField("index_name", StringType, nullable = true),
      StructField("index_state", StringType, nullable = true),
      StructField("indexed_rows", LongType, nullable = true),
      StructField("total_rows", LongType, nullable = true),
      StructField("pending_index_rows", LongType, nullable = true),
      StructField("index_fail_reason", StringType, nullable = true)
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
    val collection =
      client.getCollectionInfo(target.database, target.collection).get
    val segmentCount =
      client.getSegments(target.database, target.collection).get.size.toLong
    val loadState =
      client.getLoadState(target.database, target.collection).get.toString
    val indexes =
      client
        .describeIndexes(target.database, target.collection)
        .get
        .groupBy(_.fieldName)

    collection.schema.fields.flatMap { field =>
      indexes.get(field.name) match {
        case Some(fieldIndexes) if fieldIndexes.nonEmpty =>
          fieldIndexes
            .sortBy(index => (index.indexName, index.indexID))
            .map(index =>
              describeRow(
                target,
                collection.collectionID,
                segmentCount,
                loadState,
                field,
                Some(index)
              )
            )
        case _ =>
          Seq(
            describeRow(
              target,
              collection.collectionID,
              segmentCount,
              loadState,
              field,
              None
            )
          )
      }
    }
  }

  private def describeRow(
      target: ProcedureTarget,
      collectionID: Long,
      segmentCount: Long,
      loadState: String,
      field: FieldSchema,
      index: Option[MilvusIndexInfo]
  ): Row = {
    val dimension = field.typeParams.find(_.key == "dim").map(_.value.toLong)
    Row(
      target.database,
      target.collection,
      collectionID,
      segmentCount,
      loadState,
      field.fieldID,
      field.name,
      field.dataType.toString,
      field.nullable,
      field.isPrimaryKey,
      field.isPartitionKey,
      field.isClusteringKey,
      field.autoID,
      dimension.map(Long.box).orNull,
      index.map(value => Long.box(value.indexID)).orNull,
      index.map(_.indexName).orNull,
      index.map(_.state.toString).orNull,
      index.map(value => Long.box(value.indexedRows)).orNull,
      index.map(value => Long.box(value.totalRows)).orNull,
      index.map(value => Long.box(value.pendingIndexRows)).orNull,
      index.map(_.failReason).orNull
    )
  }
}

private[procedure] object CollectionProcedureResult {
  val stateSchema: StructType = StructType(
    Seq(
      StructField("database", StringType, nullable = false),
      StructField("collection", StringType, nullable = false),
      StructField("state", StringType, nullable = false)
    )
  )

  val statusSchema: StructType = StructType(
    Seq(
      StructField("database", StringType, nullable = false),
      StructField("collection", StringType, nullable = false),
      StructField("status", StringType, nullable = false)
    )
  )
}
