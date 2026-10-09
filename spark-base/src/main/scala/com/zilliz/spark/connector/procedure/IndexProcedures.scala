package com.zilliz.spark.connector.procedure

import org.apache.spark.sql.types.{
  BooleanType,
  LongType,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.sql.Row

import com.zilliz.milvus.client.api.{MilvusClient, MilvusIndexInfo}
import io.milvus.grpc.common.IndexState

/** Creates a Milvus index, optionally waiting for the build to finish
  * (capability A2).
  */
object CreateIndexProcedure extends Procedure {
  override val name: String = "create_index"

  override val parameters: Seq[Parameter] = Seq(
    Parameter("collection", StringType),
    Parameter("field", StringType),
    Parameter("index_name", StringType),
    Parameter("index_type", StringType, required = false),
    Parameter("metric_type", StringType, required = false),
    Parameter("params", StringType, required = false),
    Parameter("wait", BooleanType, required = false),
    Parameter("timeout_seconds", LongType, required = false)
  )

  override val outputSchema: StructType = StructType(
    Seq(
      StructField("database", StringType, nullable = false),
      StructField("collection", StringType, nullable = false),
      StructField("field", StringType, nullable = false),
      StructField("index_name", StringType, nullable = false),
      StructField("state", StringType, nullable = false)
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
    val field = ProcedureSupport.nonBlank(args.string("field"), name, "field")
    val indexName =
      ProcedureSupport.nonBlank(args.string("index_name"), name, "index_name")
    val waitOptions = ProcedureSupport.waitOptions(args, name)
    val baseParams = Map(
      "index_type" -> args.stringOpt("index_type").getOrElse("AUTOINDEX"),
      "metric_type" -> args.stringOpt("metric_type").getOrElse("L2")
    )
    val indexParams = args.stringOpt("params") match {
      case Some(value) => baseParams + ("params" -> value)
      case None        => baseParams
    }

    client
      .createIndex(
        target.database,
        target.collection,
        field,
        indexParams,
        indexName = indexName
      )
      .get

    val state =
      if (!waitOptions.enabled) "submitted"
      else {
        ProcedureSupport.await(
          s"procedure $name for ${target.database}.${target.collection}",
          waitOptions.timeoutSeconds
        )(remainingMillis =>
          currentIndex(client, target, field, indexName, remainingMillis)
        ) {
          case None => WaitDecision.Continue
          case Some(index) if index.state == IndexState.Finished =>
            WaitDecision.Done
          case Some(index) if index.state == IndexState.Failed =>
            WaitDecision.Failed(
              s"index '${index.indexName}' entered Failed: ${index.failReason}"
            )
          case Some(_) => WaitDecision.Continue
        }
        IndexState.Finished.toString
      }

    Seq(Row(target.database, target.collection, field, indexName, state))
  }

  private def currentIndex(
      client: MilvusClient,
      target: ProcedureTarget,
      field: String,
      indexName: String,
      timeoutMillis: Long
  ): Option[MilvusIndexInfo] =
    client
      .describeIndexes(
        target.database,
        target.collection,
        field,
        indexName,
        timeoutMillis
      )
      .get
      .find(index => index.fieldName == field && index.indexName == indexName)
}

/** Drops one named Milvus index (capability A2). */
object DropIndexProcedure extends Procedure {
  override val name: String = "drop_index"

  override val parameters: Seq[Parameter] = Seq(
    Parameter("collection", StringType),
    Parameter("index_name", StringType)
  )

  override val outputSchema: StructType = StructType(
    Seq(
      StructField("database", StringType, nullable = false),
      StructField("collection", StringType, nullable = false),
      StructField("index_name", StringType, nullable = false),
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
    val indexName =
      ProcedureSupport.nonBlank(args.string("index_name"), name, "index_name")
    client.dropIndex(target.database, target.collection, indexName).get
    Seq(Row(target.database, target.collection, indexName, "dropped"))
  }
}
