package com.zilliz.spark.connector

import java.io.File
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.net.URI
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.Base64
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.{
  DefaultScalaModule,
  ScalaObjectMapper
}
import com.google.protobuf.ByteString

import io.milvus.grpc.common.{
  ClientInfo,
  ConsistencyLevel,
  ErrorCode,
  KeyValuePair,
  Status
}
import io.milvus.grpc.common.{SegmentLevel, SegmentState}
import io.milvus.grpc.milvus.{
  ConnectRequest,
  CreateCollectionRequest,
  CreateDatabaseRequest,
  DeleteRequest,
  DescribeCollectionRequest,
  DescribeCollectionResponse,
  GetImportStateRequest,
  GetImportStateResponse,
  GetPersistentSegmentInfoRequest,
  ImportRequest,
  InsertRequest,
  MilvusServiceGrpc,
  MutationResult,
  ShowPartitionsRequest
}
import io.milvus.grpc.schema.{
  CollectionSchema,
  DataType,
  FieldData,
  FieldSchema,
  FunctionSchema,
  ValueField
}

import io.grpc._
import io.grpc.{ClientInterceptor, Metadata, Status => GrpcStatus}
import io.grpc.netty.shaded.io.grpc.netty.{GrpcSslContexts, NettyChannelBuilder}
import io.grpc.stub.MetadataUtils
import io.grpc.Status.Code

/** A simplified client for interacting with Milvus
  */
class MilvusClient(params: MilvusConnectionParams) {
  private val retryInterceptor = new GrpcRetryInterceptor(
    maxRetries = 5,
    initialDelayMillis = 500,
    delayMultiplier = 2.0,
    maxDelayMillis = 5000
  )
  private lazy val channel: ManagedChannel = {
    val uri = new URI(params.uri)
    val scheme = uri.getScheme
    val isHttps = scheme.equalsIgnoreCase("https")
    val host = uri.getHost
    var port = uri.getPort
    if (port == -1) {
      if (isHttps) {
        port = 443
      } else {
        port = 80
      }
    }

    val interceptors = Seq(
      getConnectionMetadataInterceptor(),
      retryInterceptor
    )
    var channelBuilder = if (params.serverPemPath.nonEmpty) {
      val sslContext = GrpcSslContexts
        .forClient()
        .trustManager(
          new File(params.serverPemPath)
        )
        .build()
      NettyChannelBuilder
        .forAddress(host, port)
        .sslContext(sslContext)
    } else if (
      params.clientKeyPath.nonEmpty && params.clientPemPath.nonEmpty && params.caPemPath.nonEmpty
    ) {
      val sslContext = GrpcSslContexts
        .forClient()
        .keyManager(
          new File(params.clientKeyPath),
          new File(params.clientPemPath)
        )
        .trustManager(new File(params.caPemPath))
        .build()
      NettyChannelBuilder
        .forAddress(host, port)
        .sslContext(sslContext)
    } else {
      NettyChannelBuilder
        .forAddress(host, port)
        .usePlaintext()
    }
    channelBuilder = channelBuilder
      .maxInboundMessageSize(Integer.MAX_VALUE)
      .keepAliveTime(60, TimeUnit.SECONDS)
      .keepAliveTimeout(10, TimeUnit.SECONDS)
      .keepAliveWithoutCalls(false)
      .idleTimeout(5, TimeUnit.MINUTES)
      .enableRetry()
      .maxRetryAttempts(5)
      .intercept(interceptors: _*)
    if (isHttps) {
      channelBuilder = channelBuilder.useTransportSecurity()
    }
    channelBuilder.build()
  }
  private lazy val stub: MilvusServiceGrpc.MilvusServiceBlockingStub = {
    val server = MilvusServiceGrpc
      .blockingStub(channel)
      .withWaitForReady()
      .withDeadlineAfter(10, TimeUnit.SECONDS)
    server.connect(
      ConnectRequest(
        clientInfo = Some(
          ClientInfo(
            sdkType = "spark-connector",
            sdkVersion = "0.1.0",
            localTime = java.time.LocalDateTime.now().toString,
            host = java.net.InetAddress.getLocalHost.getHostName,
            user = "scala-sdk-user"
          )
        )
      )
    )
    server
  }

  private lazy val httpClient: HttpClient = {
    HttpClient
      .newBuilder()
      .version(HttpClient.Version.HTTP_2)
      .connectTimeout(Duration.ofSeconds(10))
      .build()
  }

  def getConnectionMetadataInterceptor(): ClientInterceptor = {
    val metaData = new Metadata()
    metaData.put(
      Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER),
      Base64.getEncoder.encodeToString(
        params.token.getBytes(StandardCharsets.UTF_8)
      )
    )
    metaData.put(
      Metadata.Key.of("dbname", Metadata.ASCII_STRING_MARSHALLER),
      params.databaseName
    )
    return MetadataUtils.newAttachHeadersInterceptor(metaData)
  }

  def checkStatus(api: String, status: Status): Try[Status] = {
    if (status.code != 0 || status.errorCode != ErrorCode.Success) {
      Failure(new Exception(s"Failed to $api: ${status.reason}"))
    } else {
      Success(status)
    }
  }

  def createDatabase(
      dbName: String,
      properties: Map[String, String] = Map.empty
  ): Try[Status] = {
    try {
      val status = stub.createDatabase(
        CreateDatabaseRequest(
          dbName = dbName,
          properties = properties
            .map(kv => KeyValuePair(key = kv._1, value = kv._2))
            .toSeq
        )
      )
      checkStatus("createDatabase", status)
    } catch {
      case e: Exception =>
        Failure(
          new Exception(s"Failed to create database: ${e.getMessage}")
        )
    }

  }

  def createCollectionField(
      name: String,
      isPrimary: Boolean = false,
      description: String = "",
      dataType: DataType,
      typeParams: Map[String, String] = Map.empty,
      autoID: Boolean = false,
      elementType: DataType = DataType.None,
      defaultValue: Option[ValueField] = None,
      isDynamic: Boolean = false,
      isPartitionKey: Boolean = false,
      isClusteringKey: Boolean = false,
      nullable: Boolean = false,
      isFunctionOutput: Boolean = false
  ): FieldSchema = {
    FieldSchema(
      name = name,
      isPrimaryKey = isPrimary,
      description = description,
      dataType = dataType,
      typeParams =
        typeParams.map(kv => KeyValuePair(key = kv._1, value = kv._2)).toSeq,
      autoID = autoID,
      elementType = elementType,
      defaultValue = defaultValue,
      isDynamic = isDynamic,
      isPartitionKey = isPartitionKey,
      isClusteringKey = isClusteringKey,
      nullable = nullable,
      isFunctionOutput = isFunctionOutput
    )
  }

  def createCollectionSchema(
      dbName: String = "",
      name: String,
      description: String = "",
      fields: Seq[FieldSchema],
      enableDynamicSchema: Boolean = false,
      enableAutoID: Boolean = false,
      properties: Map[String, String] = Map.empty,
      functions: Seq[FunctionSchema] = Seq.empty
  ): CollectionSchema = {
    CollectionSchema(
      name = name,
      description = description,
      fields = fields,
      autoID = enableAutoID,
      enableDynamicField = enableDynamicSchema,
      properties = properties
        .map(kv => KeyValuePair(key = kv._1, value = kv._2))
        .toSeq,
      functions = functions,
      dbName = dbName
    )
  }

  def createCollection(
      dbName: String = "",
      collectionName: String,
      schema: CollectionSchema,
      shardsNum: Int = 1,
      consistencyLevel: ConsistencyLevel = ConsistencyLevel.Strong,
      numPartitions: Long = 0L,
      properties: Map[String, String] = Map.empty
  ): Try[Status] = {
    try {
      val status = stub.createCollection(
        CreateCollectionRequest(
          dbName = dbName,
          collectionName = collectionName,
          schema = schema.toByteString,
          shardsNum = shardsNum,
          consistencyLevel = consistencyLevel,
          numPartitions = numPartitions,
          properties = properties
            .map(kv => KeyValuePair(key = kv._1, value = kv._2))
            .toSeq
        )
      )
      checkStatus("createCollection", status)
    } catch {
      case e: Exception =>
        Failure(
          new Exception(s"Failed to create collection: ${e.getMessage}")
        )
    }
  }

  def packFieldData(): FieldData = {
    FieldData(
      `type` = DataType.Int64,
      fieldName = "pk",
      isDynamic = false,
      validData = Seq.empty
    )
  }

  def insert(
      dbName: String = "",
      collectionName: String,
      partitionName: Option[String] = None,
      fieldsData: Seq[FieldData] = Seq.empty,
      numRows: Int = 0,
      schemaTimestamp: Long = 0L
  ): Try[Status] = {
    try {
      val insertResult = stub.insert(
        InsertRequest(
          dbName = dbName,
          collectionName = collectionName,
          partitionName = partitionName.getOrElse(""),
          fieldsData = fieldsData,
          numRows = numRows,
          schemaTimestamp = schemaTimestamp
        )
      )
      checkStatus(
        "insert",
        insertResult.status.getOrElse(
          Status(
            errorCode = ErrorCode.UnexpectedError,
            reason = "Insert Status is empty"
          )
        )
      )
    } catch {
      case e: Exception =>
        Failure(
          new Exception(s"Failed to insert data: ${e.getMessage}")
        )
    }
  }

  def importData(
      dbName: String = "",
      collectionName: String,
      partitionName: Option[String] = None,
      files: Seq[String] = Seq.empty,
      options: Map[String, String] = Map.empty,
      rowBased: Boolean = false
  ): Try[Seq[Long]] = {
    try {
      val importResult = stub.`import`(
        ImportRequest(
          dbName = dbName,
          collectionName = collectionName,
          partitionName = partitionName.getOrElse(""),
          files = files,
          options = options
            .map(kv => KeyValuePair(key = kv._1, value = kv._2))
            .toSeq,
          rowBased = rowBased
        )
      )
      val status = importResult.status.getOrElse(
        Status(
          errorCode = ErrorCode.UnexpectedError,
          reason = "Import Status is empty"
        )
      )
      if (status.errorCode == ErrorCode.Success) {
        Success(importResult.tasks.toSeq)
      } else {
        Failure(
          new Exception(
            s"Import failed with error code: ${status.errorCode}, reason: ${status.reason}"
          )
        )
      }
    } catch {
      case e: Exception =>
        Failure(
          new Exception(s"Failed to import data: ${e.getMessage}")
        )
    }
  }

  def getImportState(taskId: Long): Try[GetImportStateResponse] = {
    try {
      val importStateResult = stub.getImportState(
        GetImportStateRequest(task = taskId)
      )
      val status = importStateResult.status.getOrElse(
        Status(
          errorCode = ErrorCode.UnexpectedError,
          reason = "GetImportState Status is empty"
        )
      )
      if (status.errorCode == ErrorCode.Success) {
        Success(importStateResult)
      } else {
        Failure(
          new Exception(
            s"Get import state failed with error code: ${status.errorCode}, reason: ${status.reason}"
          )
        )
      }
    } catch {
      case e: Exception =>
        Failure(
          new Exception(s"Failed to get import state: ${e.getMessage}")
        )
    }
  }

  def delete[T](
      dbName: String = "",
      collectionName: String,
      partitionName: Option[String] = None,
      pkName: Option[String] = None,
      pks: Seq[T] = Seq.empty
  )(implicit processor: PKProcessor[T]): Try[Status] = {
    try {
      val expr: String = pkName match {
        case Some(name) => {
          s"$name in [${processor.process(pks)}]"
        }
        case None => {
          val remotePKName = getPKName(dbName, collectionName)
          val name = remotePKName
            .getOrElse(
              throw new Exception(
                s"Failed to get PK name for collection $collectionName"
              )
            )
          s"$name in [${processor.process(pks)}]"
        }
      }
      val deleteResult = stub.delete(
        DeleteRequest(
          dbName = dbName,
          collectionName = collectionName,
          partitionName = partitionName.getOrElse(""),
          expr = expr
        )
      )
      Success(
        Status(
          errorCode = ErrorCode.Success,
          reason =
            s"Mock success for deleting from collection: $collectionName with expr: $expr${partitionName
                .map(p => s" partition: $p")
                .getOrElse("")}"
        )
      )
    } catch {
      case e: Exception =>
        Failure(
          new Exception(s"Failed to delete data: ${e.getMessage}")
        )
    }
  }

  private def describeCollectionRPC(
      dbName: String,
      collectionName: String
  ): DescribeCollectionResponse = {
    return stub.describeCollection(
      DescribeCollectionRequest(
        dbName = dbName,
        collectionName = collectionName
      )
    )
  }

  def getPKName(dbName: String, collectionName: String): Try[String] = {
    try {
      val collectionInfo = describeCollectionRPC(dbName, collectionName)
      Success(
        collectionInfo.schema
          .getOrElse(
            throw new Exception(
              s"Collection schema for $collectionName not found"
            )
          )
          .fields
          .find(_.isPrimaryKey)
          .map(_.name)
          .getOrElse(
            throw new Exception(
              s"Primary key not found for collection $collectionName"
            )
          )
      )
    } catch {
      case e: Exception =>
        Failure(new Exception(s"Failed to get PK name: ${e.getMessage}"))
    }
  }

  def getCollectionSchema(
      dbName: String,
      collectionName: String
  ): Try[CollectionSchema] = {
    try {
      val collectionInfo = describeCollectionRPC(dbName, collectionName)
      Success(
        collectionInfo.schema.getOrElse(
          throw new Exception(
            s"Collection schema for $collectionName not found"
          )
        )
      )
    } catch {
      case e: Exception =>
        Failure(
          new Exception(s"Failed to get collection schema: ${e.getMessage}")
        )
    }
  }

  def getCollectionInfo(
      dbName: String,
      collectionName: String
  ): Try[MilvusCollectionInfo] = {
    try {
      val collectionInfo = describeCollectionRPC(dbName, collectionName)
      Success(
        MilvusCollectionInfo(
          dbName = dbName,
          collectionName = collectionName,
          collectionID = collectionInfo.collectionID,
          schema = collectionInfo.schema.getOrElse(
            throw new Exception(
              s"Collection schema for $collectionName not found"
            )
          )
        )
      )
    } catch {
      case e: Exception =>
        Failure(
          new Exception(s"Failed to get collection info: ${e.getMessage}")
        )
    }
  }

  def getSegments(
      dbName: String,
      collectionName: String
  ): Try[Seq[MilvusSegmentInfo]] = {
    try {
      val segments = stub.getPersistentSegmentInfo(
        GetPersistentSegmentInfoRequest(
          dbName = dbName,
          collectionName = collectionName
        )
      )
      Success(
        segments.infos.map(info =>
          MilvusSegmentInfo(
            segmentID = info.segmentID,
            collectionID = info.collectionID,
            partitionID = info.partitionID,
            numRows = info.numRows,
            state = info.state,
            level = info.level
          )
        )
      )
    } catch {
      case e: Exception =>
        Failure(
          new Exception(s"Failed to get segments: ${e.getMessage}")
        )
    }
  }

  def getSegmentInfo(
      collectionID: Long,
      segmentID: Long
  ): Try[MilvusSegmentLogInfo] = {
    try {
      val req = GetSegmentsInfoReq(
        dbName = params.databaseName,
        collectionID = collectionID,
        segmentIDs = Seq(segmentID)
      )
      val jsonString = GetSegmentsInfoReq.toJson(req)

      val request = HttpRequest
        .newBuilder()
        .uri(URI.create(params.uri + MilvusClient.segmentsUrl))
        .header("Content-Type", "application/json")
        .header(
          "Authorization",
          "Basic " + Base64.getEncoder.encodeToString(
            params.token.getBytes(StandardCharsets.UTF_8)
          )
        )
        .POST(HttpRequest.BodyPublishers.ofString(jsonString))
        .build();

      val response =
        httpClient.send(request, HttpResponse.BodyHandlers.ofString())
      if (response.statusCode() != 200) {
        return Failure(
          new Exception(s"Failed to get segment info: ${response.body()}")
        )
      }
      val responseBody = response.body()
      val responseJson = MilvusClient.mapper.readTree(responseBody)
      if (responseJson.has("code") && responseJson.get("code").asInt() != 0) {
        return Failure(
          new Exception(
            s"Failed to get segment info: ${responseJson.get("message").asText()}"
          )
        )
      }
      var insertLogIDs = Seq[String]()
      var deleteLogIDs = Seq[String]()
      responseJson
        .get("data")
        .get("segmentInfos")
        .elements()
        .asScala
        .foreach(info => {
          info
            .get("insertLogs")
            .elements()
            .asScala
            .foreach(insertLogs => {
              val fieldID = insertLogs.get("fieldID").asLong()
              insertLogs
                .get("logIDs")
                .elements()
                .asScala
                .foreach(logID => {
                  insertLogIDs = insertLogIDs :+ s"${fieldID}/${logID.asLong()}"
                })
            })
          info
            .get("deltaLogs")
            .elements()
            .asScala
            .foreach(deleteLogs => {
              deleteLogs
                .get("logIDs")
                .elements()
                .asScala
                .foreach(logID => {
                  deleteLogIDs = deleteLogIDs :+ logID.asLong().toString
                })
            })
        })
      return Success(
        MilvusSegmentLogInfo(
          segmentID = segmentID,
          insertLogIDs = insertLogIDs,
          deleteLogIDs = deleteLogIDs
        )
      )
    } catch {
      case e: Exception =>
        Failure(
          new Exception(s"Failed to get segment info: ${e.getMessage}")
        )
    }
  }

  def getPartitionID(
      dbName: String,
      collectionName: String,
      partitionName: String
  ): Try[Long] = {
    try {
      val partitionInfos = stub.showPartitions(
        ShowPartitionsRequest(
          dbName = dbName,
          collectionName = collectionName
        )
      )
      Success(
        partitionInfos.partitionNames.zipWithIndex
          .find(_._1 == partitionName)
          .map(pair => partitionInfos.partitionIDs(pair._2))
          .getOrElse(
            throw new Exception(
              s"Partition $partitionName not found in collection $collectionName"
            )
          )
      )
    } catch {
      case e: Exception =>
        Failure(
          new Exception(s"Failed to get partition ID: ${e.getMessage}")
        )
    }
  }

  def getPartitionInfos(
      dbName: String,
      collectionName: String
  ): Try[Seq[MilvusPartitionInfo]] = {
    try {
      val partitionInfos = stub.showPartitions(
        ShowPartitionsRequest(
          dbName = dbName,
          collectionName = collectionName
        )
      )
      Success(
        partitionInfos.partitionIDs.zip(partitionInfos.partitionNames).map {
          case (id, name) =>
            MilvusPartitionInfo(
              partitionID = id,
              partitionName = name
            )
        }
      )
    } catch {
      case e: Exception =>
        Failure(
          new Exception(s"Failed to get partition infos: ${e.getMessage}")
        )
    }
  }

  def close(): Unit = {
    channel.shutdownNow()
  }
}

object MilvusClient {
  val baseUrl = "/v2/vectordb"
  val segmentsUrl = s"$baseUrl/segments/describe"

  val mapper: ObjectMapper with ScalaObjectMapper = {
    val m = new ObjectMapper() with ScalaObjectMapper
    m.registerModule(DefaultScalaModule)
    m
  }

  def apply(params: MilvusConnectionParams): MilvusClient = {
    new MilvusClient(params)
  }

  def apply(options: MilvusOption): MilvusClient = {
    new MilvusClient(
      MilvusConnectionParams(
        options.uri,
        options.token,
        options.databaseName,
        options.serverPemPath,
        options.clientPemPath,
        options.clientKeyPath,
        options.caPemPath
      )
    )
  }
}

case class MilvusConnectionParams(
    uri: String,
    token: String = "",
    databaseName: String = "",
    // one tls way
    serverPemPath: String = "",
    // two tls way
    clientPemPath: String = "",
    clientKeyPath: String = "",
    caPemPath: String = ""
)

case class MilvusCollectionInfo(
    dbName: String,
    collectionName: String,
    collectionID: Long,
    schema: CollectionSchema
)

case class MilvusSegmentInfo(
    segmentID: Long,
    collectionID: Long,
    partitionID: Long,
    numRows: Long,
    state: SegmentState,
    level: SegmentLevel
)

case class MilvusSegmentLogInfo(
    segmentID: Long,
    insertLogIDs: Seq[String], // "/field_id/log_id"
    deleteLogIDs: Seq[String] // "/log_id"
)

case class MilvusPartitionInfo(
    partitionID: Long,
    partitionName: String
)

case class GetSegmentsInfoReq(
    @JsonProperty("dbName") dbName: String,
    @JsonProperty(
      "collectionID"
    ) collectionID: Long,
    @JsonProperty("segmentIDs") segmentIDs: Seq[
      Long
    ]
)

object GetSegmentsInfoReq {
  def toJson(req: GetSegmentsInfoReq): String = {
    MilvusClient.mapper.writeValueAsString(req)
  }

  def fromJson(jsonString: String): GetSegmentsInfoReq = {
    MilvusClient.mapper.readValue(jsonString, classOf[GetSegmentsInfoReq])
  }
}

trait PKProcessor[T] {
  def process(seq: Seq[T]): String
}

object PKProcessor {
  implicit object IntProcessor extends PKProcessor[Int] {
    def process(seq: Seq[Int]): String = seq.mkString(", ")
  }

  implicit object StringProcessor extends PKProcessor[String] {
    def process(seq: Seq[String]): String = seq.map(s => s"'$s'").mkString(", ")
  }
}

class GrpcRetryInterceptor(
    maxRetries: Int = 5,
    initialDelayMillis: Long = 500,
    delayMultiplier: Double = 2.0,
    maxDelayMillis: Long = 5000
) extends ClientInterceptor {

  private val nonRetryableCodes: Set[Code] = Set(
    Code.DEADLINE_EXCEEDED,
    Code.PERMISSION_DENIED,
    Code.UNAUTHENTICATED,
    Code.INVALID_ARGUMENT,
    Code.ALREADY_EXISTS,
    Code.RESOURCE_EXHAUSTED,
    Code.UNIMPLEMENTED
  )

  override def interceptCall[ReqT, RespT](
      method: MethodDescriptor[ReqT, RespT],
      callOptions: CallOptions,
      next: Channel
  ): ClientCall[ReqT, RespT] = {
    new ForwardingClientCall.SimpleForwardingClientCall[ReqT, RespT](
      next.newCall(method, callOptions)
    ) {
      override def start(
          responseListener: ClientCall.Listener[RespT],
          headers: Metadata
      ): Unit = {
        var currentAttempt = 0
        var currentDelay = initialDelayMillis

        def executeCall(): Unit = {
          currentAttempt += 1
          println(
            s"Attempting gRPC call for method ${method.getFullMethodName()}, attempt $currentAttempt"
          )

          val originalListener =
            new ForwardingClientCallListener.SimpleForwardingClientCallListener[
              RespT
            ](responseListener) {
              override def onClose(
                  status: GrpcStatus,
                  trailers: Metadata
              ): Unit = {
                if (status.isOk) {
                  // Call succeeded
                  super.onClose(status, trailers)
                } else {
                  val statusCode = status.getCode
                  if (nonRetryableCodes.contains(statusCode)) {
                    println(
                      s"gRPC call failed with non-retryable status: $statusCode. Not retrying."
                    )
                    super.onClose(status, trailers)
                  } else if (currentAttempt < maxRetries) {
                    println(
                      s"gRPC call failed with retryable status: $statusCode. Retrying in $currentDelay ms."
                    )
                    Thread.sleep(currentDelay)
                    currentDelay = Math.min(
                      (currentDelay * delayMultiplier).toLong,
                      maxDelayMillis
                    )
                    super.onClose(status, trailers)
                    executeCall()
                  } else {
                    println(
                      s"gRPC call failed after $maxRetries attempts with status: $statusCode. No more retries."
                    )
                    super.onClose(status, trailers)
                  }
                }
              }
            }
          super.start(originalListener, headers)
        }

        executeCall() // Start the first attempt
      }
    }
  }
}
