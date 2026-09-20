package com.zilliz.milvus.client.api

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

import com.zilliz.milvus.client.{
  CollectionNotFoundException,
  DatabaseNotFoundException,
  MilvusConnectionException,
  MilvusRateLimitException,
  MilvusRpcException
}
import com.zilliz.milvus.client.grpc.RpcRetry
import io.milvus.grpc.common.{
  ClientInfo,
  CompactionState,
  ConsistencyLevel,
  ErrorCode,
  IndexState,
  KeyValuePair,
  Status
}
import io.milvus.grpc.common.{LoadState, SegmentLevel, SegmentState}
import io.milvus.grpc.milvus.{
  AddCollectionFieldRequest,
  BatchUpdateManifestItem,
  BatchUpdateManifestRequest,
  ConnectRequest,
  CreateCollectionRequest,
  CreateDatabaseRequest,
  CreateIndexRequest,
  CreatePartitionRequest,
  CreateSnapshotRequest,
  DeleteRequest,
  DescribeCollectionRequest,
  DescribeCollectionResponse,
  DescribeIndexRequest,
  DescribeIndexResponse,
  DescribeSnapshotRequest,
  DescribeSnapshotResponse,
  DropCollectionRequest,
  DropIndexRequest,
  DropSnapshotRequest,
  FlushRequest,
  GetCompactionStateRequest,
  GetCompactionStateResponse,
  GetImportStateRequest,
  GetImportStateResponse,
  GetLoadStateRequest,
  GetLoadStateResponse,
  GetPersistentSegmentInfoRequest,
  GetPersistentSegmentInfoResponse,
  GetRestoreSnapshotStateRequest,
  GetRestoreSnapshotStateResponse,
  ImportRequest,
  InsertRequest,
  ListDatabasesRequest,
  ListDatabasesResponse,
  ListSnapshotsRequest,
  ListSnapshotsResponse,
  LoadCollectionRequest,
  ManualCompactionRequest,
  ManualCompactionResponse,
  MilvusServiceGrpc,
  MutationResult,
  QueryRequest,
  ReleaseCollectionRequest,
  RestoreExternalSnapshotRequest,
  RestoreExternalSnapshotResponse,
  RestoreSnapshotState,
  ShowCollectionsRequest,
  ShowCollectionsResponse,
  ShowPartitionsRequest,
  ShowPartitionsResponse
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
import io.grpc.{
  ClientInterceptor,
  Metadata,
  Status => GrpcStatus,
  StatusException,
  StatusRuntimeException
}
import io.grpc.netty.shaded.io.grpc.netty.{GrpcSslContexts, NettyChannelBuilder}
import io.grpc.stub.MetadataUtils
import io.grpc.Status.Code

/** A simplified client for interacting with Milvus
  */
class MilvusClient(params: MilvusConnectionParams)
    extends com.zilliz.milvus.storage.Logging {
  private val DefaultRpcTimeoutMillis = TimeUnit.SECONDS.toMillis(10)

  /** Reads are sent again after UNAVAILABLE or Milvus rate limiting; writes are
    * sent once (see [[read]]).
    */
  private val readRetry = RpcRetry()
  @volatile private var channelInitialized = false
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

    val interceptors = Seq(getConnectionMetadataInterceptor())
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
      // Transparent retry of a stream the server never started. No per-method
      // retry policy is configured: reads retry in `read`, writes do not.
      .enableRetry()
      .intercept(interceptors: _*)
    if (isHttps) {
      channelBuilder = channelBuilder.useTransportSecurity()
    }
    val built = channelBuilder.build()
    channelInitialized = true
    built
  }
  private lazy val stub: MilvusServiceGrpc.MilvusServiceBlockingStub = {
    val server = MilvusServiceGrpc
      .blockingStub(channel)
      .withWaitForReady()
    server
      .withDeadlineAfter(10, TimeUnit.SECONDS)
      .connect(
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

  /** The stub a write goes out on: one call, one deadline. */
  private def rpcStub: MilvusServiceGrpc.MilvusServiceBlockingStub =
    stub.withDeadlineAfter(DefaultRpcTimeoutMillis, TimeUnit.MILLISECONDS)

  /** One read-only RPC. Every attempt is a new call with the time left of
    * `timeoutMillis`, so a procedure polling with its remaining time never
    * waits past it. UNAVAILABLE and Milvus rate limiting in the response status
    * are sent again; the last answer is returned as it came.
    */
  private def read[A](timeoutMillis: Long)(
      call: MilvusServiceGrpc.MilvusServiceBlockingStub => A
  )(status: A => Option[Status]): A =
    readRetry
      .run(timeoutMillis)(left =>
        call(stub.withDeadlineAfter(left, TimeUnit.MILLISECONDS))
      ) { result =>
        RpcRetry.isUnavailable(result) ||
        result.toOption
          .flatMap(status)
          .exists(MilvusClient.isRateLimited)
      }
      .get

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
    // Success path first: avoids misclassifying success responses whose reason
    // may coincidentally contain rate-limit-like text.
    if (status.code == 0 && status.errorCode == ErrorCode.Success) {
      return Success(status)
    }
    // Failure path: classify rate limit vs other errors.
    val reason = Option(status.reason).getOrElse("")
    if (MilvusClient.isRateLimited(status)) {
      Failure(new MilvusRateLimitException(s"Failed to $api: $reason"))
    } else {
      Failure(new Exception(s"Failed to $api: $reason"))
    }
  }

  /** Lists every database visible to the configured Milvus identity. */
  def listDatabases(): Try[Seq[String]] =
    rpcCall("listDatabases")(listDatabasesRPC()).flatMap { response =>
      checkResponseStatus("listDatabases", response.status)
        .map(_ => response.dbNames)
    }

  /** Lists every collection in `dbName`.
    *
    * A confirmed missing database is kept distinct from transport,
    * authorization, and other RPC failures so the Spark catalog can expose the
    * corresponding namespace semantics without turning failures into an empty
    * listing.
    */
  def showCollections(dbName: String): Try[Seq[String]] =
    rpcCall(s"showCollections for database '$dbName'")(
      showCollectionsRPC(dbName)
    ).flatMap { response =>
      response.status match {
        case Some(status) if MilvusClient.isDatabaseNotFound(status) =>
          Failure(
            new DatabaseNotFoundException(
              s"Milvus database '$dbName' does not exist"
            )
          )
        case status =>
          checkResponseStatus(
            s"showCollections for database '$dbName'",
            status
          )
            .map(_ => response.collectionNames)
      }
    }

  private def rpcCall[A](operation: String)(call: => A): Try[A] =
    Try(call).recoverWith { case error =>
      val detail = Option(error.getMessage)
        .filter(_.nonEmpty)
        .map(message => s": $message")
        .getOrElse("")
      val wrapped = new MilvusRpcException(s"Failed to $operation$detail")
      wrapped.initCause(error)
      Failure(wrapped)
    }

  private def checkResponseStatus(
      api: String,
      status: Option[Status]
  ): Try[Status] =
    status match {
      case Some(value) => checkStatus(api, value)
      case None =>
        Failure(
          new MilvusRpcException(s"Failed to $api: response status is missing")
        )
    }

  /** Package-visible seams keep the public API testable without opening a real
    * channel or adding a runtime client abstraction.
    */
  private[api] def listDatabasesRPC(): ListDatabasesResponse =
    read(DefaultRpcTimeoutMillis)(_.listDatabases(ListDatabasesRequest()))(
      _.status
    )

  private[api] def showCollectionsRPC(
      dbName: String
  ): ShowCollectionsResponse =
    read(DefaultRpcTimeoutMillis)(
      _.showCollections(ShowCollectionsRequest(dbName = dbName))
    )(_.status)

  def createDatabase(
      dbName: String,
      properties: Map[String, String] = Map.empty
  ): Try[Status] = {
    try {
      val status = rpcStub.createDatabase(
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
      val status = rpcStub.createCollection(
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

  def dropCollection(
      dbName: String = "",
      collectionName: String
  ): Try[Status] = {
    try {
      val status = rpcStub.dropCollection(
        DropCollectionRequest(
          dbName = dbName,
          collectionName = collectionName
        )
      )
      checkStatus("dropCollection", status)
    } catch {
      case e: Exception =>
        Failure(
          new Exception(s"Failed to drop collection: ${e.getMessage}")
        )
    }
  }

  /** Registers new manifest versions of existing segments (capability A4, the
    * backfill branch): Milvus advances each segment's manifest to the version
    * given and broadcasts the change, so query nodes reload the segment. The
    * segments must exist and be flushed; Milvus does not open the manifest to
    * check it (docs/design/README.md section 5).
    */
  def batchUpdateManifest(
      dbName: String,
      collectionName: String,
      items: Seq[(Long, Long)],
      fieldNames: Seq[String] = Seq.empty
  ): Try[Status] = {
    try {
      val status = rpcStub.batchUpdateManifest(
        BatchUpdateManifestRequest(
          dbName = dbName,
          collectionName = collectionName,
          fieldNames = fieldNames,
          items = items.map { case (segmentId, version) =>
            BatchUpdateManifestItem(
              segmentId = segmentId,
              manifestVersion = version
            )
          }
        )
      )
      checkStatus("batchUpdateManifest", status)
    } catch {
      case e: Exception =>
        Failure(
          new Exception(
            s"Failed to register ${items.size} manifest version(s) of $collectionName: ${e.getMessage}",
            e
          )
        )
    }
  }

  /** Adds a field to an existing collection. Milvus requires it nullable or
    * with a default value, since existing rows have no value for it.
    */
  def addCollectionField(
      dbName: String,
      collectionName: String,
      field: FieldSchema
  ): Try[Status] = {
    try {
      val status = rpcStub.addCollectionField(
        AddCollectionFieldRequest(
          dbName = dbName,
          collectionName = collectionName,
          schema = ByteString.copyFrom(field.toByteArray)
        )
      )
      checkStatus("addCollectionField", status)
    } catch {
      case e: Exception =>
        Failure(
          new Exception(
            s"Failed to add field ${field.name} to $collectionName: ${e.getMessage}",
            e
          )
        )
    }
  }

  /** Builds an index on a field; Milvus needs one on every vector field before
    * a collection can be loaded.
    */
  def createIndex(
      dbName: String,
      collectionName: String,
      fieldName: String,
      params: Map[String, String] =
        Map("index_type" -> "AUTOINDEX", "metric_type" -> "L2")
  ): Try[Status] =
    createIndex(dbName, collectionName, fieldName, params, indexName = "")

  def createIndex(
      dbName: String,
      collectionName: String,
      fieldName: String,
      params: Map[String, String],
      indexName: String
  ): Try[Status] = {
    val request = CreateIndexRequest(
      dbName = dbName,
      collectionName = collectionName,
      fieldName = fieldName,
      extraParams = params.map { case (key, value) =>
        KeyValuePair(key, value)
      }.toSeq,
      indexName = indexName
    )
    rpcCall(s"create index '$indexName' on field '$fieldName'")(
      createIndexRPC(request)
    ).flatMap(checkStatus("createIndex", _))
  }

  /** Returns every index matching the optional field and index names. */
  def describeIndexes(
      dbName: String,
      collectionName: String,
      fieldName: String = "",
      indexName: String = ""
  ): Try[Seq[MilvusIndexInfo]] =
    describeIndexes(
      dbName,
      collectionName,
      fieldName,
      indexName,
      DefaultRpcTimeoutMillis
    )

  def describeIndexes(
      dbName: String,
      collectionName: String,
      fieldName: String,
      indexName: String,
      timeoutMillis: Long
  ): Try[Seq[MilvusIndexInfo]] = {
    val request = DescribeIndexRequest(
      dbName = dbName,
      collectionName = collectionName,
      fieldName = fieldName,
      indexName = indexName
    )
    rpcCall(s"describe indexes for collection '$collectionName'")(
      describeIndexesRPC(request, timeoutMillis)
    ).flatMap { response =>
      response.status match {
        case Some(status) if status.errorCode == ErrorCode.IndexNotExist =>
          Success(Seq.empty)
        case status =>
          checkResponseStatus("describeIndexes", status).map { _ =>
            response.indexDescriptions.map { index =>
              MilvusIndexInfo(
                indexName = index.indexName,
                indexID = index.indexID,
                params = index.params
                  .map(param => param.key -> param.value)
                  .toMap,
                fieldName = index.fieldName,
                indexedRows = index.indexedRows,
                totalRows = index.totalRows,
                state = index.state,
                failReason = index.indexStateFailReason,
                pendingIndexRows = index.pendingIndexRows,
                minIndexVersion = index.minIndexVersion,
                maxIndexVersion = index.maxIndexVersion
              )
            }
          }
      }
    }
  }

  /** Drops the named index from a collection. */
  def dropIndex(
      dbName: String,
      collectionName: String,
      indexName: String
  ): Try[Unit] = {
    val request = DropIndexRequest(
      dbName = dbName,
      collectionName = collectionName,
      indexName = indexName
    )
    rpcCall(s"drop index '$indexName' from collection '$collectionName'")(
      dropIndexRPC(request)
    ).flatMap(checkStatus("dropIndex", _)).map(_ => ())
  }

  /** Releases a loaded collection from Milvus query nodes. */
  def releaseCollection(
      dbName: String,
      collectionName: String
  ): Try[Unit] = {
    val request = ReleaseCollectionRequest(
      dbName = dbName,
      collectionName = collectionName
    )
    rpcCall(s"release collection '$collectionName'")(
      releaseCollectionRPC(request)
    ).flatMap(checkStatus("releaseCollection", _)).map(_ => ())
  }

  /** Submits a manual compaction for a collection. */
  def manualCompaction(
      dbName: String,
      collectionName: String
  ): Try[MilvusCompactionInfo] = {
    val request = ManualCompactionRequest(
      dbName = dbName,
      collectionName = collectionName
    )
    rpcCall(s"start compaction for collection '$collectionName'")(
      manualCompactionRPC(request)
    ).flatMap { response =>
      checkResponseStatus("manualCompaction", response.status).map { _ =>
        MilvusCompactionInfo(
          compactionID = response.compactionID,
          compactionPlanCount = response.compactionPlanCount
        )
      }
    }
  }

  /** Returns the current state and plan counts of a submitted compaction. */
  def getCompactionState(compactionID: Long): Try[MilvusCompactionState] =
    getCompactionState(compactionID, DefaultRpcTimeoutMillis)

  def getCompactionState(
      compactionID: Long,
      timeoutMillis: Long
  ): Try[MilvusCompactionState] = {
    val request = GetCompactionStateRequest(compactionID = compactionID)
    rpcCall(s"get state for compaction '$compactionID'")(
      getCompactionStateRPC(request, timeoutMillis)
    ).flatMap { response =>
      checkResponseStatus("getCompactionState", response.status).map { _ =>
        MilvusCompactionState(
          state = response.state,
          executingPlanCount = response.executingPlanNo,
          timeoutPlanCount = response.timeoutPlanNo,
          completedPlanCount = response.completedPlanNo,
          failedPlanCount = response.failedPlanNo
        )
      }
    }
  }

  private[api] def createIndexRPC(request: CreateIndexRequest): Status =
    rpcStub.createIndex(request)

  private[api] def describeIndexesRPC(
      request: DescribeIndexRequest,
      timeoutMillis: Long
  ): DescribeIndexResponse =
    read(timeoutMillis)(_.describeIndex(request))(_.status)

  private[api] def dropIndexRPC(request: DropIndexRequest): Status =
    rpcStub.dropIndex(request)

  private[api] def releaseCollectionRPC(
      request: ReleaseCollectionRequest
  ): Status =
    rpcStub.releaseCollection(request)

  private[api] def manualCompactionRPC(
      request: ManualCompactionRequest
  ): ManualCompactionResponse =
    rpcStub.manualCompaction(request)

  private[api] def getCompactionStateRPC(
      request: GetCompactionStateRequest,
      timeoutMillis: Long
  ): GetCompactionStateResponse =
    read(timeoutMillis)(_.getCompactionState(request))(_.status)

  def loadCollection(dbName: String, collectionName: String): Try[Status] = {
    try
      checkStatus(
        "loadCollection",
        rpcStub.loadCollection(
          LoadCollectionRequest(
            dbName = dbName,
            collectionName = collectionName
          )
        )
      )
    catch {
      case e: Exception =>
        Failure(
          new Exception(s"Failed to load $collectionName: ${e.getMessage}", e)
        )
    }
  }

  /** The collection's load state: NotExist, NotLoad, Loading or Loaded. */
  def getLoadState(dbName: String, collectionName: String): Try[LoadState] =
    getLoadState(dbName, collectionName, DefaultRpcTimeoutMillis)

  def getLoadState(
      dbName: String,
      collectionName: String,
      timeoutMillis: Long
  ): Try[LoadState] = {
    try {
      val response = getLoadStateRPC(
        GetLoadStateRequest(dbName = dbName, collectionName = collectionName),
        timeoutMillis
      )
      checkStatus(
        "getLoadState",
        response.status.getOrElse(
          Status(
            errorCode = ErrorCode.UnexpectedError,
            reason = "load state is empty"
          )
        )
      ).map(_ => response.state)
    } catch {
      case e: Exception =>
        Failure(
          new Exception(
            s"Failed to get load state of $collectionName: ${e.getMessage}",
            e
          )
        )
    }
  }

  private[api] def getLoadStateRPC(
      request: GetLoadStateRequest,
      timeoutMillis: Long
  ): GetLoadStateResponse =
    read(timeoutMillis)(_.getLoadState(request))(_.status)

  /** Scalar query, for reading rows back through the service. */
  def query(
      dbName: String,
      collectionName: String,
      expr: String,
      outputFields: Seq[String]
  ): Try[Seq[FieldData]] = {
    try {
      val request = QueryRequest(
        dbName = dbName,
        collectionName = collectionName,
        expr = expr,
        outputFields = outputFields,
        useDefaultConsistency = true
      )
      val results =
        read(DefaultRpcTimeoutMillis)(_.query(request))(_.status)
      checkStatus(
        "query",
        results.status.getOrElse(
          Status(
            errorCode = ErrorCode.UnexpectedError,
            reason = "Query status is empty"
          )
        )
      ).map(_ => results.fieldsData)
    } catch {
      case e: Exception =>
        Failure(
          new Exception(
            s"Failed to query $collectionName: ${e.getMessage}",
            e
          )
        )
    }
  }

  def flush(
      dbName: String = "",
      collectionNames: Seq[String] = Seq.empty
  ): Try[Status] = {
    try {
      val flushResponse = rpcStub.flush(
        FlushRequest(
          dbName = dbName,
          collectionNames = collectionNames
        )
      )
      checkStatus(
        "flush",
        flushResponse.status.getOrElse(
          Status(
            errorCode = ErrorCode.UnexpectedError,
            reason = "Flush Status is empty"
          )
        )
      )
    } catch {
      case e: Exception =>
        Failure(
          new Exception(s"Failed to flush collection: ${e.getMessage}", e)
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
      val insertResult = rpcStub.insert(
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
      val importResult = rpcStub.`import`(
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
      val importStateResult = read(DefaultRpcTimeoutMillis)(
        _.getImportState(GetImportStateRequest(task = taskId))
      )(_.status)
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
      val deleteResult = rpcStub.delete(
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

  private[api] def describeCollectionRPC(
      dbName: String,
      collectionName: String
  ): DescribeCollectionResponse = {
    val request = DescribeCollectionRequest(
      dbName = dbName,
      collectionName = collectionName
    )
    read(DefaultRpcTimeoutMillis)(_.describeCollection(request))(_.status)
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

  def getPkField(
      dbName: String,
      collectionName: String
  ): Try[(String, Long)] = {
    getCollectionSchema(dbName, collectionName).map { schema =>
      val pkField = schema.fields.find(_.isPrimaryKey).get
      val fieldId = if (pkField.fieldID == 0) {
        val fieldIndex = schema.fields.indexOf(pkField)
        fieldIndex + 100
      } else {
        pkField.fieldID
      }
      (pkField.name, fieldId)
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
      collectionInfo.status match {
        case Some(status) if MilvusClient.isCollectionNotFound(status) =>
          throw new CollectionNotFoundException(
            s"Milvus collection '$dbName.$collectionName' does not exist"
          )
        case status =>
          checkResponseStatus("get collection info", status).get
      }
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
      case e: CollectionNotFoundException => Failure(e)
      case e: Exception =>
        Failure(
          new Exception(
            s"Failed to get collection info: ${e.getMessage}",
            e
          )
        )
    }
  }

  /** Creates one snapshot without rolling it back if a later read fails. */
  def createSnapshot(
      dbName: String,
      collectionName: String,
      snapshotName: String,
      description: String,
      compactionProtectionSeconds: Long
  ): Try[Unit] = {
    val request = CreateSnapshotRequest(
      name = snapshotName,
      description = description,
      dbName = dbName,
      collectionName = collectionName,
      compactionProtectionSeconds = compactionProtectionSeconds
    )
    rpcCall(s"create snapshot '$snapshotName'")(createSnapshotRPC(request))
      .flatMap(checkStatus("createSnapshot", _))
      .map(_ => ())
  }

  private[api] def createSnapshotRPC(request: CreateSnapshotRequest): Status =
    rpcStub.createSnapshot(request)

  /** Lists the snapshots that belong to a collection. */
  def listSnapshots(
      dbName: String,
      collectionName: String
  ): Try[Seq[String]] = {
    val request = ListSnapshotsRequest(
      dbName = dbName,
      collectionName = collectionName
    )
    rpcCall(s"list snapshots for collection '$collectionName'")(
      listSnapshotsRPC(request)
    ).flatMap { response =>
      checkResponseStatus("listSnapshots", response.status)
        .map(_ => response.snapshots)
    }
  }

  /** Returns the metadata stored for one snapshot. */
  def describeSnapshot(
      dbName: String,
      collectionName: String,
      snapshotName: String
  ): Try[MilvusSnapshotInfo] = {
    val request = DescribeSnapshotRequest(
      name = snapshotName,
      dbName = dbName,
      collectionName = collectionName
    )
    rpcCall(s"describe snapshot '$snapshotName'")(
      describeSnapshotRPC(request)
    ).flatMap { response =>
      checkResponseStatus("describeSnapshot", response.status).map { _ =>
        MilvusSnapshotInfo(
          name = response.name,
          description = response.description,
          collectionName = response.collectionName,
          partitionNames = response.partitionNames,
          createTs = response.createTs,
          s3Location = response.s3Location
        )
      }
    }
  }

  private[api] def listSnapshotsRPC(
      request: ListSnapshotsRequest
  ): ListSnapshotsResponse =
    read(DefaultRpcTimeoutMillis)(_.listSnapshots(request))(_.status)

  private[api] def describeSnapshotRPC(
      request: DescribeSnapshotRequest
  ): DescribeSnapshotResponse =
    read(DefaultRpcTimeoutMillis)(_.describeSnapshot(request))(_.status)

  /** Asks Milvus to restore a snapshot that lives on object storage — one this
    * connector wrote, or one Milvus exported — into a collection that does not
    * exist yet, and returns the restore job Milvus opened for it. The
    * `metadataUri` names the snapshot document; `externalSpec` is the storage
    * spec JSON Milvus reads the snapshot's files with, empty for its own
    * storage.
    */
  def restoreExternalSnapshot(
      dbName: String,
      targetCollectionName: String,
      metadataUri: String,
      externalSpec: String
  ): Try[Long] = {
    val request = RestoreExternalSnapshotRequest(
      dbName = dbName,
      targetCollectionName = targetCollectionName,
      snapshotMetadataUri = metadataUri,
      externalSpec = externalSpec
    )
    rpcCall(
      s"restore external snapshot '$metadataUri' into '$targetCollectionName'"
    )(restoreExternalSnapshotRPC(request)).flatMap { response =>
      checkResponseStatus("restoreExternalSnapshot", response.status)
        .map(_ => response.jobId)
    }
  }

  private[api] def restoreExternalSnapshotRPC(
      request: RestoreExternalSnapshotRequest
  ): RestoreExternalSnapshotResponse =
    rpcStub.restoreExternalSnapshot(request)

  def getRestoreSnapshotState(jobId: Long): Try[MilvusRestoreSnapshotJob] =
    getRestoreSnapshotState(jobId, DefaultRpcTimeoutMillis)

  /** The state of one restore job, with the time left of `timeoutMillis` as the
    * call's deadline, so a procedure polling with its remaining time never
    * waits past it.
    */
  def getRestoreSnapshotState(
      jobId: Long,
      timeoutMillis: Long
  ): Try[MilvusRestoreSnapshotJob] = {
    val request = GetRestoreSnapshotStateRequest(jobId = jobId)
    rpcCall(s"get state for restore job '$jobId'")(
      getRestoreSnapshotStateRPC(request, timeoutMillis)
    ).flatMap { response =>
      checkResponseStatus("getRestoreSnapshotState", response.status)
        .flatMap { _ =>
          response.info match {
            case Some(info) =>
              Success(
                MilvusRestoreSnapshotJob(
                  jobId = info.jobId,
                  snapshotName = info.snapshotName,
                  dbName = info.dbName,
                  collectionName = info.collectionName,
                  state = info.state,
                  progress = info.progress,
                  reason = info.reason,
                  startTimeMillis = info.startTime,
                  timeCostMillis = info.timeCost
                )
              )
            case None =>
              Failure(
                new MilvusRpcException(
                  s"Failed to getRestoreSnapshotState: no job info for '$jobId'"
                )
              )
          }
        }
    }
  }

  private[api] def getRestoreSnapshotStateRPC(
      request: GetRestoreSnapshotStateRequest,
      timeoutMillis: Long
  ): GetRestoreSnapshotStateResponse =
    read(timeoutMillis)(_.getRestoreSnapshotState(request))(_.status)

  def createSnapshotForRead(
      dbName: String,
      collectionName: String,
      snapshotName: String,
      description: String,
      compactionProtectionSeconds: Long
  ): Try[MilvusSnapshotInfo] = {
    createSnapshot(
      dbName,
      collectionName,
      snapshotName,
      description,
      compactionProtectionSeconds
    ).flatMap { _ =>
      describeSnapshotWithRetry(
        dbName,
        collectionName,
        snapshotName
      ) match {
        case success @ Success(_) => success
        case Failure(e) =>
          dropSnapshot(dbName, collectionName, snapshotName) match {
            case Failure(dropErr) =>
              logWarning(
                s"Failed to drop snapshot $snapshotName after describeSnapshot failure",
                dropErr
              )
            case Success(_) =>
          }
          Failure(e)
      }
    }
  }

  /** Retries snapshot metadata reads to cover the short visibility delay that
    * can follow a successful create request.
    */
  def describeSnapshotWithRetry(
      dbName: String,
      collectionName: String,
      snapshotName: String,
      maxAttempts: Int = 3
  ): Try[MilvusSnapshotInfo] = {
    if (maxAttempts <= 0) {
      return Failure(
        new IllegalArgumentException(
          s"maxAttempts must be positive, got $maxAttempts"
        )
      )
    }
    var lastFailure = Option.empty[Throwable]
    (1 to maxAttempts).foreach { attempt =>
      val result = describeSnapshot(dbName, collectionName, snapshotName)

      result match {
        case success @ Success(_) => return success
        case Failure(e) =>
          lastFailure = Some(e)
          if (attempt < maxAttempts) {
            logWarning(
              s"describeSnapshot failed for $snapshotName (attempt $attempt/$maxAttempts)",
              e
            )
            try TimeUnit.MILLISECONDS.sleep(200L * attempt)
            catch {
              case interrupted: InterruptedException =>
                Thread.currentThread().interrupt()
                return Failure(interrupted)
            }
          }
      }
    }
    Failure(
      lastFailure.getOrElse(
        new RuntimeException(s"describeSnapshot failed for $snapshotName")
      )
    )
  }

  def dropSnapshot(
      dbName: String,
      collectionName: String,
      snapshotName: String
  ): Try[Unit] = {
    try {
      val status = rpcStub.dropSnapshot(
        DropSnapshotRequest(
          name = snapshotName,
          dbName = dbName,
          collectionName = collectionName
        )
      )
      checkStatus("dropSnapshot", status).map(_ => ())
    } catch {
      case e: StatusRuntimeException => Failure(e)
      case e: Exception              => Failure(e)
    }
  }

  def getSegments(
      dbName: String,
      collectionName: String
  ): Try[Seq[MilvusSegmentInfo]] = {
    try {
      val request = GetPersistentSegmentInfoRequest(
        dbName = dbName,
        collectionName = collectionName
      )
      val segments = getSegmentsRPC(request)
      checkResponseStatus("getSegments", segments.status).map { _ =>
        segments.infos.map(info =>
          MilvusSegmentInfo(
            segmentID = info.segmentID,
            collectionID = info.collectionID,
            partitionID = info.partitionID,
            numRows = info.numRows,
            state = info.state,
            level = info.level,
            storageVersion = info.storageVersion
          )
        )
      }
    } catch {
      case e: Exception =>
        Failure(
          new Exception(s"Failed to get segments: ${e.getMessage}", e)
        )
    }
  }

  private[api] def getSegmentsRPC(
      request: GetPersistentSegmentInfoRequest
  ): GetPersistentSegmentInfoResponse =
    read(DefaultRpcTimeoutMillis)(_.getPersistentSegmentInfo(request))(
      _.status
    )

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

  def createPartition(
      dbName: String = "",
      collectionName: String,
      partitionName: String
  ): Try[Status] = {
    try {
      val status = rpcStub.createPartition(
        CreatePartitionRequest(
          dbName = dbName,
          collectionName = collectionName,
          partitionName = partitionName
        )
      )
      checkStatus("createPartition", status)
    } catch {
      case e: Exception =>
        Failure(new Exception(s"Failed to create partition: ${e.getMessage}"))
    }
  }

  private def showPartitionsRPC(
      dbName: String,
      collectionName: String
  ): ShowPartitionsResponse = {
    val request = ShowPartitionsRequest(
      dbName = dbName,
      collectionName = collectionName
    )
    read(DefaultRpcTimeoutMillis)(_.showPartitions(request))(_.status)
  }

  def getPartitionID(
      dbName: String,
      collectionName: String,
      partitionName: String
  ): Try[Long] = {
    try {
      val partitionInfos = showPartitionsRPC(dbName, collectionName)
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
      val partitionInfos = showPartitionsRPC(dbName, collectionName)
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
    if (channelInitialized) channel.shutdownNow()
  }
}

object MilvusClient {
  val baseUrl = "/v2/vectordb"
  val segmentsUrl = s"$baseUrl/segments/describe"

  // Milvus ErrServiceRateLimit (also matches gRPC RESOURCE_EXHAUSTED numeric value).
  val RateLimitErrorCode: Int = 8

  /** Whether a response status is Milvus rate limiting. A success status never
    * is, whatever its reason text says.
    */
  private[client] def isRateLimited(status: Status): Boolean =
    !(status.code == 0 && status.errorCode == ErrorCode.Success) && (
      status.code == RateLimitErrorCode ||
        status.errorCode == ErrorCode.RateLimit ||
        Option(status.reason)
          .getOrElse("")
          .toLowerCase
          .contains(RateLimitReasonMarker)
    )
  // Case-insensitive reason marker used as a fallback when error code is not set.
  val RateLimitReasonMarker: String = "rate limit exceeded"
  val ServiceNotImplementedMarker: String = "service not implemented"
  private[client] val CollectionNotFoundCode: Int = 100
  private[client] val DatabaseNotFoundCode: Int = 800

  private val SnapshotRpcNames: Set[String] = Set(
    "createsnapshot",
    "describesnapshot",
    "dropsnapshot",
    "restoreexternalsnapshot",
    "getrestoresnapshotstate"
  )

  private val ServiceUnavailableMarkers: Set[String] = Set(
    ServiceNotImplementedMarker,
    "unknown method",
    "method not registered"
  )

  private def grpcStatus(t: Throwable): Option[GrpcStatus] = t match {
    case e: StatusRuntimeException => Some(e.getStatus)
    case e: StatusException        => Some(e.getStatus)
    case _                         => None
  }

  private def isSnapshotRpcUnavailable(description: String): Boolean = {
    val normalized = description.toLowerCase
    SnapshotRpcNames.exists(normalized.contains) &&
    ServiceUnavailableMarkers.exists(normalized.contains)
  }

  private def hasGrpcCode(
      t: Throwable,
      codes: Set[GrpcStatus.Code]
  ): Boolean = {
    val visited = java.util.Collections.newSetFromMap(
      new java.util.IdentityHashMap[Throwable, java.lang.Boolean]()
    )
    var current = t
    while (current != null && visited.add(current)) {
      grpcStatus(current).foreach { status =>
        if (codes.contains(status.getCode)) return true
      }
      current = current.getCause
    }
    false
  }

  private[client] def isCollectionNotFound(status: Status): Boolean =
    (status.errorCode == ErrorCode.CollectionNotExists ||
      status.errorCode == ErrorCode.CollectionNameNotFound) ||
      status.code == CollectionNotFoundCode ||
      status.code == DatabaseNotFoundCode

  private[client] def isDatabaseNotFound(status: Status): Boolean =
    status.code == DatabaseNotFoundCode

  def isServiceNotImplemented(t: Throwable): Boolean = {
    val visited = java.util.Collections.newSetFromMap(
      new java.util.IdentityHashMap[Throwable, java.lang.Boolean]()
    )
    var current = t
    while (current != null && visited.add(current)) {
      grpcStatus(current).foreach { status =>
        if (status.getCode == GrpcStatus.Code.UNIMPLEMENTED) return true
        if (status.getCode == GrpcStatus.Code.UNKNOWN) {
          val description = Option(status.getDescription).getOrElse("")
          if (isSnapshotRpcUnavailable(description)) return true
        }
      }
      current = current.getCause
    }
    false
  }

  def isSnapshotAlreadyDropped(t: Throwable): Boolean =
    hasGrpcCode(t, Set(GrpcStatus.Code.NOT_FOUND))

  def isTerminalSnapshotDropError(t: Throwable): Boolean =
    hasGrpcCode(
      t,
      Set(
        GrpcStatus.Code.NOT_FOUND,
        GrpcStatus.Code.PERMISSION_DENIED,
        GrpcStatus.Code.INVALID_ARGUMENT
      )
    )

  val mapper: ObjectMapper with ScalaObjectMapper = {
    val m = new ObjectMapper() with ScalaObjectMapper
    m.registerModule(DefaultScalaModule)
    m
  }

  def apply(params: MilvusConnectionParams): MilvusClient = {
    new MilvusClient(params)
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

case class MilvusSnapshotInfo(
    name: String,
    description: String,
    collectionName: String,
    partitionNames: Seq[String],
    createTs: Long,
    s3Location: String
)

/** One restore job as `GetRestoreSnapshotState` reports it. `state` is Milvus's
  * own enum; `progress` is a percentage; `reason` is set when it failed. Times
  * are milliseconds.
  */
case class MilvusRestoreSnapshotJob(
    jobId: Long,
    snapshotName: String,
    dbName: String,
    collectionName: String,
    state: RestoreSnapshotState,
    progress: Int,
    reason: String,
    startTimeMillis: Long,
    timeCostMillis: Long
)

case class MilvusIndexInfo(
    indexName: String,
    indexID: Long,
    params: Map[String, String],
    fieldName: String,
    indexedRows: Long,
    totalRows: Long,
    state: IndexState,
    failReason: String,
    pendingIndexRows: Long,
    minIndexVersion: Int,
    maxIndexVersion: Int
)

case class MilvusCompactionInfo(
    compactionID: Long,
    compactionPlanCount: Int
)

case class MilvusCompactionState(
    state: CompactionState,
    executingPlanCount: Long,
    timeoutPlanCount: Long,
    completedPlanCount: Long,
    failedPlanCount: Long
)

case class MilvusSegmentInfo(
    segmentID: Long,
    collectionID: Long,
    partitionID: Long,
    numRows: Long,
    state: SegmentState,
    level: SegmentLevel,
    storageVersion: Long = 1L
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
