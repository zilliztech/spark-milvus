package com.zilliz.milvus.client.api

import scala.util.{Failure, Success, Try}

import org.scalatest.funsuite.AnyFunSuite

import com.zilliz.milvus.client.MilvusRpcException
import io.milvus.grpc.common.{
  CompactionState,
  ErrorCode,
  IndexState,
  KeyValuePair,
  LoadState,
  Status
}
import io.milvus.grpc.milvus.{
  CreateIndexRequest,
  CreateSnapshotRequest,
  DescribeCollectionResponse,
  DescribeIndexRequest,
  DescribeIndexResponse,
  DescribeSnapshotRequest,
  DescribeSnapshotResponse,
  DropIndexRequest,
  GetCompactionStateRequest,
  GetCompactionStateResponse,
  GetLoadStateRequest,
  GetLoadStateResponse,
  GetPersistentSegmentInfoRequest,
  GetPersistentSegmentInfoResponse,
  IndexDescription,
  ListSnapshotsRequest,
  ListSnapshotsResponse,
  ManualCompactionRequest,
  ManualCompactionResponse,
  ReleaseCollectionRequest
}

import io.grpc.{Status => GrpcStatus, StatusRuntimeException}

class MilvusClientManagementRpcTest extends AnyFunSuite {

  private val connection = MilvusConnectionParams(
    uri = "http://localhost:19530",
    databaseName = "default"
  )

  private val ok = Status(code = 0, errorCode = ErrorCode.Success)
  private val denied = Status(
    errorCode = ErrorCode.PermissionDenied,
    reason = "permission denied"
  )

  test("closing an unused client does not initialize its channel") {
    new MilvusClient(MilvusConnectionParams(uri = "not a valid URI")).close()
  }

  test("createIndex sends index name and parameters") {
    var captured = Option.empty[CreateIndexRequest]
    val client = stubClient(
      createIndex = request => {
        captured = Some(request)
        ok
      }
    )

    val result = client.createIndex(
      "analytics",
      "events",
      "embedding",
      params = Map("index_type" -> "HNSW", "metric_type" -> "COSINE"),
      indexName = "embedding_hnsw"
    )

    assert(result == Success(ok))
    assert(captured.exists(_.dbName == "analytics"))
    assert(captured.exists(_.collectionName == "events"))
    assert(captured.exists(_.fieldName == "embedding"))
    assert(captured.exists(_.indexName == "embedding_hnsw"))
    assert(
      captured.toSeq
        .flatMap(_.extraParams)
        .map(param => param.key -> param.value)
        .toMap == Map("index_type" -> "HNSW", "metric_type" -> "COSINE")
    )
  }

  test("createIndex rejects failure status and wraps transport failures") {
    val rejected = stubClient(createIndex = _ => denied)
    assert(rejected.createIndex("db", "collection", "field").isFailure)

    val transportFailure = unavailable("create index failed")
    val unavailableClient = stubClient(
      createIndex = _ => throw transportFailure
    )
    assertTransportFailure(
      unavailableClient.createIndex("db", "collection", "field"),
      "create index",
      transportFailure
    )
  }

  test("createSnapshot sends metadata and preserves RPC failures") {
    var captured = Option.empty[CreateSnapshotRequest]
    val client = stubClient(
      createSnapshot = request => {
        captured = Some(request)
        ok
      }
    )

    assert(
      client.createSnapshot("analytics", "events", "nightly", "daily", 60L) ==
        Success(())
    )
    assert(captured.exists(_.dbName == "analytics"))
    assert(captured.exists(_.collectionName == "events"))
    assert(captured.exists(_.name == "nightly"))
    assert(captured.exists(_.description == "daily"))
    assert(captured.exists(_.compactionProtectionSeconds == 60L))

    val rejected = stubClient(createSnapshot = _ => denied)
    assert(
      rejected
        .createSnapshot("db", "collection", "snapshot", "", 0L)
        .isFailure
    )

    val failure = unavailable("create snapshot failed")
    val unavailableClient = stubClient(createSnapshot = _ => throw failure)
    assertTransportFailure(
      unavailableClient.createSnapshot("db", "collection", "snapshot", "", 0L),
      "create snapshot",
      failure
    )
  }

  test("listSnapshots sends its scope and preserves successful empty lists") {
    var captured = Option.empty[ListSnapshotsRequest]
    val populated = stubClient(
      listSnapshots = request => {
        captured = Some(request)
        ListSnapshotsResponse(
          status = Some(ok),
          snapshots = Seq("nightly", "weekly")
        )
      }
    )

    assert(
      populated.listSnapshots("analytics", "events") ==
        Success(Seq("nightly", "weekly"))
    )
    assert(captured.exists(_.dbName == "analytics"))
    assert(captured.exists(_.collectionName == "events"))

    val empty = stubClient(
      listSnapshots = _ => ListSnapshotsResponse(status = Some(ok))
    )
    assert(empty.listSnapshots("analytics", "events") == Success(Seq.empty))
  }

  test("listSnapshots rejects missing and failure statuses") {
    val missing = stubClient(
      listSnapshots = _ => ListSnapshotsResponse(snapshots = Seq("untrusted"))
    )
    assertMissingStatus(missing.listSnapshots("db", "collection"))

    val rejected = stubClient(
      listSnapshots = _ =>
        ListSnapshotsResponse(
          status = Some(denied),
          snapshots = Seq("untrusted")
        )
    )
    assert(rejected.listSnapshots("db", "collection").isFailure)
  }

  test("listSnapshots wraps transport failures with their cause") {
    val failure = unavailable("list failed")
    val client = stubClient(listSnapshots = _ => throw failure)

    assertTransportFailure(
      client.listSnapshots("db", "collection"),
      "list snapshots",
      failure
    )
  }

  test("describeSnapshot maps metadata and sends the snapshot identity") {
    var captured = Option.empty[DescribeSnapshotRequest]
    val client = stubClient(
      describeSnapshot = request => {
        captured = Some(request)
        DescribeSnapshotResponse(
          status = Some(ok),
          name = "nightly",
          description = "nightly backup",
          collectionName = "events",
          partitionNames = Seq("p0", "p1"),
          createTs = 42L,
          s3Location = "s3://snapshots/nightly"
        )
      }
    )

    assert(
      client.describeSnapshot("analytics", "events", "nightly") == Success(
        MilvusSnapshotInfo(
          name = "nightly",
          description = "nightly backup",
          collectionName = "events",
          partitionNames = Seq("p0", "p1"),
          createTs = 42L,
          s3Location = "s3://snapshots/nightly"
        )
      )
    )
    assert(captured.exists(_.dbName == "analytics"))
    assert(captured.exists(_.collectionName == "events"))
    assert(captured.exists(_.name == "nightly"))
  }

  test("describeSnapshotWithRetry covers post-create visibility delay") {
    var attempts = 0
    val client = stubClient(
      describeSnapshot = _ => {
        attempts += 1
        if (attempts == 1)
          DescribeSnapshotResponse(status = Some(denied))
        else
          DescribeSnapshotResponse(
            status = Some(ok),
            name = "nightly",
            collectionName = "events"
          )
      }
    )

    assert(
      client
        .describeSnapshotWithRetry("analytics", "events", "nightly", 2)
        .map(_.name) == Success("nightly")
    )
    assert(attempts == 2)
  }

  test(
    "describeSnapshot rejects missing/failure statuses and transport errors"
  ) {
    val missing = stubClient(
      describeSnapshot = _ => DescribeSnapshotResponse(name = "untrusted")
    )
    assertMissingStatus(missing.describeSnapshot("db", "collection", "s"))

    val rejected = stubClient(
      describeSnapshot = _ => DescribeSnapshotResponse(status = Some(denied))
    )
    assert(rejected.describeSnapshot("db", "collection", "s").isFailure)

    val failure = unavailable("describe failed")
    val unavailableClient = stubClient(describeSnapshot = _ => throw failure)
    assertTransportFailure(
      unavailableClient.describeSnapshot("db", "collection", "s"),
      "describe snapshot",
      failure
    )
  }

  test("describeIndexes maps index details and request filters") {
    var captured = Option.empty[DescribeIndexRequest]
    val client = stubClient(
      describeIndexes = (request, _) => {
        captured = Some(request)
        DescribeIndexResponse(
          status = Some(ok),
          indexDescriptions = Seq(
            IndexDescription(
              indexName = "embedding_hnsw",
              indexID = 101L,
              params = Seq(
                KeyValuePair("index_type", "HNSW"),
                KeyValuePair("metric_type", "COSINE")
              ),
              fieldName = "embedding",
              indexedRows = 90L,
              totalRows = 100L,
              state = IndexState.InProgress,
              indexStateFailReason = "",
              pendingIndexRows = 10L,
              minIndexVersion = 2,
              maxIndexVersion = 3
            )
          )
        )
      }
    )

    val result = client.describeIndexes(
      "analytics",
      "events",
      fieldName = "embedding",
      indexName = "embedding_hnsw"
    )

    assert(
      result == Success(
        Seq(
          MilvusIndexInfo(
            indexName = "embedding_hnsw",
            indexID = 101L,
            params = Map("index_type" -> "HNSW", "metric_type" -> "COSINE"),
            fieldName = "embedding",
            indexedRows = 90L,
            totalRows = 100L,
            state = IndexState.InProgress,
            failReason = "",
            pendingIndexRows = 10L,
            minIndexVersion = 2,
            maxIndexVersion = 3
          )
        )
      )
    )
    assert(captured.exists(_.dbName == "analytics"))
    assert(captured.exists(_.collectionName == "events"))
    assert(captured.exists(_.fieldName == "embedding"))
    assert(captured.exists(_.indexName == "embedding_hnsw"))
  }

  test(
    "describeIndexes preserves empty success and treats IndexNotExist as empty"
  ) {
    val empty = stubClient(
      describeIndexes = (_, _) => DescribeIndexResponse(status = Some(ok))
    )
    assert(empty.describeIndexes("db", "collection") == Success(Seq.empty))

    val absent = stubClient(
      describeIndexes = (_, _) =>
        DescribeIndexResponse(
          status = Some(
            Status(
              errorCode = ErrorCode.IndexNotExist,
              reason = "index not found"
            )
          )
        )
    )
    assert(absent.describeIndexes("db", "collection") == Success(Seq.empty))
  }

  test("describeIndexes rejects missing and unrelated failure statuses") {
    val missing = stubClient(
      describeIndexes = (_, _) =>
        DescribeIndexResponse(
          indexDescriptions = Seq(IndexDescription(indexName = "untrusted"))
        )
    )
    assertMissingStatus(missing.describeIndexes("db", "collection"))

    val rejected = stubClient(
      describeIndexes = (_, _) =>
        DescribeIndexResponse(
          status = Some(denied),
          indexDescriptions = Seq(IndexDescription(indexName = "untrusted"))
        )
    )
    assert(rejected.describeIndexes("db", "collection").isFailure)
  }

  test("describeIndexes wraps transport failures") {
    val failure = unavailable("describe indexes failed")
    val client = stubClient(describeIndexes = (_, _) => throw failure)

    assertTransportFailure(
      client.describeIndexes("db", "collection"),
      "describe indexes",
      failure
    )
  }

  test("dropIndex and releaseCollection send requests and accept success") {
    var dropped = Option.empty[DropIndexRequest]
    var released = Option.empty[ReleaseCollectionRequest]
    val client = stubClient(
      dropIndex = request => {
        dropped = Some(request)
        ok
      },
      releaseCollection = request => {
        released = Some(request)
        ok
      }
    )

    assert(
      client.dropIndex("analytics", "events", "embedding_hnsw") == Success(())
    )
    assert(client.releaseCollection("analytics", "events") == Success(()))
    assert(dropped.exists(_.dbName == "analytics"))
    assert(dropped.exists(_.collectionName == "events"))
    assert(dropped.exists(_.indexName == "embedding_hnsw"))
    assert(released.exists(_.dbName == "analytics"))
    assert(released.exists(_.collectionName == "events"))
  }

  test("dropIndex and releaseCollection reject statuses and wrap exceptions") {
    val rejected = stubClient(
      dropIndex = _ => denied,
      releaseCollection = _ => denied
    )
    assert(rejected.dropIndex("db", "collection", "index").isFailure)
    assert(rejected.releaseCollection("db", "collection").isFailure)

    val dropFailure = unavailable("drop failed")
    val releaseFailure = unavailable("release failed")
    val unavailableClient = stubClient(
      dropIndex = _ => throw dropFailure,
      releaseCollection = _ => throw releaseFailure
    )
    assertTransportFailure(
      unavailableClient.dropIndex("db", "collection", "index"),
      "drop index",
      dropFailure
    )
    assertTransportFailure(
      unavailableClient.releaseCollection("db", "collection"),
      "release collection",
      releaseFailure
    )
  }

  test("manualCompaction maps identifiers and sends collection scope") {
    var captured = Option.empty[ManualCompactionRequest]
    val client = stubClient(
      manualCompaction = request => {
        captured = Some(request)
        ManualCompactionResponse(
          status = Some(ok),
          compactionID = 88L,
          compactionPlanCount = 3
        )
      }
    )

    assert(
      client.manualCompaction("analytics", "events") ==
        Success(MilvusCompactionInfo(88L, 3))
    )
    assert(captured.exists(_.dbName == "analytics"))
    assert(captured.exists(_.collectionName == "events"))
  }

  test(
    "manualCompaction rejects missing/failure statuses and transport errors"
  ) {
    val missing = stubClient(
      manualCompaction = _ => ManualCompactionResponse(compactionID = 88L)
    )
    assertMissingStatus(missing.manualCompaction("db", "collection"))

    val rejected = stubClient(
      manualCompaction = _ => ManualCompactionResponse(status = Some(denied))
    )
    assert(rejected.manualCompaction("db", "collection").isFailure)

    val failure = unavailable("compaction failed")
    val unavailableClient = stubClient(manualCompaction = _ => throw failure)
    assertTransportFailure(
      unavailableClient.manualCompaction("db", "collection"),
      "start compaction",
      failure
    )
  }

  test("getCompactionState maps state and plan counts") {
    var captured = Option.empty[GetCompactionStateRequest]
    val client = stubClient(
      getCompactionState = (request, _) => {
        captured = Some(request)
        GetCompactionStateResponse(
          status = Some(ok),
          state = CompactionState.Completed,
          executingPlanNo = 1L,
          timeoutPlanNo = 2L,
          completedPlanNo = 3L,
          failedPlanNo = 4L
        )
      }
    )

    assert(
      client.getCompactionState(88L) == Success(
        MilvusCompactionState(
          state = CompactionState.Completed,
          executingPlanCount = 1L,
          timeoutPlanCount = 2L,
          completedPlanCount = 3L,
          failedPlanCount = 4L
        )
      )
    )
    assert(captured.exists(_.compactionID == 88L))
  }

  test(
    "getCompactionState rejects missing/failure statuses and transport errors"
  ) {
    val missing = stubClient(
      getCompactionState =
        (_, _) => GetCompactionStateResponse(state = CompactionState.Executing)
    )
    assertMissingStatus(missing.getCompactionState(88L))

    val rejected = stubClient(
      getCompactionState =
        (_, _) => GetCompactionStateResponse(status = Some(denied))
    )
    assert(rejected.getCompactionState(88L).isFailure)

    val failure = unavailable("state failed")
    val unavailableClient =
      stubClient(getCompactionState = (_, _) => throw failure)
    assertTransportFailure(
      unavailableClient.getCompactionState(88L),
      "get state for compaction",
      failure
    )
  }

  test("polling RPCs forward default and explicit deadline budgets") {
    var indexTimeouts = Vector.empty[Long]
    var loadTimeouts = Vector.empty[Long]
    var compactionTimeouts = Vector.empty[Long]
    val client = stubClient(
      describeIndexes = (_, timeoutMillis) => {
        indexTimeouts :+= timeoutMillis
        DescribeIndexResponse(status = Some(ok))
      },
      getLoadState = (_, timeoutMillis) => {
        loadTimeouts :+= timeoutMillis
        GetLoadStateResponse(
          status = Some(ok),
          state = LoadState.LoadStateLoaded
        )
      },
      getCompactionState = (_, timeoutMillis) => {
        compactionTimeouts :+= timeoutMillis
        GetCompactionStateResponse(
          status = Some(ok),
          state = CompactionState.Completed
        )
      }
    )

    assert(client.describeIndexes("db", "collection") == Success(Seq.empty))
    assert(
      client.describeIndexes("db", "collection", "", "", 123L) ==
        Success(Seq.empty)
    )
    assert(
      client.getLoadState("db", "collection") ==
        Success(LoadState.LoadStateLoaded)
    )
    assert(
      client.getLoadState("db", "collection", 234L) ==
        Success(LoadState.LoadStateLoaded)
    )
    assert(client.getCompactionState(88L).isSuccess)
    assert(client.getCompactionState(88L, 345L).isSuccess)

    assert(indexTimeouts == Seq(10000L, 123L))
    assert(loadTimeouts == Seq(10000L, 234L))
    assert(compactionTimeouts == Seq(10000L, 345L))
  }

  test("flush and getSegments retain wrapped causes") {
    val segmentFailure = unavailable("segments failed")
    val client = stubClient(getSegments = _ => throw segmentFailure)
    client.getSegments("db", "collection") match {
      case Failure(error) => assert(error.getCause eq segmentFailure)
      case other          => fail(s"expected segment failure, got $other")
    }

    val invalid = new MilvusClient(connection.copy(uri = "not a valid URI"))
    invalid.flush() match {
      case Failure(error) =>
        assert(error.getCause.isInstanceOf[java.net.URISyntaxException])
      case other => fail(s"expected flush failure, got $other")
    }
  }

  test("describe inputs reject missing and failed response statuses") {
    val missing = stubClient(
      describeCollection = (_, _) => DescribeCollectionResponse(),
      getSegments = _ => GetPersistentSegmentInfoResponse()
    )
    assert(missing.getCollectionInfo("db", "collection").isFailure)
    assertMissingStatus(missing.getSegments("db", "collection"))

    val rejected = stubClient(
      describeCollection =
        (_, _) => DescribeCollectionResponse(status = Some(denied)),
      getSegments = _ => GetPersistentSegmentInfoResponse(status = Some(denied))
    )
    assert(rejected.getCollectionInfo("db", "collection").isFailure)
    assert(rejected.getSegments("db", "collection").isFailure)
  }

  private def assertMissingStatus(result: Try[_]): Unit =
    result match {
      case Failure(error: MilvusRpcException) =>
        assert(error.getMessage.contains("response status is missing"))
      case other => fail(s"expected missing-status failure, got $other")
    }

  private def assertTransportFailure(
      result: Try[_],
      operation: String,
      cause: Throwable
  ): Unit =
    result match {
      case Failure(error: MilvusRpcException) =>
        assert(error.getMessage.contains(operation))
        assert(error.getCause eq cause)
      case other => fail(s"expected transport failure, got $other")
    }

  private def unavailable(message: String): StatusRuntimeException =
    new StatusRuntimeException(
      GrpcStatus.UNAVAILABLE.withDescription(message)
    )

  private def stubClient(
      createIndex: CreateIndexRequest => Status = _ =>
        unexpected("createIndex"),
      createSnapshot: CreateSnapshotRequest => Status = _ =>
        unexpected("createSnapshot"),
      describeIndexes: (DescribeIndexRequest, Long) => DescribeIndexResponse =
        (_, _) => unexpected("describeIndexes"),
      dropIndex: DropIndexRequest => Status = _ => unexpected("dropIndex"),
      releaseCollection: ReleaseCollectionRequest => Status = _ =>
        unexpected("releaseCollection"),
      manualCompaction: ManualCompactionRequest => ManualCompactionResponse =
        _ => unexpected("manualCompaction"),
      getCompactionState: (
          GetCompactionStateRequest,
          Long
      ) => GetCompactionStateResponse = (_, _) =>
        unexpected("getCompactionState"),
      getLoadState: (GetLoadStateRequest, Long) => GetLoadStateResponse =
        (_, _) => unexpected("getLoadState"),
      listSnapshots: ListSnapshotsRequest => ListSnapshotsResponse = _ =>
        unexpected("listSnapshots"),
      describeSnapshot: DescribeSnapshotRequest => DescribeSnapshotResponse =
        _ => unexpected("describeSnapshot"),
      describeCollection: (String, String) => DescribeCollectionResponse =
        (_, _) => unexpected("describeCollection"),
      getSegments: GetPersistentSegmentInfoRequest => GetPersistentSegmentInfoResponse =
        _ => unexpected("getSegments")
  ): MilvusClient = {
    val invokeCreateIndex = createIndex
    val invokeCreateSnapshot = createSnapshot
    val invokeDescribeIndexes = describeIndexes
    val invokeDropIndex = dropIndex
    val invokeReleaseCollection = releaseCollection
    val invokeManualCompaction = manualCompaction
    val invokeGetCompactionState = getCompactionState
    val invokeGetLoadState = getLoadState
    val invokeListSnapshots = listSnapshots
    val invokeDescribeSnapshot = describeSnapshot
    val invokeDescribeCollection = describeCollection
    val invokeGetSegments = getSegments

    new MilvusClient(connection) {
      override private[api] def createIndexRPC(
          request: CreateIndexRequest
      ): Status = invokeCreateIndex(request)

      override private[api] def createSnapshotRPC(
          request: CreateSnapshotRequest
      ): Status = invokeCreateSnapshot(request)

      override private[api] def describeIndexesRPC(
          request: DescribeIndexRequest,
          timeoutMillis: Long
      ): DescribeIndexResponse =
        invokeDescribeIndexes(request, timeoutMillis)

      override private[api] def dropIndexRPC(
          request: DropIndexRequest
      ): Status = invokeDropIndex(request)

      override private[api] def releaseCollectionRPC(
          request: ReleaseCollectionRequest
      ): Status = invokeReleaseCollection(request)

      override private[api] def manualCompactionRPC(
          request: ManualCompactionRequest
      ): ManualCompactionResponse = invokeManualCompaction(request)

      override private[api] def getCompactionStateRPC(
          request: GetCompactionStateRequest,
          timeoutMillis: Long
      ): GetCompactionStateResponse =
        invokeGetCompactionState(request, timeoutMillis)

      override private[api] def getLoadStateRPC(
          request: GetLoadStateRequest,
          timeoutMillis: Long
      ): GetLoadStateResponse = invokeGetLoadState(request, timeoutMillis)

      override private[api] def listSnapshotsRPC(
          request: ListSnapshotsRequest
      ): ListSnapshotsResponse = invokeListSnapshots(request)

      override private[api] def describeSnapshotRPC(
          request: DescribeSnapshotRequest
      ): DescribeSnapshotResponse = invokeDescribeSnapshot(request)

      override private[api] def describeCollectionRPC(
          dbName: String,
          collectionName: String
      ): DescribeCollectionResponse =
        invokeDescribeCollection(dbName, collectionName)

      override private[api] def getSegmentsRPC(
          request: GetPersistentSegmentInfoRequest
      ): GetPersistentSegmentInfoResponse = invokeGetSegments(request)
    }
  }

  private def unexpected(operation: String): Nothing =
    throw new IllegalStateException(s"unexpected $operation call")
}
