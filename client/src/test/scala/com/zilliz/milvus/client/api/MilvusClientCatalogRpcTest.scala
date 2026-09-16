package com.zilliz.milvus.client.api

import scala.util.{Failure, Success}

import org.scalatest.funsuite.AnyFunSuite

import com.zilliz.milvus.client.{DatabaseNotFoundException, MilvusRpcException}
import io.milvus.grpc.common.{ErrorCode, Status}
import io.milvus.grpc.milvus.{ListDatabasesResponse, ShowCollectionsResponse}

import io.grpc.{Status => GrpcStatus, StatusRuntimeException}

class MilvusClientCatalogRpcTest extends AnyFunSuite {

  private val connection = MilvusConnectionParams(
    uri = "http://localhost:19530",
    token = "",
    databaseName = "default"
  )

  private val ok = Status(code = 0, errorCode = ErrorCode.Success)

  test("listDatabases returns names from a successful response") {
    val client = stubClient(
      list = () =>
        ListDatabasesResponse(
          status = Some(ok),
          dbNames = Seq("default", "analytics")
        )
    )

    assert(client.listDatabases() == Success(Seq("default", "analytics")))
  }

  test("listDatabases accepts a successful empty listing") {
    val client = stubClient(
      list = () => ListDatabasesResponse(status = Some(ok))
    )

    assert(client.listDatabases() == Success(Seq.empty))
  }

  test("listDatabases rejects a response without status") {
    val client = stubClient(
      list = () => ListDatabasesResponse(dbNames = Seq("untrusted"))
    )

    client.listDatabases() match {
      case Failure(_: MilvusRpcException) => succeed
      case other => fail(s"expected MilvusRpcException, got $other")
    }
  }

  test("showCollections sends the database and returns collection names") {
    var requestedDatabase = Option.empty[String]
    val client = stubClient(
      show = dbName => {
        requestedDatabase = Some(dbName)
        ShowCollectionsResponse(
          status = Some(ok),
          collectionNames = Seq("events", "users")
        )
      }
    )

    assert(
      client.showCollections("analytics") == Success(Seq("events", "users"))
    )
    assert(requestedDatabase.contains("analytics"))
  }

  test("showCollections accepts a successful empty listing") {
    val client = stubClient(
      show = _ => ShowCollectionsResponse(status = Some(ok))
    )

    assert(client.showCollections("empty") == Success(Seq.empty))
  }

  test("showCollections rejects a response without status") {
    val client = stubClient(
      show = _ => ShowCollectionsResponse(collectionNames = Seq("untrusted"))
    )

    client.showCollections("analytics") match {
      case Failure(_: MilvusRpcException) => succeed
      case other => fail(s"expected MilvusRpcException, got $other")
    }
  }

  test("showCollections classifies only database-not-found status") {
    val missingDatabase = stubClient(
      show = _ =>
        ShowCollectionsResponse(
          status = Some(
            Status(
              code = MilvusClient.DatabaseNotFoundCode,
              errorCode = ErrorCode.UnexpectedError,
              reason = "database not found"
            )
          )
        )
    )
    val missingCollection = stubClient(
      show = _ =>
        ShowCollectionsResponse(
          status = Some(
            Status(
              code = MilvusClient.CollectionNotFoundCode,
              errorCode = ErrorCode.UnexpectedError,
              reason = "collection not found"
            )
          )
        )
    )

    missingDatabase.showCollections("missing") match {
      case Failure(_: DatabaseNotFoundException) => succeed
      case other => fail(s"expected DatabaseNotFoundException, got $other")
    }
    missingCollection.showCollections("existing") match {
      case Failure(_: DatabaseNotFoundException) =>
        fail(
          "collection-not-found must not be classified as a missing database"
        )
      case Failure(_)     => succeed
      case Success(value) => fail(s"expected failure, got $value")
    }
  }

  test("catalog RPCs reject non-success statuses without trusting names") {
    val denied = Status(
      code = 0,
      errorCode = ErrorCode.PermissionDenied,
      reason = "permission denied"
    )
    val client = stubClient(
      list = () =>
        ListDatabasesResponse(
          status = Some(denied),
          dbNames = Seq("untrusted")
        ),
      show = _ =>
        ShowCollectionsResponse(
          status = Some(denied),
          collectionNames = Seq("untrusted")
        )
    )

    assert(client.listDatabases().isFailure)
    val showFailure = client.showCollections("analytics").failed.get
    assert(showFailure.getMessage.contains("showCollections"))
    assert(showFailure.getMessage.contains("analytics"))
  }

  test("catalog RPC transport failures retain their cause and operation") {
    val listFailure = new StatusRuntimeException(
      GrpcStatus.UNAVAILABLE.withDescription("connection failed")
    )
    val showFailure = new StatusRuntimeException(
      GrpcStatus.DEADLINE_EXCEEDED.withDescription("deadline exceeded")
    )
    val client = stubClient(
      list = () => throw listFailure,
      show = _ => throw showFailure
    )

    val listError = client.listDatabases().failed.get
    assert(listError.isInstanceOf[MilvusRpcException])
    assert(listError.getMessage.contains("listDatabases"))
    assert(listError.getCause eq listFailure)

    val showError = client.showCollections("analytics").failed.get
    assert(showError.isInstanceOf[MilvusRpcException])
    assert(showError.getMessage.contains("showCollections"))
    assert(showError.getMessage.contains("analytics"))
    assert(showError.getCause eq showFailure)
  }

  private def stubClient(
      list: () => ListDatabasesResponse = () =>
        throw new IllegalStateException("unexpected listDatabases call"),
      show: String => ShowCollectionsResponse = _ =>
        throw new IllegalStateException("unexpected showCollections call")
  ): MilvusClient =
    new MilvusClient(connection) {
      override private[api] def listDatabasesRPC(): ListDatabasesResponse =
        list()

      override private[api] def showCollectionsRPC(
          dbName: String
      ): ShowCollectionsResponse =
        show(dbName)
    }
}
