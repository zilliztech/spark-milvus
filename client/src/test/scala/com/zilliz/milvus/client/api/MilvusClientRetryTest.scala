package com.zilliz.milvus.client.api

import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicInteger
import scala.util.{Failure, Success}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.milvus.grpc.common.{ErrorCode, Status}
import io.milvus.grpc.milvus.{
  ConnectRequest,
  ConnectResponse,
  CreateDatabaseRequest,
  GetLoadStateRequest,
  GetLoadStateResponse,
  ListDatabasesRequest,
  ListDatabasesResponse,
  MilvusServiceGrpc
}

import io.grpc.{Server, ServerServiceDefinition, Status => GrpcStatus}
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import io.grpc.stub.{ServerCalls, StreamObserver}

/** Retries seen from the wire: a gRPC server on a local port answers with the
  * failures a Milvus proxy or a load balancer in front of it returns, and the
  * test counts the requests that arrive.
  *
  * A read is sent again when the answer is UNAVAILABLE or Milvus rate limiting,
  * within the call's one overall deadline. A write is sent once: after a lost
  * response a second send could apply it twice.
  */
class MilvusClientRetryTest extends AnyFunSuite with Matchers {

  private val ok = Status(code = 0, errorCode = ErrorCode.Success)

  private def unary[Req, Resp](
      handler: Req => Either[GrpcStatus, Resp]
  ): io.grpc.ServerCallHandler[Req, Resp] =
    ServerCalls.asyncUnaryCall(new ServerCalls.UnaryMethod[Req, Resp] {
      override def invoke(request: Req, out: StreamObserver[Resp]): Unit =
        handler(request) match {
          case Right(response) =>
            out.onNext(response)
            out.onCompleted()
          case Left(status) => out.onError(status.asRuntimeException())
        }
    })

  private def withServer(
      build: ServerServiceDefinition.Builder => ServerServiceDefinition.Builder
  )(body: MilvusClient => Unit): Unit = {
    val service = build(
      ServerServiceDefinition
        .builder("milvus.proto.milvus.MilvusService")
        .addMethod(
          MilvusServiceGrpc.METHOD_CONNECT,
          unary[ConnectRequest, ConnectResponse](_ =>
            Right(ConnectResponse(status = Some(ok)))
          )
        )
    ).build()
    val server: Server = NettyServerBuilder
      .forAddress(new InetSocketAddress("127.0.0.1", 0))
      .addService(service)
      .build()
      .start()
    val client = MilvusClient(
      MilvusConnectionParams(uri = s"http://127.0.0.1:${server.getPort}")
    )
    try body(client)
    finally {
      client.close()
      server.shutdownNow()
    }
  }

  test("a read answered UNAVAILABLE is sent again and succeeds") {
    val requests = new AtomicInteger()
    withServer(
      _.addMethod(
        MilvusServiceGrpc.METHOD_LIST_DATABASES,
        unary[ListDatabasesRequest, ListDatabasesResponse] { _ =>
          if (requests.incrementAndGet() == 1)
            Left(GrpcStatus.UNAVAILABLE.withDescription("connection reset"))
          else
            Right(ListDatabasesResponse(status = Some(ok), dbNames = Seq("db")))
        }
      )
    ) { client =>
      client.listDatabases() shouldBe Success(Seq("db"))
      requests.get() shouldBe 2
    }
  }

  test("a read answered with Milvus rate limiting is sent again") {
    val requests = new AtomicInteger()
    withServer(
      _.addMethod(
        MilvusServiceGrpc.METHOD_LIST_DATABASES,
        unary[ListDatabasesRequest, ListDatabasesResponse] { _ =>
          if (requests.incrementAndGet() == 1)
            Right(
              ListDatabasesResponse(status =
                Some(
                  Status(
                    code = MilvusClient.RateLimitErrorCode,
                    errorCode = ErrorCode.RateLimit,
                    reason = "rate limit exceeded"
                  )
                )
              )
            )
          else
            Right(ListDatabasesResponse(status = Some(ok), dbNames = Seq("db")))
        }
      )
    ) { client =>
      client.listDatabases() shouldBe Success(Seq("db"))
      requests.get() shouldBe 2
    }
  }

  test("a read keeps retrying only within the timeout it was given") {
    val requests = new AtomicInteger()
    withServer(
      _.addMethod(
        MilvusServiceGrpc.METHOD_GET_LOAD_STATE,
        unary[GetLoadStateRequest, GetLoadStateResponse] { _ =>
          requests.incrementAndGet()
          Left(GrpcStatus.UNAVAILABLE)
        }
      ).addMethod(
        MilvusServiceGrpc.METHOD_LIST_DATABASES,
        unary[ListDatabasesRequest, ListDatabasesResponse](_ =>
          Right(ListDatabasesResponse(status = Some(ok)))
        )
      )
    ) { client =>
      // The first call opens the channel; the timed call then measures only
      // its own attempts.
      client.listDatabases() shouldBe Success(Seq.empty)
      val started = System.nanoTime()
      client.getLoadState("default", "c", 1500L) shouldBe a[Failure[_]]
      val elapsedMillis = (System.nanoTime() - started) / 1000000L
      requests.get() should be >= 2
      elapsedMillis should be < 3000L
    }
  }

  test("a write answered UNAVAILABLE is sent once") {
    val requests = new AtomicInteger()
    withServer(
      _.addMethod(
        MilvusServiceGrpc.METHOD_CREATE_DATABASE,
        unary[CreateDatabaseRequest, Status] { _ =>
          requests.incrementAndGet()
          Left(GrpcStatus.UNAVAILABLE)
        }
      )
    ) { client =>
      client.createDatabase("db") shouldBe a[Failure[_]]
      requests.get() shouldBe 1
    }
  }
}
