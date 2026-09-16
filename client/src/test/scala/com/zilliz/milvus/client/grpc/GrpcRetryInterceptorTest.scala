package com.zilliz.milvus.client.grpc

import java.io.{ByteArrayInputStream, InputStream}
import java.nio.charset.StandardCharsets

import org.scalatest.funsuite.AnyFunSuite

import io.grpc.{
  CallOptions,
  Channel,
  ClientCall,
  Metadata,
  MethodDescriptor,
  Status
}

class GrpcRetryInterceptorTest extends AnyFunSuite {

  test("a bounded call forwards a retryable failure without backoff") {
    var starts = 0
    var closed = Option.empty[Status]
    val channel = new Channel {
      override def authority(): String = "test"

      override def newCall[ReqT, RespT](
          method: MethodDescriptor[ReqT, RespT],
          callOptions: CallOptions
      ): ClientCall[ReqT, RespT] =
        new ClientCall[ReqT, RespT] {
          override def start(
              listener: ClientCall.Listener[RespT],
              headers: Metadata
          ): Unit = {
            starts += 1
            listener.onClose(Status.UNAVAILABLE, new Metadata())
          }
          override def request(numMessages: Int): Unit = ()
          override def cancel(message: String, cause: Throwable): Unit = ()
          override def halfClose(): Unit = ()
          override def sendMessage(message: ReqT): Unit = ()
        }
    }
    val call = new GrpcRetryInterceptor(initialDelayMillis = 500L)
      .interceptCall(
        method,
        CallOptions.DEFAULT.withOption(
          GrpcRetryInterceptor.DisableRetries,
          java.lang.Boolean.TRUE
        ),
        channel
      )

    call.start(
      new ClientCall.Listener[String] {
        override def onClose(status: Status, trailers: Metadata): Unit =
          closed = Some(status)
      },
      new Metadata()
    )

    assert(starts == 1)
    assert(closed.exists(_.getCode == Status.Code.UNAVAILABLE))
  }

  private val stringMarshaller = new MethodDescriptor.Marshaller[String] {
    override def stream(value: String): InputStream =
      new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8))

    override def parse(stream: InputStream): String =
      scala.io.Source.fromInputStream(stream).mkString
  }

  private val method: MethodDescriptor[String, String] =
    MethodDescriptor
      .newBuilder(stringMarshaller, stringMarshaller)
      .setType(MethodDescriptor.MethodType.UNARY)
      .setFullMethodName("test/retry")
      .build()
}
