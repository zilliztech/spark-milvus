package com.zilliz.milvus.client.grpc

import java.util.concurrent.TimeUnit

import io.grpc._
import io.grpc.{Status => GrpcStatus}
import io.grpc.Status.Code

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
                } else if (
                  callOptions
                    .getOption(GrpcRetryInterceptor.DisableRetries)
                    .booleanValue()
                ) {
                  // Procedure polling owns its retry schedule. Forward the
                  // failure immediately so an interceptor backoff cannot run
                  // past the procedure's overall deadline.
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
                    try Thread.sleep(currentDelay)
                    catch {
                      case interrupted: InterruptedException =>
                        Thread.currentThread().interrupt()
                        super.onClose(
                          GrpcStatus.CANCELLED
                            .withDescription("gRPC retry interrupted")
                            .withCause(interrupted),
                          trailers
                        )
                        return
                    }
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

object GrpcRetryInterceptor {
  private[client] val DisableRetries: CallOptions.Key[java.lang.Boolean] =
    CallOptions.Key.createWithDefault(
      "spark-milvus-disable-client-retries",
      java.lang.Boolean.FALSE
    )
}
