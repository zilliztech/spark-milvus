package com.zilliz.milvus.client.grpc

import java.util.concurrent.TimeUnit
import scala.annotation.tailrec
import scala.util.{Failure, Try}

import io.grpc.Status

/** Sends one read-only RPC again after a transient failure, inside one overall
  * deadline.
  *
  * Every attempt is a new call: `attempt` receives the milliseconds left and
  * builds its call from scratch, so nothing of a failed call is reused. Only
  * reads go through here; a write whose response is lost may already be
  * applied, and sending it again would apply it twice.
  *
  * The wait between attempts starts at `initialDelayMillis` and grows by
  * `multiplier` up to `maxDelayMillis`. No wait starts that would end at or
  * after the deadline; when attempts or time run out, the last result is
  * returned as it is.
  */
final case class RpcRetry(
    maxAttempts: Int = 5,
    initialDelayMillis: Long = 500L,
    maxDelayMillis: Long = 5000L,
    multiplier: Double = 2.0
) {
  require(maxAttempts >= 1, s"maxAttempts must be positive, got $maxAttempts")
  require(initialDelayMillis >= 0L, "initialDelayMillis must not be negative")

  def run[A](
      timeoutMillis: Long,
      nanoTime: () => Long = () => System.nanoTime(),
      sleep: Long => Unit = millis => Thread.sleep(millis)
  )(attempt: Long => A)(retryable: Try[A] => Boolean): Try[A] = {
    require(timeoutMillis > 0L, "RPC timeout must be positive")
    val started = nanoTime()
    def left(): Long =
      timeoutMillis - TimeUnit.NANOSECONDS.toMillis(nanoTime() - started)

    @tailrec
    def loop(attempts: Int, delay: Long): Try[A] = {
      val result = Try(attempt(math.max(1L, left())))
      if (attempts >= maxAttempts || !retryable(result) || delay >= left()) {
        result
      } else {
        val interrupted =
          try {
            sleep(delay)
            false
          } catch {
            case _: InterruptedException =>
              Thread.currentThread().interrupt()
              true
          }
        if (interrupted) result
        else
          loop(
            attempts + 1,
            math.min((delay * multiplier).toLong, maxDelayMillis)
          )
      }
    }

    loop(1, initialDelayMillis)
  }
}

object RpcRetry {

  /** gRPC UNAVAILABLE: the call did not reach a server that could answer it, or
    * the connection dropped under it; the server did not reject the request.
    */
  def isUnavailable(result: Try[_]): Boolean = result match {
    case Failure(error) =>
      Status.fromThrowable(error).getCode == Status.Code.UNAVAILABLE
    case _ => false
  }
}
