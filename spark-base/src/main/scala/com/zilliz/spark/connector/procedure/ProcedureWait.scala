package com.zilliz.spark.connector.procedure

import java.util.concurrent.TimeUnit

private[procedure] final case class WaitOptions(
    enabled: Boolean,
    timeoutSeconds: Long
)

private[procedure] sealed trait WaitDecision

private[procedure] object WaitDecision {
  case object Continue extends WaitDecision
  case object Done extends WaitDecision
  final case class Failed(reason: String) extends WaitDecision
}

/** Bounded waiting for Milvus operations submitted by management procedures. */
private[procedure] object ProcedureWait {
  private val DefaultWaitSeconds = 600L
  private val PollMillis = 1000L

  def options(args: ProcedureArgs, procedure: String): WaitOptions = {
    val enabled = args.booleanOpt("wait").getOrElse(false)
    if (!enabled && args.contains("timeout_seconds")) {
      throw new IllegalArgumentException(
        s"procedure $procedure: argument 'timeout_seconds' requires wait => true"
      )
    }
    val timeout = args.longOpt("timeout_seconds").getOrElse(DefaultWaitSeconds)
    if (enabled && timeout <= 0L) {
      throw new IllegalArgumentException(
        s"procedure $procedure: argument 'timeout_seconds' must be positive, got $timeout"
      )
    }
    if (timeout > TimeUnit.NANOSECONDS.toSeconds(Long.MaxValue)) {
      throw new IllegalArgumentException(
        s"procedure $procedure: argument 'timeout_seconds' is too large: $timeout"
      )
    }
    WaitOptions(enabled, timeout)
  }

  def await[T](description: String, timeoutSeconds: Long)(
      probe: Long => T
  )(decide: T => WaitDecision): T =
    await(
      description,
      timeoutSeconds,
      PollMillis,
      () => System.nanoTime(),
      millis => Thread.sleep(millis)
    )(probe)(decide)

  private[procedure] def await[T](
      description: String,
      timeoutSeconds: Long,
      pollMillis: Long,
      nanoTime: () => Long,
      sleep: Long => Unit
  )(probe: Long => T)(decide: T => WaitDecision): T = {
    val timeoutNanos = TimeUnit.SECONDS.toNanos(timeoutSeconds)
    val started = nanoTime()
    def remainingMillis(elapsedNanos: Long): Long =
      math.max(
        1L,
        TimeUnit.NANOSECONDS.toMillis(timeoutNanos - elapsedNanos)
      )

    var last = probe(remainingMillis(0L))
    while (true) {
      val elapsed = nanoTime() - started
      if (elapsed >= timeoutNanos) {
        throw new IllegalStateException(
          s"$description did not complete within $timeoutSeconds seconds; last state: $last"
        )
      }
      decide(last) match {
        case WaitDecision.Done => return last
        case WaitDecision.Failed(reason) =>
          throw new IllegalStateException(s"$description failed: $reason")
        case WaitDecision.Continue =>
      }
      try sleep(math.min(pollMillis, remainingMillis(elapsed)))
      catch {
        case interrupted: InterruptedException =>
          Thread.currentThread().interrupt()
          throw interrupted
      }
      val afterSleep = nanoTime() - started
      if (afterSleep >= timeoutNanos) {
        throw new IllegalStateException(
          s"$description did not complete within $timeoutSeconds seconds; last state: $last"
        )
      }
      last = probe(remainingMillis(afterSleep))
    }
    last
  }
}
