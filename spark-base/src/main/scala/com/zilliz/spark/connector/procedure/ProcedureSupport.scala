package com.zilliz.spark.connector.procedure

import java.util.concurrent.TimeUnit
import java.util.Locale

import com.zilliz.milvus.client.api.{MilvusClient, MilvusConnectionParams}
import com.zilliz.spark.connector.options.MilvusOption

private[procedure] final case class ProcedureTarget(
    database: String,
    collection: String,
    connectionParams: MilvusConnectionParams
)

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

/** Shared validation, client ownership, and bounded polling for management
  * procedures. SQL parsing and Spark plan construction stay in `extensions`;
  * this object begins at the already-bound procedure arguments.
  */
private[procedure] object ProcedureSupport {
  private val DefaultWaitSeconds = 600L
  private val PollMillis = 1000L

  def target(args: ProcedureArgs, procedure: String): ProcedureTarget = {
    val (databaseInName, collection) =
      parseCollection(args.string("collection"), procedure)
    val options = collectionOptions(
      args.options,
      databaseInName,
      collection,
      procedure
    )
    val milvus = MilvusOption(options)
    if (milvus.uri.trim.isEmpty) {
      throw new IllegalArgumentException(
        s"procedure $procedure: option '${MilvusOption.MilvusUri}' is required"
      )
    }
    ProcedureTarget(
      database = milvus.databaseName,
      collection = collection,
      connectionParams = milvus.connectionParams
    )
  }

  def collectionOptions(
      options: Map[String, String],
      database: Option[String],
      collection: String,
      procedure: String
  ): Map[String, String] = {
    val normalizedKeys = options.keys.toSeq.map(_.toLowerCase(Locale.ROOT))
    if (normalizedKeys.distinct.size != normalizedKeys.size) {
      throw new IllegalArgumentException(
        s"procedure $procedure: option names must be unique ignoring case"
      )
    }
    val withoutCollection = options.filterNot { case (key, _) =>
      key.equalsIgnoreCase(MilvusOption.MilvusCollectionName)
    }
    val withoutExplicitDatabase = database match {
      case Some(_) =>
        withoutCollection.filterNot { case (key, _) =>
          key.equalsIgnoreCase(MilvusOption.MilvusDatabaseName)
        }
      case None => withoutCollection
    }
    val scoped = withoutExplicitDatabase ++
      database.map(MilvusOption.MilvusDatabaseName -> _) +
      (MilvusOption.MilvusCollectionName -> collection)
    val resolvedDatabase = scoped
      .collectFirst {
        case (key, value)
            if key.equalsIgnoreCase(MilvusOption.MilvusDatabaseName) =>
          value
      }
      .map(_.trim)
      .filter(_.nonEmpty)
      .getOrElse("default")
    scoped
      .filterNot { case (key, _) =>
        key.equalsIgnoreCase(MilvusOption.MilvusDatabaseName)
      }
      .updated(MilvusOption.MilvusDatabaseName, resolvedDatabase)
  }

  def withClient[A](args: ProcedureArgs, procedure: String)(
      run: (ProcedureTarget, MilvusClient) => A
  ): A =
    withClient(args, procedure, MilvusClient.apply)(run)

  private[procedure] def withClient[A](
      args: ProcedureArgs,
      procedure: String,
      open: MilvusConnectionParams => MilvusClient
  )(run: (ProcedureTarget, MilvusClient) => A): A = {
    val resolved = target(args, procedure)
    val client = open(resolved.connectionParams)
    try run(resolved, client)
    finally client.close()
  }

  def nonBlank(value: String, procedure: String, parameter: String): String = {
    val original = Option(value).getOrElse("")
    if (original.trim.isEmpty) {
      throw new IllegalArgumentException(
        s"procedure $procedure: argument '$parameter' must not be empty"
      )
    }
    original
  }

  def nonNegative(
      value: Long,
      procedure: String,
      parameter: String
  ): Long = {
    if (value < 0L) {
      throw new IllegalArgumentException(
        s"procedure $procedure: argument '$parameter' must be non-negative, got $value"
      )
    }
    value
  }

  def waitOptions(args: ProcedureArgs, procedure: String): WaitOptions = {
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

  def parseCollection(
      value: String,
      procedure: String
  ): (Option[String], String) = {
    val trimmed = Option(value).map(_.trim).getOrElse("")
    if (trimmed.isEmpty) {
      throw new IllegalArgumentException(
        s"procedure $procedure: collection must not be empty"
      )
    }
    trimmed.indexOf('.') match {
      case -1 => (None, trimmed)
      case i =>
        val database = trimmed.substring(0, i)
        val collection = trimmed.substring(i + 1)
        if (
          database.isEmpty || collection.isEmpty || collection.contains('.')
        ) {
          throw new IllegalArgumentException(
            s"procedure $procedure: collection must be 'db.collection' or 'collection', got '$value'"
          )
        }
        (Some(database), collection)
    }
  }
}
