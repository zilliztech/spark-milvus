package com.zilliz.spark.connector.metrics

import com.codahale.metrics.MetricRegistry
import org.scalatest.funsuite.AnyFunSuite

class OperationalMetricsTest extends AnyFunSuite {
  private val token = OperationToken("read-operation", "read", Some("7"))

  private def snapshot(
      task: Long = 1L,
      sequence: Long = 1L,
      rows: Long = 10L,
      segment: String = "segment-a",
      operation: OperationToken = token,
      end: Option[String] = None,
      progress: Long = 1L
  ): TaskSnapshot = TaskSnapshot(
    TaskIdentity(
      operation,
      task,
      0,
      0,
      "executor-1",
      s"$task-$segment",
      Some(segment)
    ),
    sequence,
    Map(
      "milvus.read.rows.decoded" -> rows,
      "milvus.read.segments.completed" -> 1L
    ),
    Map.empty,
    progress,
    end
  )

  private def count(state: OperationalState, name: String): Long =
    state.registry.counter(name).getCount
  private def gauge(state: OperationalState, name: String): Long =
    state.registry.getGauges.get(name).getValue.asInstanceOf[Long]

  test(
    "cumulative work survives duplicate and out-of-order snapshots and includes retries"
  ) {
    val state = new OperationalState(new MetricRegistry)
    state.begin(token, Map.empty)
    state.receive(snapshot(sequence = 2, rows = 20))
    state.receive(snapshot(sequence = 2, rows = 20))
    state.receive(snapshot(sequence = 1, rows = 10))
    state.receive(snapshot(task = 2, rows = 15))
    assert(count(state, "milvus_read_rows_decoded_total") == 35L)
  }

  test(
    "accepted segments join task results in either order and deduplicate retries"
  ) {
    val state = new OperationalState(new MetricRegistry)
    state.begin(token, Map.empty)
    state.receive(snapshot(end = Some("limit")))
    assert(count(state, "milvus_read_segments_completed_total") == 0L)
    state.taskEnded(Some("7"), 1L, success = true)
    state.taskEnded(Some("7"), 2L, success = true)
    state.receive(snapshot(task = 2, end = Some("exhausted")))
    state.taskEnded(Some("7"), 3L, success = false)
    state.receive(
      snapshot(task = 3, segment = "segment-b", end = Some("exhausted"))
    )
    assert(count(state, "milvus_read_segments_completed_total") == 1L)
    state.taskEnded(Some("7"), 4L, success = true)
    state.receive(
      snapshot(task = 4, segment = "segment-b", end = Some("consumer_stop"))
    )
    assert(count(state, "milvus_read_segments_completed_total") == 2L)
  }

  test("multiple logical segments in one Spark task complete independently") {
    val state = new OperationalState(new MetricRegistry)
    state.begin(token, Map.empty)
    state.taskEnded(Some("7"), 1L, success = true)
    state.receive(snapshot(segment = "segment-a", end = Some("exhausted")))
    state.receive(snapshot(segment = "segment-b", end = Some("exhausted")))
    assert(count(state, "milvus_read_segments_completed_total") == 2L)
  }

  test(
    "activity in one operation cannot conceal another operation's stalled progress"
  ) {
    var now = 1000L
    val state = new OperationalState(new MetricRegistry, () => now)
    state.begin(token, Map.empty)
    state.phase(token, "reading")
    now = 5000L
    val other = token.copy(id = "other")
    state.begin(other, Map.empty)
    state.phase(other, "reading")
    now = 10000L
    state.receive(snapshot(operation = other))
    assert(
      gauge(
        state,
        "milvus_read_reading_oldest_progress_timestamp_seconds"
      ) == 1L
    )
    state.receive(snapshot(progress = 2))
    now = 20000L
    state.receive(snapshot(sequence = 2, progress = 2))
    assert(
      gauge(
        state,
        "milvus_read_reading_oldest_progress_timestamp_seconds"
      ) == 10L
    )
    assert(gauge(state, "milvus_read_reading_active") == 2L)
  }

  test(
    "retirement removes task state and ignores late messages while preserving failure evidence"
  ) {
    val state = new OperationalState(new MetricRegistry)
    state.begin(token, Map.empty)
    val failed = snapshot(end = Some("failed"))
      .copy(failures = Map(("open", "storage") -> 1L))
    state.receive(failed)
    state.receive(failed.copy(sequence = 2))
    state.executionEnded("7", failed = true)
    state.receive(failed.copy(sequence = 3))
    assert(state.activeCount == 0)
    assert(state.trackedCount == 0)
    assert(count(state, "milvus_read_failures_open_storage_total") == 1L)
    assert(count(state, "milvus_read_operations_failed_total") == 1L)
    assert(count(state, "milvus_read_operations_success_total") == 0L)
  }

  test(
    "finished writes retain cleanup reporting and lifetime milestones until SQL retirement"
  ) {
    val state = new OperationalState(new MetricRegistry)
    val write = token.copy(kind = "write")
    state.begin(write, Map("milvus.write.rows.expected" -> 10L))
    state.set(write, "milvus.write.rows.files_ready", 10L)
    state.set(write, "milvus.write.rows.manifest_committed", 10L)
    state.set(write, "milvus.write.rows.manifest_committed", 10L)
    state.set(write, "milvus.write.receipt.published", 1L)
    state.finish(write, "success")
    state.finish(write, "failed")
    state.set(write, "milvus.write.cleanup.failures", 1L)
    assert(state.activeCount == 1)
    assert(gauge(state, "milvus_write_planning_active") == 0L)
    state.executionEnded("7", failed = true)
    assert(state.activeCount == 0)
    assert(count(state, "milvus_write_operations_success_total") == 1L)
    assert(count(state, "milvus_write_operations_failed_total") == 0L)
    assert(count(state, "milvus_cleanup_failures_total") == 1L)
    assert(count(state, "milvus_write_cleanup_failures_total") == 1L)
    assert(count(state, "milvus_write_rows_manifest_committed_total") == 10L)
    assert(count(state, "milvus_write_receipt_published_total") == 1L)
    assert(gauge(state, "milvus_write_rows_manifest_committed") == 0L)
  }

  test(
    "state overflow is explicit and prevents a complete monitoring success claim"
  ) {
    val state = new OperationalState(new MetricRegistry, maxTrackedTasks = 1)
    state.begin(token, Map.empty)
    state.receive(snapshot())
    state.receive(snapshot(task = 2))
    assert(state.trackedCount == 1)
    assert(gauge(state, "milvus_telemetry_incomplete_operations") == 1L)
    assert(count(state, "milvus_telemetry_dropped_total") == 1L)
    state.executionEnded("7", failed = false)
    assert(count(state, "milvus_read_operations_unknown_total") == 1L)
    assert(count(state, "milvus_read_operations_success_total") == 0L)
  }

  test("executor loss and application shutdown never manufacture completion") {
    val state = new OperationalState(new MetricRegistry)
    state.begin(token, Map.empty)
    state.phase(token, "reading")
    state.receive(snapshot())
    state.executorLost("executor-1")
    state.shutdown()
    assert(
      count(state, "milvus_read_failures_reading_executor_lost_total") == 1L
    )
    assert(count(state, "milvus_read_segments_completed_total") == 0L)
    assert(count(state, "milvus_read_operations_unknown_total") == 1L)
    assert(state.activeCount == 0)
  }
}
