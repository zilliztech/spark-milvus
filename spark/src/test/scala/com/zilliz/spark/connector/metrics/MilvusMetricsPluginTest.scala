package com.zilliz.spark.connector.metrics

import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.SparkSession
import org.apache.spark.TaskContext
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.{Millis, Seconds, Span}

import com.zilliz.spark.connector.read.ObservedPartitionReader

class MilvusMetricsPluginTest extends AnyFunSuite with Eventually {
  override implicit val patienceConfig: PatienceConfig = PatienceConfig(
    timeout = Span(15, Seconds),
    interval = Span(20, Millis)
  )

  test(
    "the public Spark plugin transports read and write observations and retires SQL execution state"
  ) {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("milvus-operational-metrics-test")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.plugins", classOf[MilvusMetricsPlugin].getName)
      .config("spark.milvus.metrics.reportIntervalMillis", "20")
      .getOrCreate()
    try {
      val state = OperationalMetrics.driver.get._2
      val registry = state.registry
      val resourcesClosed =
        spark.sparkContext.longAccumulator("synthetic-readers-closed")
      SQLExecution.withNewExecutionId(spark.range(2).queryExecution) {
        val read = OperationalMetrics.begin(
          "read",
          Map(ReadOperationalMetrics.PlannedSegments -> 2L)
        )
        val writeMetrics = new WriteOperationalMetrics(Map(1L -> 10L))
        val write =
          OperationalMetrics.begin("write", writeMetrics.operationalValues)
        assert(read.executionId.nonEmpty)
        assert(write.executionId == read.executionId)
        OperationalMetrics.phase(read, "reading")
        OperationalMetrics.phase(write, "writing")
        spark.sparkContext.parallelize(Seq(1, 2), 2).foreachPartition { _ =>
          val partition = TaskContext.get().partitionId().toString
          val readTask =
            OperationalMetrics.task(read, "reading", Some(partition))
          val progress = new ReadProgress(Some(readTask))
          progress.decodedRows(10L)
          progress.excludedRows(2L, 3L)
          if (partition == "0") {
            progress.emittedRows(5L)
            progress.end(ReadEndReason.Exhausted)
            progress.close()
          } else {
            val underlying = new PartitionReader[Int] {
              private var value = 0
              override def next(): Boolean = { value += 1; value <= 10 }
              override def get(): Int = value
              override def close(): Unit = resourcesClosed.add(1L)
            }
            val observed =
              new ObservedPartitionReader(underlying, progress, (_: Int) => 1L)
            (1 to 5).foreach { _ =>
              require(observed.next())
              observed.get()
            }
            // Leave an early-stopped reader open. Its completion callback must
            // close resources and publish consumer_stop before the reporter's
            // fallback completion callback sends the final snapshot.
          }
          val writeTask = OperationalMetrics.task(write, "writing")
          val work = new WriteTaskMetrics
          work.rowAccepted()
          work.values.foreach { case (name, value) =>
            writeTask.set(name, value)
          }
          writeTask.end("files_ready")
          writeTask.close()
        }
        eventually {
          assert(
            registry.counter("milvus_read_rows_decoded_total").getCount == 20L
          )
          assert(
            registry.counter("milvus_read_rows_emitted_total").getCount == 10L
          )
          assert(
            registry
              .counter("milvus_read_segments_completed_total")
              .getCount == 2L
          )
          assert(
            registry.counter("milvus_read_ends_exhausted_total").getCount == 1L
          )
          assert(
            registry
              .counter("milvus_read_ends_consumer_stop_total")
              .getCount == 1L
          )
          assert(
            registry.counter("milvus_write_rows_accepted_total").getCount == 2L
          )
          assert(resourcesClosed.value == 1L)
        }
        writeMetrics.filesReady(10L, 1L)
        writeMetrics.segmentOutcome(1L, "committed")
        writeMetrics.published()
        writeMetrics.operationalValues.foreach { case (name, value) =>
          OperationalMetrics.set(write, name, value)
        }
        OperationalMetrics.finish(write, "success")
        assert(state.activeCount == 2)
        assert(
          registry
            .counter("milvus_write_rows_manifest_committed_total")
            .getCount == 10L
        )
      }
      eventually { assert(state.activeCount == 0) }
      assert(
        registry.counter("milvus_read_operations_success_total").getCount == 1L
      )
      assert(
        registry.counter("milvus_write_operations_success_total").getCount == 1L
      )
      assert(
        registry.counter("milvus_write_receipt_published_total").getCount == 1L
      )
      assert(state.trackedCount == 0)
      intercept[IllegalStateException] {
        SQLExecution.withNewExecutionId(spark.range(1).queryExecution) {
          OperationalMetrics.begin("read")
          throw new IllegalStateException("deliberate execution failure")
        }
      }
      eventually {
        assert(state.activeCount == 0)
        assert(
          registry.counter("milvus_read_operations_failed_total").getCount == 1L
        )
      }
      assert(
        registry.counter("milvus_read_operations_success_total").getCount == 1L
      )
    } finally {
      spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
    assert(OperationalMetrics.driver.isEmpty)
    assert(OperationalMetrics.reporter.isEmpty)
  }
}
