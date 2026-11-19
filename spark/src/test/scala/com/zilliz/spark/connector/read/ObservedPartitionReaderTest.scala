package com.zilliz.spark.connector.read

import java.io.IOException

import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}
import org.scalatest.funsuite.AnyFunSuite

import com.zilliz.spark.connector.metrics.{
  ReadEndReason,
  ReadFailurePhase,
  ReadOperationalMetrics,
  ReadProgress
}

class ObservedPartitionReaderTest extends AnyFunSuite {
  private class Batches extends PartitionReader[ColumnarBatch] {
    private var index = 0
    var gets = 0
    var closes = 0
    override def next(): Boolean = {
      index += 1
      index <= 3
    }
    override def get(): ColumnarBatch = {
      gets += 1
      new ColumnarBatch(Array.empty[ColumnVector], 2)
    }
    override def close(): Unit = { closes += 1 }
  }

  test(
    "output counts get consumption once and early close has a distinct terminal reason"
  ) {
    val source = new Batches
    val progress = new ReadProgress
    val reader = new ObservedPartitionReader(source, progress)
    assert(reader.next())
    val batch = reader.get()
    assert(batch.numRows() == 2)
    assert(reader.get() eq batch)
    assert(reader.next())
    reader.close()
    reader.close()
    assert(!reader.next())
    assert(source.gets == 1)
    assert(source.closes == 1)
    assert(progress.values(ReadOperationalMetrics.EmittedRows) == 2L)
    assert(progress.values(ReadOperationalMetrics.CompletedSegments) == 1L)
    assert(
      progress.values(
        ReadOperationalMetrics.endName(ReadEndReason.ConsumerStop)
      ) == 1L
    )
    assert(
      progress.values(
        ReadOperationalMetrics.endName(ReadEndReason.Exhausted)
      ) == 0L
    )
  }

  test("cancelled readers do not count as successfully consumed") {
    var cancelled = false
    val progress = new ReadProgress(isCancelled = () => cancelled)
    val reader = new ObservedPartitionReader(new Batches, progress)
    assert(reader.next())
    reader.get()
    cancelled = true
    reader.close()
    assert(progress.values(ReadOperationalMetrics.CompletedSegments) == 0L)
    assert(
      progress.values(
        ReadOperationalMetrics.endName(ReadEndReason.Cancelled)
      ) == 1L
    )
  }

  test("read failures retain their phase when resource cleanup also fails") {
    val primary = new IOException("read failed")
    val cleanup = new IOException("cleanup failed")
    val progress = new ReadProgress
    val source = new Batches {
      override def next(): Boolean = throw primary
      override def close(): Unit = { super.close(); throw cleanup }
    }
    val reader = new ObservedPartitionReader(source, progress)
    assert(intercept[IOException](reader.next()) eq primary)
    assert(intercept[IOException](reader.close()) eq cleanup)
    reader.close()
    // The same close exception can cross nested reader wrappers.
    progress.failed(ReadFailurePhase.Close, cleanup)
    assert(
      progress.values(
        ReadOperationalMetrics.FailurePhase
      ) == ReadFailurePhase.Read
    )
    assert(progress.values(ReadOperationalMetrics.CleanupFailures) == 1L)
    assert(progress.values(ReadOperationalMetrics.CompletedSegments) == 0L)
    assert(
      progress.values(
        ReadOperationalMetrics.endName(ReadEndReason.Failed)
      ) == 1L
    )
    assert(source.closes == 1)
  }

  test(
    "a close failure after EOF changes the final successful completion to failure"
  ) {
    val cleanup = new IOException("cleanup failed")
    val progress = new ReadProgress
    val source = new Batches {
      override def close(): Unit = { super.close(); throw cleanup }
    }
    val reader = new ObservedPartitionReader(source, progress)
    while (reader.next()) reader.get()
    assert(
      progress.values(
        ReadOperationalMetrics.endName(ReadEndReason.Exhausted)
      ) == 1L
    )
    assert(intercept[IOException](reader.close()) eq cleanup)
    assert(
      progress.values(
        ReadOperationalMetrics.FailurePhase
      ) == ReadFailurePhase.Close
    )
    assert(progress.values(ReadOperationalMetrics.CleanupFailures) == 1L)
    assert(progress.values(ReadOperationalMetrics.CompletedSegments) == 0L)
    assert(
      progress.values(
        ReadOperationalMetrics.endName(ReadEndReason.Exhausted)
      ) == 0L
    )
    assert(
      progress.values(
        ReadOperationalMetrics.endName(ReadEndReason.Failed)
      ) == 1L
    )
  }

  test("a zero partition limit emits nothing and never advances its source") {
    val source = new Batches {
      override def next(): Boolean =
        fail("the zero limit must not advance its source")
    }
    val progress = new ReadProgress
    val reader =
      new ObservedPartitionReader(
        new LimitedBatchReader(source, 0, Some(progress)),
        progress
      )
    assert(!reader.next())
    reader.close()
    assert(progress.values(ReadOperationalMetrics.EmittedRows) == 0L)
    assert(
      progress.values(ReadOperationalMetrics.endName(ReadEndReason.Limit)) == 1L
    )
    assert(progress.values(ReadOperationalMetrics.CompletedSegments) == 1L)
  }
}
