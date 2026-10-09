package com.zilliz.milvus.storage.read.exec

import org.apache.arrow.vector.VectorSchemaRoot
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.read.plan.SegmentReadTask
import com.zilliz.milvus.storage.snapshot.{SegmentLayout, V2ColumnGroup}

class ExpectedRowsSegmentReaderTest extends AnyFunSuite with Matchers {

  private final class StubReader(
      rows: Long,
      readMetrics: ReadMetrics = ReadMetrics.Zero
  ) extends SegmentReader {
    var closed: Boolean = false
    var taken: Option[(Seq[Long], Seq[String], Int)] = None

    override def next(): Option[VectorSchemaRoot] = None

    override def take(
        rowIndices: Array[Long],
        columns: Seq[String],
        parallelism: Int
    ): SegmentReader.TakeResult = {
      taken = Some((rowIndices.toSeq, columns, parallelism))
      SegmentReader.EmptyTakeResult
    }

    override def deliveredRows: Long = rows

    override def metrics: ReadMetrics = readMetrics

    override def close(): Unit = closed = true
  }

  private def v2Task(expectedRows: Long): SegmentReadTask =
    SegmentReadTask(
      segmentId = 31L,
      partitionId = 7L,
      layout = SegmentLayout.ColumnGroups(
        Seq(
          V2ColumnGroup(
            fieldIds = Seq(100L),
            filePaths = Seq("files/100/1"),
            fileRowCounts = Seq(expectedRows)
          )
        )
      ),
      schemaBytes = Array.emptyByteArray,
      properties = Map.empty
    )

  test("a V2 reader accepts its declared physical row count at EOF") {
    val guarded = SegmentReaderRegistry.withExpectedRows(
      v2Task(expectedRows = 3L),
      new StubReader(rows = 3L)
    )

    guarded.next() shouldBe None
    guarded.next() shouldBe None
    guarded.deliveredRows shouldBe 3L
  }

  test("a V2 reader refuses both short and excess results at EOF") {
    Seq(2L, 4L).foreach { delivered =>
      val guarded = SegmentReaderRegistry.withExpectedRows(
        v2Task(expectedRows = 3L),
        new StubReader(rows = delivered)
      )

      val err = intercept[IllegalStateException](guarded.next())
      err.getMessage should include("segment 31")
      err.getMessage should include(s"delivered $delivered")
      err.getMessage should include("expected 3")
    }
  }

  test("closing before EOF delegates without checking a partial read") {
    val delegate = new StubReader(rows = 1L)
    val guarded = SegmentReaderRegistry.withExpectedRows(
      v2Task(expectedRows = 3L),
      delegate
    )

    guarded.close()

    delegate.closed shouldBe true
  }

  test("the expected-row wrapper preserves read metrics") {
    val metrics = ReadMetrics(1L, 2L, 3L, 4L, 5L, 6L, 7L)
    val guarded = SegmentReaderRegistry.withExpectedRows(
      v2Task(expectedRows = 3L),
      new StubReader(rows = 3L, readMetrics = metrics)
    )

    guarded.metrics shouldBe metrics
  }

  test("take delegates physical offsets without verifying sequential EOF") {
    val delegate = new StubReader(rows = 0L)
    val guarded = SegmentReaderRegistry.withExpectedRows(
      v2Task(expectedRows = 3L),
      delegate
    )
    val result = guarded.take(Array(1L, 2L), Seq("100"), parallelism = 2)
    result.next() shouldBe None
    result.close()
    delegate.taken shouldBe Some((Seq(1L, 2L), Seq("100"), 2))
    guarded.deliveredRows shouldBe 0L
    guarded.close()
    delegate.closed shouldBe true
  }

  test("a manifest reader is unchanged when its row count is unknown") {
    val task = v2Task(expectedRows = 3L).copy(
      layout = SegmentLayout.Manifest("files/segment", readVersion = 2L)
    )
    val delegate = new StubReader(rows = 0L)
    val unchanged = SegmentReaderRegistry.withExpectedRows(task, delegate)

    unchanged should be theSameInstanceAs delegate
  }
}
