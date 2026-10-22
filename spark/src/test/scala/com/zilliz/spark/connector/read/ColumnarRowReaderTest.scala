package com.zilliz.spark.connector.read

import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}
import org.scalatest.funsuite.AnyFunSuite

final class ColumnarRowReaderTest extends AnyFunSuite {
  private def batch(values: Long*): ColumnarBatch = {
    val column = new OnHeapColumnVector(math.max(1, values.size), LongType)
    values.zipWithIndex.foreach { case (value, index) =>
      column.putLong(index, value)
    }
    new ColumnarBatch(Array[ColumnVector](column), values.size)
  }

  test(
    "rows reuse the batch reader across empty batches and repeated get calls"
  ) {
    val batches = Iterator(batch(), batch(1L, 2L), batch(), batch(3L))
    var current: ColumnarBatch = null
    var closes = 0
    val source = new PartitionReader[ColumnarBatch] {
      override def next(): Boolean = {
        if (current != null) current.close()
        current = if (batches.hasNext) batches.next() else null
        current != null
      }
      override def get(): ColumnarBatch = current
      override def close(): Unit = {
        closes += 1
        if (current != null) current.close()
      }
    }
    val reader = new ColumnarRowReader(source)
    intercept[IllegalArgumentException](reader.get())
    val values = Vector.newBuilder[Long]
    while (reader.next()) {
      val value = reader.get().getLong(0)
      assert(reader.get().getLong(0) == value)
      values += value
    }
    assert(values.result() == Vector(1L, 2L, 3L))
    intercept[IllegalArgumentException](reader.get())
    reader.close()
    reader.close()
    assert(closes == 1)
    assert(!reader.next())
  }

  test("read failures remain visible and the caller can close the source") {
    val failure = new IllegalStateException("read failed")
    var closed = false
    val reader = new ColumnarRowReader(new PartitionReader[ColumnarBatch] {
      override def next(): Boolean = throw failure
      override def get(): ColumnarBatch =
        throw new AssertionError("not reached")
      override def close(): Unit = closed = true
    })
    assert(intercept[IllegalStateException](reader.next()) eq failure)
    reader.close()
    assert(closed)
  }
}
