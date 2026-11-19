package com.zilliz.spark.connector.write

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.StructType
import org.scalatest.funsuite.AnyFunSuite

import com.zilliz.spark.connector.metrics.{
  OperationToken,
  WriteOperationalMetrics,
  WritePhase
}

class ColumnReplaceMetricsTest extends AnyFunSuite {
  private val task = ColumnReplaceTask(
    7L,
    0,
    Seq.empty,
    StructType(Nil),
    Map.empty,
    "",
    WriteOptions(replaceColumns = true)
  )

  test("a rejected row preserves its error and never counts as accepted") {
    val writer = new ColumnReplaceDataWriter(task, Map.empty)
    try {
      val failure = intercept[IllegalArgumentException](
        writer.write(new GenericInternalRow(Array[Any](null)))
      )
      assert(failure.getMessage.contains("null '_milvus_row_id'"))
      val values = writer
        .currentMetricsValues()
        .map(metric => metric.name() -> metric.value())
        .toMap
      assert(values(WriteOperationalMetrics.AcceptedRows) == 0L)
      assert(values(WriteOperationalMetrics.TaskFailures) == 1L)
      assert(values(WriteOperationalMetrics.ValidationFailures) == 1L)
    } finally writer.close()
  }

  test("incomplete adopted messages fail before files are reported ready") {
    val plan = ColumnReplacePlan(
      SourceSnapshot("snapshot.json", 7L, "source", 10L, 0),
      Seq.empty,
      StructType(Nil),
      Seq(PlannedSegment(30L, 20L, "segment/30", 1L, 4L)),
      Map.empty,
      "",
      WriteResultLocation("result", Map.empty, "result"),
      WriteOptions(replaceColumns = true)
    )
    val metrics = new WriteOperationalMetrics(Map(30L -> 4L))
    val batch = new ColumnReplaceBatchWrite(
      plan,
      task,
      metrics,
      "test-operation",
      OperationToken("test-operation", "write", None)
    )
    val failure = intercept[IllegalArgumentException](
      batch.commit(Array(ColumnReplaceCommitMessage(Seq.empty)))
    )
    assert(failure.getMessage.contains("no rows of segment"))
    assert(metrics.values(WriteOperationalMetrics.ReadyRows) == 0L)
    assert(metrics.values(WriteOperationalMetrics.CommittedRows) == 0L)
    assert(
      metrics.values(
        WriteOperationalMetrics.FailurePhase
      ) == WritePhase.Validating
    )
  }
}
