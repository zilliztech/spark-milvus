package com.zilliz.spark.connector.read

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{BigIntVector, VectorSchemaRoot}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.delete.DeletePlan
import com.zilliz.milvus.storage.expr.{
  Comparison,
  ComparisonOperator,
  FieldRef,
  PredicateExpr
}
import com.zilliz.milvus.storage.expr.Literal.IntegerValue
import com.zilliz.milvus.storage.read.exec.{ReadMetrics, SegmentReader}
import com.zilliz.milvus.storage.read.plan.{DeleteSource, SegmentReadTask}
import com.zilliz.milvus.storage.schema.SchemaMapper
import com.zilliz.milvus.storage.snapshot.SegmentLayout
import com.zilliz.spark.connector.metrics.{
  ReadEndReason,
  ReadOperationalMetrics,
  ReadProgress,
  ScanMetrics
}
import io.milvus.grpc.schema.{CollectionSchema, DataType, FieldSchema}

class MilvusRowPartitionReaderTest extends AnyFunSuite with Matchers {

  private val sparkSchema = StructType(
    Seq(StructField("id", LongType, nullable = false))
  )

  private val collection = CollectionSchema(
    name = "row-filter-test",
    fields = Seq(
      FieldSchema(
        fieldID = 100L,
        name = "id",
        dataType = DataType.Int64,
        isPrimaryKey = true
      )
    )
  )

  private final class FakeSegmentReader(root: VectorSchemaRoot)
      extends SegmentReader {
    private val rowCount = root.getRowCount.toLong
    private var pending = Option(root)
    private var delivered = 0L
    private var closed = false

    def isClosed: Boolean = closed

    override def next(): Option[VectorSchemaRoot] = pending match {
      case Some(batch) =>
        pending = None
        delivered = rowCount
        Some(batch)
      case None => None
    }

    override def take(
        rowIndices: Array[Long],
        columns: Seq[String],
        parallelism: Int
    ): SegmentReader.TakeResult =
      throw new UnsupportedOperationException(
        "this sequential batch fixture does not support take"
      )

    override def deliveredRows: Long = delivered

    override def metrics: ReadMetrics = ReadMetrics.Zero

    override def close(): Unit = {
      if (!closed) pending.foreach(_.close())
      pending = None
      closed = true
    }
  }

  test("row scans filter V3 physical columns before returning rows") {
    val allocator = new RootAllocator()
    try {
      readIds(
        root(allocator),
        task(),
        allocator,
        Some(idComparison(ComparisonOperator.GreaterThanOrEqual, 12L))
      ) shouldBe Vector(12L, 13L)

      val taskWithDelete = task(
        DeleteSource.Materialized(
          DeletePlan.fromLongPks(Map(11L -> 200L))
        )
      )
      readIds(
        root(allocator),
        taskWithDelete,
        allocator,
        Some(idComparison(ComparisonOperator.LessThanOrEqual, 12L))
      ) shouldBe Vector(10L, 12L)

      allocator.getAllocatedMemory shouldBe 0L
    } finally allocator.close()
  }

  test(
    "row operational metrics separate decoding, deletes, predicates and consumed output"
  ) {
    val allocator = new RootAllocator()
    val progress = new ReadProgress
    val deleteTask =
      task(DeleteSource.Materialized(DeletePlan.fromLongPks(Map(11L -> 200L))))
    val reader = new MilvusRowPartitionReader(
      sparkSchema,
      ColumnBinding(MilvusInputPartition(deleteTask), sparkSchema),
      Some(idComparison(ComparisonOperator.LessThanOrEqual, 12L)),
      allocator,
      preopenedSegmentReader = Some(new FakeSegmentReader(root(allocator))),
      progress = progress
    )
    val observed =
      new ObservedPartitionReader(reader, progress, (_: InternalRow) => 1L)
    try {
      observed.next() shouldBe true
      observed.get().getLong(0) shouldBe 10L
      observed.get().getLong(0) shouldBe 10L
      observed.next() shouldBe true
      observed.get().getLong(0) shouldBe 12L
      observed.next() shouldBe false
      observed.next() shouldBe false
      val values = observed
        .currentMetricsValues()
        .map(metric => metric.name() -> metric.value())
        .toMap
      values(ReadOperationalMetrics.DecodedRows) shouldBe 4L
      values(ReadOperationalMetrics.DeletedRows) shouldBe 1L
      values(ReadOperationalMetrics.FilteredRows) shouldBe 1L
      values(ReadOperationalMetrics.EmittedRows) shouldBe 2L
      values(ScanMetrics.RowsMaterialized) shouldBe 2L
      values(ReadOperationalMetrics.CompletedSegments) shouldBe 1L
      values(
        ReadOperationalMetrics.endName(ReadEndReason.Exhausted)
      ) shouldBe 1L
    } finally {
      observed.close()
      observed.close()
      allocator.close()
    }
  }

  test(
    "a row limit reports consumed output without pretending the decoded batch reached EOF"
  ) {
    val allocator = new RootAllocator()
    val progress = new ReadProgress
    val base = new MilvusRowPartitionReader(
      sparkSchema,
      ColumnBinding(MilvusInputPartition(task()), sparkSchema),
      allocator = allocator,
      preopenedSegmentReader = Some(new FakeSegmentReader(root(allocator))),
      progress = progress
    )
    val reader = new ObservedPartitionReader(
      new LimitedRowReader(base, 1, Some(progress)),
      progress,
      (_: InternalRow) => 1L
    )
    try {
      reader.next() shouldBe true
      reader.get().getLong(0) shouldBe 10L
      reader.get().getLong(0) shouldBe 10L
      reader.next() shouldBe false
      val values = reader
        .currentMetricsValues()
        .map(metric => metric.name() -> metric.value())
        .toMap
      values(ReadOperationalMetrics.DecodedRows) shouldBe 4L
      values(ReadOperationalMetrics.EmittedRows) shouldBe 1L
      values(ReadOperationalMetrics.endName(ReadEndReason.Limit)) shouldBe 1L
      values(
        ReadOperationalMetrics.endName(ReadEndReason.Exhausted)
      ) shouldBe 0L
    } finally {
      reader.close()
      allocator.close()
    }
  }

  private def idComparison(
      operator: ComparisonOperator,
      value: Long
  ): PredicateExpr =
    Comparison(FieldRef(100L, DataType.Int64), operator, IntegerValue(value))

  private def root(allocator: RootAllocator): VectorSchemaRoot = {
    val arrowSchema =
      SchemaMapper.convertToArrowSchemaWithFieldIdNames(collection)
    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    root.allocateNew()
    val rowIds = root.getVector("0").asInstanceOf[BigIntVector]
    val timestamps = root.getVector("1").asInstanceOf[BigIntVector]
    val ids = root.getVector("100").asInstanceOf[BigIntVector]
    (10L to 13L).zipWithIndex.foreach { case (id, row) =>
      rowIds.setSafe(row, row.toLong)
      timestamps.setSafe(row, 100L)
      ids.setSafe(row, id)
    }
    root.setRowCount(4)
    root
  }

  private def task(
      deletes: DeleteSource = DeleteSource.None
  ): SegmentReadTask =
    SegmentReadTask(
      segmentId = 1L,
      partitionId = 1L,
      layout = SegmentLayout.Manifest("unused", 0L),
      schemaBytes = collection.toByteArray,
      properties = Map.empty,
      deletes = deletes
    )

  private def readIds(
      root: VectorSchemaRoot,
      task: SegmentReadTask,
      allocator: RootAllocator,
      pushedExpression: Option[PredicateExpr]
  ): Vector[Long] = {
    val partition = MilvusInputPartition(task)
    val binding = ColumnBinding(partition, sparkSchema)
    val fake = new FakeSegmentReader(root)
    val reader = new MilvusRowPartitionReader(
      schema = sparkSchema,
      setup = binding,
      pushedExpression = pushedExpression,
      allocator = allocator,
      preopenedSegmentReader = Some(fake)
    )
    val ids = Vector.newBuilder[Long]
    try {
      while (reader.next()) ids += reader.get().getLong(0)
    } finally reader.close()
    fake.isClosed shouldBe true
    ids.result()
  }
}
