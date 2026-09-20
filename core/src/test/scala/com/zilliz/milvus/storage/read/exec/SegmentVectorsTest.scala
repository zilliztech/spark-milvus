package com.zilliz.milvus.storage.read.exec

import java.nio.{ByteBuffer, ByteOrder}
import scala.collection.mutable
import scala.collection.JavaConverters._

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{
  BigIntVector,
  FieldVector,
  FixedSizeBinaryVector,
  VectorSchemaRoot
}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach

import com.zilliz.milvus.storage.expr.PlanParser
import com.zilliz.milvus.storage.read.plan.SegmentReadTask
import com.zilliz.milvus.storage.schema.{VectorElementType, VectorLayout}
import com.zilliz.milvus.storage.snapshot.SegmentLayout
import io.milvus.grpc.common.KeyValuePair
import io.milvus.grpc.schema.{CollectionSchema, DataType, FieldSchema}

/** What the format side hands a search for one segment: the vectors, the rows
  * to skip and where the batch sits (vector-search.html section 2.3).
  */
class SegmentVectorsTest
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterEach {

  private val layout = VectorLayout(VectorElementType.Float32, 2)
  private var allocator: RootAllocator = _

  override def beforeEach(): Unit = allocator = new RootAllocator()

  override def afterEach(): Unit = allocator.close()

  private val collection = CollectionSchema(
    name = "coll",
    fields = Seq(
      FieldSchema(
        fieldID = 100L,
        name = "id",
        dataType = DataType.Int64,
        isPrimaryKey = true
      ),
      FieldSchema(
        fieldID = 101L,
        name = "vec",
        dataType = DataType.FloatVector,
        typeParams = Seq(KeyValuePair("dim", "2"))
      )
    )
  )

  private val task = SegmentReadTask(
    segmentId = 7L,
    partitionId = 1L,
    layout = SegmentLayout.Manifest("segments/7", 3L),
    schemaBytes = collection.toByteArray,
    properties = Map.empty
  )

  private def columnNameFor(fieldId: Long): Option[String] =
    Some(fieldId.toString)

  private def exclusions(filter: Option[String]) =
    RowExclusions.of(
      task,
      collection,
      filter.map(PlanParser.parse),
      columnNameFor
    )

  /** Batches a test wrote, handed out like a real reader does. */
  private final class FakeReader(batches: Seq[VectorSchemaRoot])
      extends SegmentReader {
    private val remaining = mutable.Queue(batches: _*)
    var closed = false
    override def next(): Option[VectorSchemaRoot] =
      if (remaining.isEmpty) None else Some(remaining.dequeue())
    override def take(
        rowIndices: Array[Long],
        columns: Seq[String],
        parallelism: Int
    ): SegmentReader.TakeResult =
      throw new UnsupportedOperationException("take")
    override def deliveredRows: Long = 0L
    override def metrics: ReadMetrics = ReadMetrics.Zero
    override def close(): Unit = closed = true
  }

  private def batch(
      ids: Seq[Long],
      vectors: Seq[Option[Seq[Float]]]
  ): VectorSchemaRoot = {
    val idVector = new BigIntVector("100", allocator)
    idVector.allocateNew(ids.size)
    ids.zipWithIndex.foreach { case (value, row) => idVector.set(row, value) }
    idVector.setValueCount(ids.size)
    val vectorColumn =
      new FixedSizeBinaryVector("101", allocator, layout.rowBytes)
    vectorColumn.allocateNew(vectors.size)
    vectors.zipWithIndex.foreach {
      case (Some(values), row) =>
        val bytes = ByteBuffer
          .allocate(layout.rowBytes)
          .order(ByteOrder.nativeOrder())
        values.foreach(bytes.putFloat)
        vectorColumn.set(row, bytes.array())
      case (None, row) => vectorColumn.setNull(row)
    }
    vectorColumn.setValueCount(vectors.size)
    new VectorSchemaRoot(
      Seq[FieldVector](idVector, vectorColumn).asJava
    )
  }

  private def floats(batch: SegmentVectors.Batch, count: Int): Seq[Float] = {
    val buffer = batch.base.buffer.duplicate().order(ByteOrder.nativeOrder())
    (0 until count).map(index => buffer.getFloat(index * 4))
  }

  test("batches arrive with their first row offset and their vectors") {
    val reader = new FakeReader(
      Seq(
        batch(Seq(1L, 2L), Seq(Some(Seq(1f, 2f)), Some(Seq(3f, 4f)))),
        batch(Seq(3L), Seq(Some(Seq(5f, 6f))))
      )
    )
    // One reader batch per call: these check what the reader hands out, not
    // what a search is given.
    val vectors = SegmentVectors.over(
      reader,
      "101",
      layout,
      exclusions(None),
      allocator,
      SegmentVectorsTest.NoJoin
    )
    try {
      val first = vectors.next().get
      try {
        first.firstRow shouldBe 0L
        first.rows shouldBe 2
        first.visibleRows shouldBe 2
        first.base.borrowed shouldBe true
        floats(first, 4) shouldBe Seq(1f, 2f, 3f, 4f)
      } finally first.close()

      val second = vectors.next().get
      try {
        second.firstRow shouldBe 2L
        second.rows shouldBe 1
        floats(second, 2) shouldBe Seq(5f, 6f)
      } finally second.close()

      vectors.next() shouldBe empty
      vectors.rows shouldBe 3L
    } finally vectors.close()
    reader.closed shouldBe true
  }

  test("reader batches are joined until they fill the configured batch") {
    // milvus-storage closes a row group at a megabyte, so a search would
    // otherwise call the engine once per handful of rows.
    val reader = new FakeReader(
      Seq(
        batch(Seq(1L, 2L), Seq(Some(Seq(1f, 2f)), None)),
        batch(Seq(3L), Seq(Some(Seq(5f, 6f)))),
        batch(Seq(4L), Seq(Some(Seq(7f, 8f))))
      )
    )
    val vectors = SegmentVectors.over(
      reader,
      "101",
      layout,
      exclusions(None),
      allocator,
      1L << 20
    )
    try {
      val only = vectors.next().get
      try {
        only.firstRow shouldBe 0L
        only.rows shouldBe 4
        // The excluded row of the first part keeps its place in the whole.
        only.excluded.get(1) shouldBe true
        only.excluded.cardinality() shouldBe 1
        only.visibleRows shouldBe 3
        only.base.rows shouldBe 4
        // Copied, so the joined batch owns its bytes.
        only.base.borrowed shouldBe false
        floats(only, 8) shouldBe Seq(1f, 2f, 0f, 0f, 5f, 6f, 7f, 8f)
      } finally only.close()

      vectors.next() shouldBe empty
      vectors.rows shouldBe 4L
    } finally vectors.close()
    reader.closed shouldBe true
  }

  test("a null vector is excluded and keeps its row offset") {
    val reader = new FakeReader(
      Seq(
        batch(Seq(1L, 2L, 3L), Seq(Some(Seq(1f, 2f)), None, Some(Seq(5f, 6f))))
      )
    )
    val vectors =
      SegmentVectors.over(
        reader,
        "101",
        layout,
        exclusions(None),
        allocator,
        SegmentVectorsTest.NoJoin
      )
    try {
      val only = vectors.next().get
      try {
        only.excluded.get(1) shouldBe true
        only.excluded.cardinality() shouldBe 1
        only.visibleRows shouldBe 2
        only.base.borrowed shouldBe false
        floats(only, 6) shouldBe Seq(1f, 2f, 0f, 0f, 5f, 6f)
      } finally only.close()
    } finally vectors.close()
  }

  test("a filter excludes the rows it rejects, before any search") {
    val reader = new FakeReader(
      Seq(
        batch(
          Seq(1L, 2L, 3L),
          Seq(Some(Seq(1f, 1f)), Some(Seq(2f, 2f)), Some(Seq(3f, 3f)))
        )
      )
    )
    val filtered = exclusions(Some("id > 2"))
    filtered.neededColumns should contain("100")
    val vectors =
      SegmentVectors.over(
        reader,
        "101",
        layout,
        filtered,
        allocator,
        SegmentVectorsTest.NoJoin
      )
    try {
      val only = vectors.next().get
      try {
        only.excluded.get(0) shouldBe true
        only.excluded.get(1) shouldBe true
        only.excluded.get(2) shouldBe false
        only.visibleRows shouldBe 1
      } finally only.close()
    } finally vectors.close()
  }

  test("closing a batch releases what it held") {
    val reader = new FakeReader(Seq(batch(Seq(1L), Seq(Some(Seq(1f, 2f))))))
    val vectors =
      SegmentVectors.over(
        reader,
        "101",
        layout,
        exclusions(None),
        allocator,
        SegmentVectorsTest.NoJoin
      )
    try {
      val only = vectors.next().get
      only.close()
    } finally vectors.close()

    allocator.getAllocatedMemory shouldBe 0L
  }
}

object SegmentVectorsTest {

  /** A batch limit no batch can be under, so every call returns one reader
    * batch and the joining is out of the way.
    */
  private val NoJoin: Long = 1L
}
