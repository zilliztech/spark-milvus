package com.zilliz.spark.connector.read

import scala.collection.JavaConverters._

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{
  BigIntVector,
  FixedSizeBinaryVector,
  VectorSchemaRoot
}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.codec.FloatConverter
import com.zilliz.milvus.storage.read.exec.SegmentReader
import com.zilliz.milvus.storage.schema.FieldMetadata
import com.zilliz.spark.connector.MilvusOption
import io.milvus.grpc.schema.{CollectionSchema, DataType, FieldSchema}

/** The columnar outlet, decision 12's mechanism included.
  *
  * Driven by a fake [[SegmentReader]] rather than the native one, so the batch
  * shaping is tested on its own: whether a deleted row disappears and every
  * other value still lands in the right place does not depend on C.
  */
class MilvusColumnarPartitionReaderTest extends AnyFunSuite with Matchers {

  private val dimension = 2

  private val arrowSchema = new Schema(
    Seq(
      new Field(
        "id",
        new FieldType(false, new ArrowType.Int(64, true), null),
        java.util.Collections.emptyList[Field]()
      ),
      new Field(
        "Timestamp",
        new FieldType(false, new ArrowType.Int(64, true), null),
        java.util.Collections.emptyList[Field]()
      ),
      new Field(
        "vec",
        new FieldType(
          false,
          new ArrowType.FixedSizeBinary(dimension * 4),
          null
        ),
        java.util.Collections.emptyList[Field]()
      )
    ).asJava
  )

  private val milvusSchema = CollectionSchema(
    name = "t",
    fields = Seq(
      FieldSchema(
        fieldID = 100L,
        name = "id",
        dataType = DataType.Int64,
        isPrimaryKey = true
      ),
      FieldSchema(fieldID = 1L, name = "Timestamp", dataType = DataType.Int64),
      FieldSchema(fieldID = 101L, name = "vec", dataType = DataType.FloatVector)
    )
  )

  private def vectorMetadata: Metadata =
    new MetadataBuilder()
      .putLong(FieldMetadata.MilvusVectorDimensionMetadataKey, dimension.toLong)
      .build()

  private val sparkSchema = StructType(
    Seq(
      StructField("id", LongType, nullable = false),
      StructField("Timestamp", LongType, nullable = false),
      StructField(
        "vec",
        ArrayType(FloatType, containsNull = false),
        nullable = false,
        metadata = vectorMetadata
      )
    )
  )

  /** Hands over the roots it was given, then stops. */
  private class FakeSegmentReader(roots: Seq[VectorSchemaRoot])
      extends SegmentReader {
    private var remaining = roots
    private var closed = false
    def isClosed: Boolean = closed
    override def next(): Option[VectorSchemaRoot] = remaining match {
      case head :: tail => remaining = tail; Some(head)
      case Nil          => None
    }
    override def deliveredRows: Long = 0L
    override def close(): Unit = closed = true
  }

  private def buildRoot(
      allocator: RootAllocator,
      ids: Seq[Long],
      vectors: Seq[Seq[Float]]
  ): VectorSchemaRoot = {
    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    val id = root.getVector("id").asInstanceOf[BigIntVector]
    val ts = root.getVector("Timestamp").asInstanceOf[BigIntVector]
    val vec = root.getVector("vec").asInstanceOf[FixedSizeBinaryVector]
    id.allocateNew(ids.size)
    ts.allocateNew(ids.size)
    vec.allocateNew(ids.size)
    ids.zipWithIndex.foreach { case (value, i) =>
      id.setSafe(i, value)
      ts.setSafe(i, 100L + i)
      vec.set(i, vectors(i).flatMap(FloatConverter.toFloatBytes).toArray)
    }
    root.setRowCount(ids.size)
    root
  }

  private def reader(
      roots: Seq[VectorSchemaRoot],
      deleted: (VectorSchemaRoot, Int) => Boolean = (_, _) => false,
      schema: StructType = sparkSchema
  ) = new MilvusColumnarPartitionReader(
    schema,
    new FakeSegmentReader(roots.toList),
    milvusSchema,
    deleted,
    rawVectors = false,
    partitionName = "20",
    segmentId = 30L
  )

  test("a batch with no deletes carries every row") {
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val root = buildRoot(
        allocator,
        Seq(1L, 2L, 3L),
        Seq(Seq(1f, 2f), Seq(3f, 4f), Seq(5f, 6f))
      )
      val r = reader(Seq(root))
      try {
        r.next() shouldBe true
        val batch = r.get()
        batch.numRows() shouldBe 3
        batch.numCols() shouldBe 3
        (0 until 3).map(batch.column(0).getLong) shouldBe Seq(1L, 2L, 3L)
        val vec = batch.column(2)
        (0 until 2).map(vec.getArray(0).getFloat) shouldBe Seq(1f, 2f)
        (0 until 2).map(vec.getArray(2).getFloat) shouldBe Seq(5f, 6f)
        r.next() shouldBe false
      } finally r.close()
    } finally allocator.close()
  }

  // Decision 12: ColumnarBatch has no way to mark a row invalid, so the batch
  // delivered is the surviving rows and nothing else.
  test("a deleted row is gone and the rest keep their values") {
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val root = buildRoot(
        allocator,
        Seq(1L, 2L, 3L, 4L),
        Seq(Seq(1f, 1f), Seq(2f, 2f), Seq(3f, 3f), Seq(4f, 4f))
      )
      // Rows 1 and 2 are deleted, so 1 and 4 survive, in that order.
      val r = reader(Seq(root), deleted = (_, i) => i == 1 || i == 2)
      try {
        r.next() shouldBe true
        val batch = r.get()
        batch.numRows() shouldBe 2
        (0 until 2).map(batch.column(0).getLong) shouldBe Seq(1L, 4L)
        val vec = batch.column(2)
        (0 until 2).map(vec.getArray(0).getFloat) shouldBe Seq(1f, 1f)
        (0 until 2).map(vec.getArray(1).getFloat) shouldBe Seq(4f, 4f)
      } finally r.close()
    } finally allocator.close()
  }

  test("a batch whose rows are all deleted comes back empty, not skipped") {
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val root =
        buildRoot(allocator, Seq(1L, 2L), Seq(Seq(1f, 1f), Seq(2f, 2f)))
      val r = reader(Seq(root), deleted = (_, _) => true)
      try {
        r.next() shouldBe true
        r.get().numRows() shouldBe 0
      } finally r.close()
    } finally allocator.close()
  }

  test("the metadata columns come from the partition, not the data") {
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val schema = StructType(
        sparkSchema.fields.toSeq ++ Seq(
          StructField(MilvusOption.MilvusExtraColumnPartition, StringType),
          StructField(MilvusOption.MilvusExtraColumnSegmentID, LongType),
          StructField(MilvusOption.MilvusExtraColumnRowOffset, LongType)
        )
      )
      val first =
        buildRoot(allocator, Seq(1L, 2L), Seq(Seq(1f, 1f), Seq(2f, 2f)))
      val second = buildRoot(allocator, Seq(3L), Seq(Seq(3f, 3f)))
      val r = reader(Seq(first, second), schema = schema)
      try {
        r.next() shouldBe true
        val batch = r.get()
        batch.column(3).getUTF8String(0).toString shouldBe "20"
        batch.column(4).getLong(1) shouldBe 30L
        // Row offsets count from the start of the segment, not of the batch.
        (0 until 2).map(batch.column(5).getLong) shouldBe Seq(0L, 1L)

        r.next() shouldBe true
        r.get().column(5).getLong(0) shouldBe 2L
      } finally r.close()
    } finally allocator.close()
  }

  test("closing the reader closes the segment reader under it") {
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val root = buildRoot(allocator, Seq(1L), Seq(Seq(1f, 1f)))
      val fake = new FakeSegmentReader(List(root))
      val r = new MilvusColumnarPartitionReader(
        sparkSchema,
        fake,
        milvusSchema,
        (_, _) => false,
        rawVectors = false,
        partitionName = "20",
        segmentId = 30L
      )
      r.next() shouldBe true
      r.close()
      fake.isClosed shouldBe true
    } finally allocator.close()
  }

  test("a column the batch does not carry is an error, not a null column") {
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val root = buildRoot(allocator, Seq(1L), Seq(Seq(1f, 1f)))
      val schema = StructType(
        sparkSchema.fields.toSeq :+ StructField("absent", LongType)
      )
      val r = reader(Seq(root), schema = schema)
      try {
        val err = intercept[IllegalStateException](r.next())
        err.getMessage should include("absent")
      } finally r.close()
    } finally allocator.close()
  }
}
