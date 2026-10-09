package com.zilliz.spark.connector.read

import scala.collection.JavaConverters._

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{
  BigIntVector,
  FixedSizeBinaryVector,
  VarBinaryVector,
  VectorSchemaRoot
}
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.codec.FloatConverter
import com.zilliz.milvus.storage.expr.{
  Comparison,
  ComparisonOperator,
  Expr,
  FieldRef,
  PredicateExpr
}
import com.zilliz.milvus.storage.expr.Literal.IntegerValue
import com.zilliz.milvus.storage.read.exec.{ReadMetrics, SegmentReader}
import com.zilliz.milvus.storage.schema.{FieldMetadata, SchemaMapper}
import com.zilliz.spark.connector.options.MilvusOption
import io.milvus.grpc.common.KeyValuePair
import io.milvus.grpc.schema.{CollectionSchema, DataType, FieldSchema}

/** The columnar outlet, decision 12's mechanism included.
  *
  * Driven by a fake [[SegmentReader]] rather than the native one, so the batch
  * shaping is tested on its own: whether a deleted row disappears and every
  * other value still lands in the right place does not depend on C.
  *
  * Every batch is built from a schema [[SchemaMapper]] produced, never a
  * hand-written one. A hand-written Arrow schema names its columns after the
  * Spark fields and picks a physical type by eye, and both of those hid a
  * defect: the manifest line names columns by field id, and a nullable dense
  * vector is stored as VarBinary rather than FixedSizeBinary.
  */
class MilvusColumnarPartitionReaderTest extends AnyFunSuite with Matchers {

  private val dimension = 2

  private def dimParam = Seq(KeyValuePair("dim", dimension.toString))

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
      FieldSchema(
        fieldID = 101L,
        name = "vec",
        dataType = DataType.FloatVector,
        typeParams = dimParam
      )
    )
  )

  /** The same collection with the vector nullable, which is what moves its
    * physical type from FixedSizeBinary to VarBinary.
    */
  private val nullableVectorSchema =
    milvusSchema.copy(fields = milvusSchema.fields.map {
      case f if f.name == "vec" => f.copy(nullable = true)
      case f                    => f
    })

  private val jsonSchema = CollectionSchema(
    name = "t",
    fields = Seq(
      FieldSchema(
        fieldID = 100L,
        name = "id",
        dataType = DataType.Int64,
        isPrimaryKey = true
      ),
      FieldSchema(fieldID = 1L, name = "Timestamp", dataType = DataType.Int64),
      FieldSchema(fieldID = 102L, name = "meta", dataType = DataType.JSON)
    )
  )

  // The column-group line keeps the field's own name; the manifest line uses
  // the field id. Both come straight from the mapper the readers use.
  private def namedSchema(schema: CollectionSchema): Schema =
    SchemaMapper.convertToArrowSchema(schema)

  private def fieldIdSchema(schema: CollectionSchema): Schema =
    SchemaMapper.convertToArrowSchemaWithFieldIdNames(schema)

  private def byFieldId(schema: CollectionSchema): String => String = {
    val map = schema.fields.map(f => f.name -> f.fieldID.toString).toMap ++
      Map("RowID" -> "0", "Timestamp" -> "1")
    name => map.getOrElse(name, name)
  }

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

  private val jsonSparkSchema = StructType(
    Seq(
      StructField("id", LongType, nullable = false),
      StructField("Timestamp", LongType, nullable = false),
      StructField("meta", StringType)
    )
  )

  /** Hands over the roots it was given, then stops. */
  private class FakeSegmentReader(roots: Seq[VectorSchemaRoot])
      extends SegmentReader {
    override def metrics: ReadMetrics = ReadMetrics.Zero
    private var remaining = roots
    private var closed = false
    def isClosed: Boolean = closed
    override def next(): Option[VectorSchemaRoot] = remaining match {
      case head :: tail => remaining = tail; Some(head)
      case Nil          => None
    }
    override def deliveredRows: Long = 0L
    override def take(
        rowIndices: Array[Long],
        columns: Seq[String],
        parallelism: Int
    ): SegmentReader.TakeResult =
      throw new UnsupportedOperationException(
        "this sequential batch fixture does not support take"
      )
    override def close(): Unit = closed = true
  }

  private class TrackingOwner extends AutoCloseable {
    var closes = 0
    override def close(): Unit = closes += 1
  }

  private def floatBytes(values: Seq[Float]): Array[Byte] =
    values.flatMap(FloatConverter.toFloatBytes).toArray

  /** A batch of the non-nullable collection, in whichever naming `arrowSchema`
    * uses.
    */
  private def buildRoot(
      allocator: RootAllocator,
      arrowSchema: Schema,
      idColumn: String,
      tsColumn: String,
      vecColumn: String,
      ids: Seq[Long],
      vectors: Seq[Seq[Float]]
  ): VectorSchemaRoot = {
    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    val id = root.getVector(idColumn).asInstanceOf[BigIntVector]
    val ts = root.getVector(tsColumn).asInstanceOf[BigIntVector]
    val vec = root.getVector(vecColumn).asInstanceOf[FixedSizeBinaryVector]
    id.allocateNew(ids.size)
    ts.allocateNew(ids.size)
    vec.allocateNew(ids.size)
    ids.zipWithIndex.foreach { case (value, i) =>
      id.setSafe(i, value)
      ts.setSafe(i, 100L + i)
      vec.set(i, floatBytes(vectors(i)))
    }
    root.setRowCount(ids.size)
    root
  }

  /** The column-group naming: `id`, `Timestamp`, `vec`. */
  private def namedRoot(
      allocator: RootAllocator,
      ids: Seq[Long],
      vectors: Seq[Seq[Float]]
  ): VectorSchemaRoot =
    buildRoot(
      allocator,
      namedSchema(milvusSchema),
      "id",
      "Timestamp",
      "vec",
      ids,
      vectors
    )

  private def reader(
      roots: Seq[VectorSchemaRoot],
      deleted: (VectorSchemaRoot, Int) => Boolean = (_, _) => false,
      schema: StructType = sparkSchema,
      arrowColumnFor: String => String = identity,
      collection: CollectionSchema = milvusSchema,
      rawVectors: Boolean = false,
      requestedExtraColumns: Set[String] = Set.empty,
      pushedExpression: Option[PredicateExpr] = None,
      milvusFilter: Option[Expr] = None,
      columnNameFor: Long => Option[String] = (_: Long) => None
  ) = new MilvusColumnarPartitionReader(
    schema,
    new FakeSegmentReader(roots.toList),
    collection,
    deleted,
    arrowColumnFor,
    rawVectors = rawVectors,
    partitionName = "20",
    segmentId = 30L,
    requestedExtraColumns = requestedExtraColumns,
    pushedExpression = pushedExpression,
    milvusFilter = milvusFilter,
    columnNameFor = columnNameFor
  )

  test("a batch with no deletes carries every row") {
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val root = namedRoot(
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

  // The manifest line's Arrow columns are called 100, 1 and 101. Looking a
  // Spark field name up directly finds nothing and the first batch throws.
  test("the manifest line reads columns named by field id") {
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val root = buildRoot(
        allocator,
        fieldIdSchema(milvusSchema),
        "100",
        "1",
        "101",
        Seq(7L, 8L),
        Seq(Seq(1f, 2f), Seq(3f, 4f))
      )
      val r = reader(Seq(root), arrowColumnFor = byFieldId(milvusSchema))
      try {
        r.next() shouldBe true
        val batch = r.get()
        (0 until 2).map(batch.column(0).getLong) shouldBe Seq(7L, 8L)
        (0 until 2).map(batch.column(2).getArray(1).getFloat) shouldBe
          Seq(3f, 4f)
      } finally r.close()
    } finally allocator.close()
  }

  // A nullable dense vector is Arrow VarBinary, not FixedSizeBinary. Casting
  // the vector to FixedSizeBinaryVector throws before any accessor runs.
  test("a nullable dense vector is read from its variable-width column") {
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val arrowSchema = namedSchema(nullableVectorSchema)
      arrowSchema
        .findField("vec")
        .getType shouldBe a[org.apache.arrow.vector.types.pojo.ArrowType.Binary]

      val root = VectorSchemaRoot.create(arrowSchema, allocator)
      val id = root.getVector("id").asInstanceOf[BigIntVector]
      val ts = root.getVector("Timestamp").asInstanceOf[BigIntVector]
      val vec = root.getVector("vec").asInstanceOf[VarBinaryVector]
      id.allocateNew(3)
      ts.allocateNew(3)
      vec.allocateNew(3)
      Seq(1L, 2L, 3L).zipWithIndex.foreach { case (value, i) =>
        id.setSafe(i, value)
        ts.setSafe(i, 100L + i)
      }
      // A present vector, a null, and a present vector after the null, so the
      // offsets have to be followed rather than assumed uniform.
      vec.setSafe(0, floatBytes(Seq(1f, 2f)))
      vec.setNull(1)
      vec.setSafe(2, floatBytes(Seq(5f, 6f)))
      root.setRowCount(3)

      val schema = StructType(
        sparkSchema.fields.toSeq.map {
          case f if f.name == "vec" => f.copy(nullable = true)
          case f                    => f
        }
      )
      val r =
        reader(Seq(root), schema = schema, collection = nullableVectorSchema)
      try {
        r.next() shouldBe true
        val column = r.get().column(2)
        column.isNullAt(0) shouldBe false
        column.isNullAt(1) shouldBe true
        column.isNullAt(2) shouldBe false
        column.numNulls shouldBe 1
        (0 until 2).map(column.getArray(0).getFloat) shouldBe Seq(1f, 2f)
        (0 until 2).map(column.getArray(2).getFloat) shouldBe Seq(5f, 6f)
      } finally r.close()
    } finally allocator.close()
  }

  test("a nullable dense vector whose row is the wrong width is refused") {
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val root =
        VectorSchemaRoot.create(namedSchema(nullableVectorSchema), allocator)
      val id = root.getVector("id").asInstanceOf[BigIntVector]
      val ts = root.getVector("Timestamp").asInstanceOf[BigIntVector]
      val vec = root.getVector("vec").asInstanceOf[VarBinaryVector]
      id.allocateNew(1)
      ts.allocateNew(1)
      vec.allocateNew(1)
      id.setSafe(0, 1L)
      ts.setSafe(0, 100L)
      // Three floats where the schema says two.
      vec.setSafe(0, floatBytes(Seq(1f, 2f, 3f)))
      root.setRowCount(1)

      val schema = StructType(sparkSchema.fields.toSeq.map {
        case f if f.name == "vec" => f.copy(nullable = true)
        case f                    => f
      })
      val r =
        reader(Seq(root), schema = schema, collection = nullableVectorSchema)
      try {
        r.next() shouldBe true
        val err =
          intercept[IllegalArgumentException](r.get().column(2).getArray(0))
        err.getMessage should include("dimension 2")
      } finally r.close()
    } finally allocator.close()
  }

  // JSON is StringType to Spark and Binary on disk. Spark's ArrowColumnVector
  // picks its accessor from the physical type, so it answers getBinary only.
  test("a JSON column is read as a string") {
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val root = VectorSchemaRoot.create(namedSchema(jsonSchema), allocator)
      val id = root.getVector("id").asInstanceOf[BigIntVector]
      val ts = root.getVector("Timestamp").asInstanceOf[BigIntVector]
      val meta = root.getVector("meta").asInstanceOf[VarBinaryVector]
      id.allocateNew(4)
      ts.allocateNew(4)
      meta.allocateNew(4)
      (0 until 4).foreach { i =>
        id.setSafe(i, i.toLong)
        ts.setSafe(i, 100L + i)
      }
      meta.setSafe(0, """{"a":1}""".getBytes("UTF-8"))
      meta.setNull(1)
      meta.setSafe(2, "".getBytes("UTF-8"))
      meta.setSafe(3, """{"名":"值"}""".getBytes("UTF-8"))
      root.setRowCount(4)

      val r = reader(
        Seq(root),
        schema = jsonSparkSchema,
        collection = jsonSchema
      )
      try {
        r.next() shouldBe true
        val column = r.get().column(2)
        column.getUTF8String(0).toString shouldBe """{"a":1}"""
        column.isNullAt(1) shouldBe true
        column.getUTF8String(2).toString shouldBe ""
        column.getUTF8String(3).toString shouldBe """{"名":"值"}"""
      } finally r.close()
    } finally allocator.close()
  }

  test("a JSON value that is not UTF-8 is refused, not returned garbled") {
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val root = VectorSchemaRoot.create(namedSchema(jsonSchema), allocator)
      val id = root.getVector("id").asInstanceOf[BigIntVector]
      val ts = root.getVector("Timestamp").asInstanceOf[BigIntVector]
      val meta = root.getVector("meta").asInstanceOf[VarBinaryVector]
      id.allocateNew(1)
      ts.allocateNew(1)
      meta.allocateNew(1)
      id.setSafe(0, 1L)
      ts.setSafe(0, 100L)
      meta.setSafe(0, Array[Byte](0xc3.toByte, 0x28.toByte))
      root.setRowCount(1)

      val r =
        reader(Seq(root), schema = jsonSparkSchema, collection = jsonSchema)
      try {
        r.next() shouldBe true
        val err =
          intercept[IllegalArgumentException](
            r.get().column(2).getUTF8String(0)
          )
        err.getMessage should include("UTF-8")
      } finally r.close()
    } finally allocator.close()
  }

  // Decision 12: ColumnarBatch has no way to mark a row invalid, so the batch
  // delivered is the surviving rows and nothing else.
  test("a deleted row is gone and the rest keep their values") {
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val root = namedRoot(
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

  test("Milvus filter, Spark predicate and deletes combine before projection") {
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val root = namedRoot(
        allocator,
        Seq(1L, 2L, 3L, 4L, 5L),
        Seq(
          Seq(1f, 1f),
          Seq(2f, 2f),
          Seq(3f, 3f),
          Seq(4f, 4f),
          Seq(5f, 5f)
        )
      )
      val expression = Comparison(
        FieldRef(100L, DataType.Int64),
        ComparisonOperator.LessThanOrEqual,
        IntegerValue(4L)
      )
      val outputSchema = StructType(Seq(sparkSchema("vec")))
      val r = reader(
        Seq(root),
        deleted = (_, row) => row == 2,
        schema = outputSchema,
        pushedExpression = Some(expression),
        milvusFilter = Some(Expr.Compare("id", ">=", 2L)),
        columnNameFor = id => if (id == 100L) Some("id") else None
      )
      try {
        r.next() shouldBe true
        val batch = r.get()
        batch.numCols() shouldBe 1
        batch.numRows() shouldBe 2
        (0 until 2).map(batch.column(0).getArray(0).getFloat) shouldBe
          Seq(2f, 2f)
        (0 until 2).map(batch.column(0).getArray(1).getFloat) shouldBe
          Seq(4f, 4f)
      } finally r.close()
    } finally allocator.close()
  }

  test("predicate binding uses field-id column names on the manifest line") {
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val root = buildRoot(
        allocator,
        fieldIdSchema(milvusSchema),
        "100",
        "1",
        "101",
        Seq(1L, 2L, 3L),
        Seq(Seq(1f, 1f), Seq(2f, 2f), Seq(3f, 3f))
      )
      val expression = Comparison(
        FieldRef(100L, DataType.Int64),
        ComparisonOperator.GreaterThanOrEqual,
        IntegerValue(2L)
      )
      val r = reader(
        Seq(root),
        arrowColumnFor = byFieldId(milvusSchema),
        pushedExpression = Some(expression),
        milvusFilter = Some(Expr.Compare("id", "<=", 2L)),
        columnNameFor = id => Some(id.toString)
      )
      try {
        r.next() shouldBe true
        val batch = r.get()
        batch.numRows() shouldBe 1
        batch.column(0).getLong(0) shouldBe 2L
      } finally r.close()
    } finally allocator.close()
  }

  test("a Milvus filter whose physical column was not read fails") {
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val root = namedRoot(allocator, Seq(1L), Seq(Seq(1f, 1f)))
      val r = reader(
        Seq(root),
        arrowColumnFor = name => if (name == "id") "missing-id" else name,
        milvusFilter = Some(Expr.Compare("id", ">=", 1L))
      )
      try {
        val error = intercept[IllegalStateException](r.next())
        error.getMessage should include("id")
        error.getMessage should include("missing-id")
      } finally r.close()
    } finally allocator.close()
  }

  test("a batch whose rows are all deleted comes back empty, not skipped") {
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val root =
        namedRoot(allocator, Seq(1L, 2L), Seq(Seq(1f, 1f), Seq(2f, 2f)))
      val r = reader(Seq(root), deleted = (_, _) => true)
      try {
        r.next() shouldBe true
        r.get().numRows() shouldBe 0
      } finally r.close()
    } finally allocator.close()
  }

  test("metadata columns combine read position with the stored timestamp") {
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val schema = StructType(
        sparkSchema.fields.toSeq ++ Seq(
          StructField(MilvusOption.MilvusExtraColumnSegmentID, LongType),
          StructField(MilvusOption.MilvusExtraColumnRowOffset, LongType),
          StructField(MilvusOption.MilvusExtraColumnTimestamp, LongType)
        )
      )
      val first =
        namedRoot(allocator, Seq(1L, 2L), Seq(Seq(1f, 1f), Seq(2f, 2f)))
      val second = namedRoot(allocator, Seq(3L), Seq(Seq(3f, 3f)))
      val r = reader(
        Seq(first, second),
        schema = schema,
        arrowColumnFor = name =>
          if (name == MilvusOption.MilvusExtraColumnTimestamp) "Timestamp"
          else name,
        requestedExtraColumns = Set(
          MilvusOption.MilvusExtraColumnSegmentID,
          MilvusOption.MilvusExtraColumnRowOffset,
          MilvusOption.MilvusExtraColumnTimestamp
        )
      )
      try {
        r.next() shouldBe true
        val batch = r.get()
        batch.column(3).getLong(1) shouldBe 30L
        // Row offsets count from the start of the segment, not of the batch.
        (0 until 2).map(batch.column(4).getLong) shouldBe Seq(0L, 1L)
        (0 until 2).map(batch.column(5).getLong) shouldBe Seq(100L, 101L)

        r.next() shouldBe true
        r.get().column(4).getLong(0) shouldBe 2L
        r.get().column(5).getLong(0) shouldBe 100L
      } finally r.close()
    } finally allocator.close()
  }

  test("a real field with a metadata-like name is not synthesized") {
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val collection = CollectionSchema(
        name = "t",
        fields = Seq(
          FieldSchema(
            fieldID = 102L,
            name = MilvusOption.MilvusExtraColumnSegmentID,
            dataType = DataType.Int64
          )
        )
      )
      val root = VectorSchemaRoot.create(namedSchema(collection), allocator)
      val values = root
        .getVector(MilvusOption.MilvusExtraColumnSegmentID)
        .asInstanceOf[BigIntVector]
      values.allocateNew(1)
      values.setSafe(0, 77L)
      root.setRowCount(1)
      val r = reader(
        Seq(root),
        schema = StructType(
          Seq(
            StructField(
              MilvusOption.MilvusExtraColumnSegmentID,
              LongType,
              nullable = false
            )
          )
        ),
        collection = collection
      )
      try {
        r.next() shouldBe true
        r.get().column(0).getLong(0) shouldBe 77L
      } finally r.close()
    } finally allocator.close()
  }

  test("closing the reader closes the segment reader under it") {
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val root = namedRoot(allocator, Seq(1L), Seq(Seq(1f, 1f)))
      val fake = new FakeSegmentReader(List(root))
      val owner = new TrackingOwner
      val r = new MilvusColumnarPartitionReader(
        sparkSchema,
        fake,
        milvusSchema,
        (_, _) => false,
        identity,
        rawVectors = false,
        partitionName = "20",
        segmentId = 30L,
        taskAllocatorOwner = Some(owner)
      )
      r.next() shouldBe true
      r.close()
      r.close()
      fake.isClosed shouldBe true
      owner.closes shouldBe 1
    } finally allocator.close()
  }

  test("a column the batch does not carry is an error, not a null column") {
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val root = namedRoot(allocator, Seq(1L), Seq(Seq(1f, 1f)))
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
