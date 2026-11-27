package com.zilliz.spark.connector.types

import java.nio.{ByteBuffer, ByteOrder}
import scala.jdk.CollectionConverters._

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{
  BitVectorHelper,
  FixedSizeBinaryVector,
  IntVector,
  VarBinaryVector,
  VectorSchemaRoot
}
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.types.pojo.Schema
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.codec.ArrayEncoder
import com.zilliz.milvus.storage.schema.SchemaMapper
import io.milvus.grpc.common.KeyValuePair
import io.milvus.grpc.schema.{
  CollectionSchema,
  DataType,
  FieldSchema,
  StructArrayFieldSchema,
  TypeSchema
}

class MilvusStructArrayColumnTest extends AnyFunSuite with Matchers {

  private val text = FieldSchema(
    fieldID = 201,
    name = "items[text]",
    dataType = DataType.Array,
    elementType = DataType.VarChar,
    nullable = true
  )
  private val vector = FieldSchema(
    fieldID = 202,
    name = "items[embedding]",
    dataType = DataType.ArrayOfVector,
    elementType = DataType.FloatVector,
    typeParams = Seq(KeyValuePair("dim", "2")),
    nullable = true
  )
  private val inner = TypeSchema(nullable = true).withArrayElement(
    TypeSchema(nullable = true).withLeafType(DataType.Int32)
  )
  private val nested = FieldSchema(
    fieldID = 203,
    name = "items[values]",
    dataType = DataType.Array,
    elementType = DataType.Array,
    nullable = true,
    elementNullable = true,
    typeSchema = Some(TypeSchema(nullable = true).withArrayElement(inner))
  )
  private val parent = StructArrayFieldSchema(
    fieldID = 200,
    name = "items",
    fields = Seq(text, vector, nested),
    nullable = true
  )

  private def withBatch(parent: StructArrayFieldSchema, ids: Long*)(
      body: VectorSchemaRoot => Unit
  ): Unit = {
    val schema = SchemaMapper.convertToArrowSchemaWithFieldIdNames(
      CollectionSchema(structArrayFields = Seq(parent))
    )
    val fields =
      schema.getFields.asScala.filter(f => ids.contains(f.getName.toLong))
    val allocator = new RootAllocator()
    try {
      val root = VectorSchemaRoot.create(new Schema(fields.asJava), allocator)
      try {
        root.allocateNew()
        body(root)
      } finally root.close()
    } finally allocator.close()
  }

  private def open(
      root: VectorSchemaRoot,
      parent: StructArrayFieldSchema,
      raw: Boolean = false
  ) =
    MilvusStructArrayColumn(
      root,
      parent,
      SparkTypes.toStructArrayField(parent, raw),
      id => Some(id.toString),
      raw
    )

  private def floats(a: Float, b: Float): Array[Byte] =
    ByteBuffer
      .allocate(8)
      .order(ByteOrder.LITTLE_ENDIAN)
      .putFloat(a)
      .putFloat(b)
      .array()

  private def listRow(list: ListVector, row: Int, length: Int): Unit = {
    list.startNewValue(row)
    list.endValue(row, length)
  }

  test(
    "zip legacy scalar and vector members alongside native nested lists, preserving null and empty arrays"
  ) {
    withBatch(parent, 201, 202, 203) { root =>
      val labels = root.getVector("201").asInstanceOf[VarBinaryVector]
      labels.setNull(0)
      labels.setSafe(1, ArrayEncoder.encode(DataType.VarChar, Seq.empty))
      labels.setSafe(
        2,
        ArrayEncoder.encode(DataType.VarChar, Seq("a", "b", "c"))
      )
      val vectors = root.getVector("202").asInstanceOf[ListVector]
      vectors.setNull(0)
      listRow(vectors, 1, 0)
      listRow(vectors, 2, 3)
      val payload = vectors.getDataVector.asInstanceOf[FixedSizeBinaryVector]
      val bytes = floats(3f, 4f)
      payload.setSafe(0, floats(1f, 2f))
      payload.setSafe(1, bytes)
      payload.setSafe(2, floats(5f, 6f))
      val lists = root.getVector("203").asInstanceOf[ListVector]
      lists.setNull(0)
      listRow(lists, 1, 0)
      listRow(lists, 2, 3)
      val arrays = lists.getDataVector.asInstanceOf[ListVector]
      arrays.setNull(0)
      listRow(arrays, 1, 2)
      listRow(arrays, 2, 0)
      val ints = arrays.getDataVector.asInstanceOf[IntVector]
      ints.setSafe(0, 42)
      ints.setNull(1)
      root.setRowCount(3)

      Seq(false, true).foreach { raw =>
        val result = open(root, parent, raw)
        try {
          result.numNulls() shouldBe 1
          result.isNullAt(0) shouldBe true
          result.isNullAt(1) shouldBe false
          result.getArray(1).numElements() shouldBe 0
          val row = result.getArray(2)
          row.numElements() shouldBe 3
          val first = row.getStruct(0, 3)
          first.getUTF8String(0).toString shouldBe "a"
          first.isNullAt(2) shouldBe true
          val second = row.getStruct(1, 3)
          second.getUTF8String(0).toString shouldBe "b"
          if (raw) second.getBinary(1).toSeq shouldBe bytes.toSeq
          else second.getArray(1).toFloatArray.toSeq shouldBe Seq(3f, 4f)
          second.getArray(2).getInt(0) shouldBe 42
          second.getArray(2).isNullAt(1) shouldBe true
          row.getStruct(2, 3).getArray(2).numElements() shouldBe 0
        } finally result.close()
      }
    }
  }

  test(
    "unused vector slots under a null list do not shift the next struct's fields"
  ) {
    val p = parent.copy(fields = Seq(text, vector))
    withBatch(p, 201, 202) { root =>
      val labels = root.getVector("201").asInstanceOf[VarBinaryVector]
      labels.setNull(0)
      labels.setSafe(1, ArrayEncoder.encode(DataType.VarChar, Seq("next")))
      val vectors = root.getVector("202").asInstanceOf[ListVector]
      listRow(vectors, 0, 2)
      listRow(vectors, 1, 1)
      val payload = vectors.getDataVector.asInstanceOf[FixedSizeBinaryVector]
      payload.setNull(0)
      payload.setNull(1)
      payload.setSafe(2, floats(5f, 6f))
      root.setRowCount(2)
      BitVectorHelper.unsetBit(vectors.getValidityBuffer, 0)
      val result = open(root, p)
      try {
        result.isNullAt(0) shouldBe true
        val struct = result.getArray(1).getStruct(0, 2)
        struct.getUTF8String(0).toString shouldBe "next"
        struct.getArray(1).toFloatArray.toSeq shouldBe Seq(5f, 6f)
      } finally result.close()
    }
  }

  test(
    "element-nullable vectors keep logical slots in typed and raw views, including missing siblings"
  ) {
    val p =
      parent.copy(fields = Seq(vector.copy(elementNullable = true), nested))
    withBatch(p, 202) { root =>
      val lists = root.getVector("202").asInstanceOf[ListVector]
      listRow(lists, 0, 2)
      val binary = lists.getDataVector.asInstanceOf[VarBinaryVector]
      binary.setNull(0)
      val bytes = floats(-1f, 1f)
      binary.setSafe(1, bytes)
      root.setRowCount(1)
      Seq(false, true).foreach { raw =>
        val result = open(root, p, raw)
        try {
          val row = result.getArray(0)
          row.numElements() shouldBe 2
          row.getStruct(0, 2).isNullAt(0) shouldBe true
          row.getStruct(0, 2).isNullAt(1) shouldBe true
          row.getStruct(1, 2).isNullAt(1) shouldBe true
          if (raw) row.getStruct(1, 2).getBinary(0).toSeq shouldBe bytes.toSeq
          else
            row.getStruct(1, 2).getArray(0).toFloatArray.toSeq shouldBe Seq(
              -1f,
              1f
            )
        } finally result.close()
      }
      binary.setSafe(1, Array[Byte](1, 2))
      intercept[IllegalArgumentException](
        open(root, p, raw = true)
      ).getMessage should include("8 bytes")
    }
  }

  test(
    "an absent nullable parent is null for every row; an absent required member fails"
  ) {
    withBatch(parent, 0) { root =>
      root.setRowCount(3)
      val result = open(root, parent)
      try result.numNulls() shouldBe 3
      finally result.close()
      intercept[IllegalArgumentException](
        open(root, parent.copy(nullable = false))
      ).getMessage should include("non-nullable")
    }
    withBatch(parent, 201) { root =>
      root
        .getVector("201")
        .asInstanceOf[VarBinaryVector]
        .setSafe(0, ArrayEncoder.encode(DataType.VarChar, Seq("a")))
      root.setRowCount(1)
      intercept[IllegalArgumentException](
        open(root, parent)
      ).getMessage should include("embedding")
    }
  }

  test(
    "reject mismatched sibling lengths and validity before Spark reads any struct values"
  ) {
    val p = parent.copy(fields = Seq(text, vector))
    withBatch(p, 201, 202) { root =>
      val labels = root.getVector("201").asInstanceOf[VarBinaryVector]
      labels.setSafe(0, ArrayEncoder.encode(DataType.VarChar, Seq("a")))
      val vectors = root.getVector("202").asInstanceOf[ListVector]
      listRow(vectors, 0, 0)
      root.setRowCount(1)
      intercept[IllegalArgumentException](
        open(root, p)
      ).getMessage should include("length at row 0")
      vectors.setNull(0)
      intercept[IllegalArgumentException](
        open(root, p)
      ).getMessage should include("validity")
    }
  }
}
