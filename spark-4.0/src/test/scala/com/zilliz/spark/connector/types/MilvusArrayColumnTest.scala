package com.zilliz.spark.connector.types

import scala.collection.JavaConverters._

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VarBinaryVector
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.milvus.grpc.schema.{
  FloatArray,
  IntArray,
  LongArray,
  ScalarField,
  StringArray
}

/** A Milvus Array column arrives as one serialized ScalarField per row; the row
  * path and the columnar path must both give the elements back.
  */
class MilvusArrayColumnTest extends AnyFunSuite with Matchers {

  private def longs(values: Long*): Array[Byte] =
    ScalarField(data =
      ScalarField.Data.LongData(LongArray(data = values))
    ).toByteArray

  private def strings(values: String*): Array[Byte] =
    ScalarField(data =
      ScalarField.Data.StringData(StringArray(data = values))
    ).toByteArray

  private def withVector(rows: Seq[Option[Array[Byte]]])(
      body: VarBinaryVector => Unit
  ): Unit = {
    val allocator = new RootAllocator(Long.MaxValue)
    val vector = new VarBinaryVector("arr", allocator)
    try {
      vector.allocateNew()
      rows.zipWithIndex.foreach {
        case (Some(bytes), i) => vector.setSafe(i, bytes)
        case (None, i)        => vector.setNull(i)
      }
      vector.setValueCount(rows.size)
      body(vector)
    } finally {
      vector.close()
      allocator.close()
    }
  }

  test("the columnar column decodes every row once and answers getArray") {
    withVector(
      Seq(Some(longs(1L, 2L, 3L)), None, Some(longs()), Some(longs(9L)))
    ) { v =>
      val column = new MilvusArrayColumn(v, LongType)
      column.isNullAt(1) shouldBe true
      column.numNulls shouldBe 1
      column.getArray(0).toLongArray.toSeq shouldBe Seq(1L, 2L, 3L)
      column.getArray(2).numElements shouldBe 0
      column.getArray(3).toLongArray.toSeq shouldBe Seq(9L)
      column.close()
    }
  }

  test("string elements come back as UTF8String") {
    withVector(Seq(Some(strings("a", "bc")))) { v =>
      val column = new MilvusArrayColumn(v, StringType)
      val arr = column.getArray(0)
      arr.numElements shouldBe 2
      arr.getUTF8String(1).toString shouldBe "bc"
      column.close()
    }
  }

  test("the row path decodes the same bytes") {
    withVector(Seq(Some(longs(4L, 5L)))) { v =>
      val data = ArrowConverter
        .arrowValueToSparkValue(v, 0, ArrayType(LongType))
        .asInstanceOf[org.apache.spark.sql.catalyst.util.ArrayData]
      data.toLongArray.toSeq shouldBe Seq(4L, 5L)
    }
  }

  test("a Spark element type Milvus arrays cannot hold is refused") {
    withVector(Seq(Some(longs(1L)))) { v =>
      an[IllegalArgumentException] should be thrownBy
        new MilvusArrayColumn(v, DecimalType(10, 2))
    }
  }

  private def floats(values: Float*): Array[Byte] =
    ScalarField(data =
      ScalarField.Data.FloatData(FloatArray(data = values))
    ).toByteArray

  private def ints(values: Int*): Array[Byte] =
    ScalarField(data =
      ScalarField.Data.IntData(IntArray(data = values))
    ).toByteArray

  // The row path took an Array<Float/Int8/Int16> value for a binary-backed
  // vector because both are VarBinary, and failed.
  test(
    "the row path decodes Float and Int8/Int16 element arrays like the columnar path"
  ) {
    import org.apache.arrow.vector.VectorSchemaRoot
    import com.zilliz.milvus.storage.schema.FieldMetadata
    import com.zilliz.spark.connector.types.ArrowConverter
    import io.milvus.grpc.schema.{DataType => MilvusDataType}
    val arrayMetadata = new MetadataBuilder()
      .putLong(
        FieldMetadata.MilvusDataTypeMetadataKey,
        MilvusDataType.Array.value
      )
      .build()
    def check(
        rows: Seq[Option[Array[Byte]]],
        elementType: DataType
    )(
        expect: (Int, org.apache.spark.sql.catalyst.util.ArrayData) => Unit
    ): Unit =
      withVector(rows) { v =>
        val root = new VectorSchemaRoot(
          java.util.Arrays.asList(v.getField),
          java.util.Arrays.asList[org.apache.arrow.vector.FieldVector](v),
          rows.size
        )
        val schema = StructType(
          Seq(StructField("arr", ArrayType(elementType), true, arrayMetadata))
        )
        val column = new MilvusArrayColumn(v, elementType)
        rows.indices.foreach { i =>
          val row = ArrowConverter.arrowToInternalRow(root, i, schema)
          if (rows(i).isEmpty) {
            row.isNullAt(0) shouldBe true
            column.isNullAt(i) shouldBe true
          } else {
            expect(i, row.getArray(0))
            expect(i, column.getArray(i))
          }
        }
        column.close()
      }
    check(
      Seq(Some(floats(1.5f, -2f)), Some(floats()), None),
      FloatType
    ) {
      case (0, a) => a.toFloatArray.toSeq shouldBe Seq(1.5f, -2f)
      case (_, a) => a.numElements shouldBe 0
    }
    check(Seq(Some(ints(-128, 127)), Some(ints()), None), ShortType) {
      case (0, a) => a.toShortArray.toSeq shouldBe Seq[Short](-128, 127)
      case (_, a) => a.numElements shouldBe 0
    }
  }
}
