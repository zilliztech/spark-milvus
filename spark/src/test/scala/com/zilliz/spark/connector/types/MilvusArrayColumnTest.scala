package com.zilliz.spark.connector.types

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VarBinaryVector
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarArray
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.milvus.grpc.schema.{
  FloatArray,
  IntArray,
  LongArray,
  ScalarField,
  StringArray
}

/** A Milvus Array stores one serialized ScalarField per row. */
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

  test("Float and Int8/Int16 arrays preserve values, empty arrays and nulls") {
    def check(
        rows: Seq[Option[Array[Byte]]],
        elementType: DataType
    )(expect: ColumnarArray => Unit): Unit =
      withVector(rows) { v =>
        val column = new MilvusArrayColumn(v, elementType)
        try {
          expect(column.getArray(0))
          column.getArray(1).numElements shouldBe 0
          column.isNullAt(2) shouldBe true
        } finally column.close()
      }
    check(Seq(Some(floats(1.5f, -2f)), Some(floats()), None), FloatType) {
      _.toFloatArray.toSeq shouldBe Seq(1.5f, -2f)
    }
    check(Seq(Some(ints(-128, 127)), Some(ints()), None), ShortType) {
      _.toShortArray.toSeq shouldBe Seq[Short](-128, 127)
    }
  }
}
