package com.zilliz.spark.connector.types

import scala.collection.JavaConverters._

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VarBinaryVector
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.milvus.grpc.schema.{LongArray, ScalarField, StringArray}

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
}
