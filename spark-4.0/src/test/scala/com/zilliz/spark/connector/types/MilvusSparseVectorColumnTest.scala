package com.zilliz.spark.connector.types

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VarBinaryVector
import org.apache.spark.sql.types.{FloatType, LongType, MapType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.codec.SparseFloatVectorConverter

/** Checked against `SparseFloatVectorConverter`, the decoder the row path uses,
  * so the two paths cannot read the same bytes differently.
  */
class MilvusSparseVectorColumnTest extends AnyFunSuite with Matchers {

  private def withVector(rows: Seq[Array[Byte]])(
      body: VarBinaryVector => Unit
  ): Unit = {
    val allocator = new RootAllocator(Long.MaxValue)
    val vector = new VarBinaryVector("sparse", allocator)
    try {
      vector.allocateNew()
      rows.zipWithIndex.foreach { case (bytes, i) => vector.setSafe(i, bytes) }
      vector.setValueCount(rows.size)
      body(vector)
    } finally {
      vector.close()
      allocator.close()
    }
  }

  private def entriesOf(
      column: org.apache.spark.sql.vectorized.ColumnVector,
      rowId: Int
  ): Seq[(Long, Float)] = {
    val map = column.getMap(rowId)
    val keys = map.keyArray()
    val values = map.valueArray()
    (0 until map.numElements()).map(i => (keys.getLong(i), values.getFloat(i)))
  }

  test("a sparse vector reads back as the entries it was encoded from") {
    // Milvus requires the values be finite and non-negative.
    val entries = Seq(3L -> 1.5f, 17L -> 2.25f, 900L -> 0.5f)
    val bytes =
      SparseFloatVectorConverter.encodeSparseFloatVectorEntries(entries)
    withVector(Seq(bytes)) { vector =>
      val column = MilvusSparseVectorColumn(vector)
      column.dataType shouldBe MapType(
        LongType,
        FloatType,
        valueContainsNull = false
      )
      entriesOf(column, 0) shouldBe
        SparseFloatVectorConverter.decodeSparseFloatVector(bytes)
      entriesOf(column, 0) shouldBe entries
    }
  }

  // Rows sit end to end in one Arrow buffer, so a wrong window would read a
  // neighbour's entries and still look like a valid vector.
  test("each row sees only its own entries") {
    val first = Seq(1L -> 1.0f, 2L -> 2.0f)
    val second = Seq(10L -> 10.0f)
    val third = Seq(20L -> 20.0f, 21L -> 21.0f, 22L -> 22.0f)
    val rows = Seq(first, second, third).map(
      SparseFloatVectorConverter.encodeSparseFloatVectorEntries
    )
    withVector(rows) { vector =>
      val column = MilvusSparseVectorColumn(vector)
      entriesOf(column, 0) shouldBe first
      entriesOf(column, 1) shouldBe second
      entriesOf(column, 2) shouldBe third
    }
  }

  test("an empty sparse vector is an empty map, not a null one") {
    withVector(Seq(Array.emptyByteArray)) { vector =>
      val column = MilvusSparseVectorColumn(vector)
      column.isNullAt(0) shouldBe false
      column.getMap(0).numElements() shouldBe 0
    }
  }

  // The stored index is unsigned; read as a signed Int it would come back
  // negative and the entry would be lost.
  test("an index above Int.MaxValue survives") {
    val big = 0xfffffff0L
    val bytes = SparseFloatVectorConverter.encodeSparseFloatVectorEntries(
      Seq(big -> 7.5f)
    )
    withVector(Seq(bytes)) { vector =>
      entriesOf(MilvusSparseVectorColumn(vector), 0) shouldBe Seq(big -> 7.5f)
    }
  }

  test("a null row is null and its neighbour still reads") {
    val allocator = new RootAllocator(Long.MaxValue)
    val vector = new VarBinaryVector("sparse", allocator)
    try {
      vector.allocateNew()
      vector.setNull(0)
      vector.setSafe(
        1,
        SparseFloatVectorConverter.encodeSparseFloatVectorEntries(
          Seq(5L -> 5.5f)
        )
      )
      vector.setValueCount(2)
      val column = MilvusSparseVectorColumn(vector)
      column.hasNull shouldBe true
      column.isNullAt(0) shouldBe true
      entriesOf(column, 1) shouldBe Seq(5L -> 5.5f)
    } finally {
      vector.close()
      allocator.close()
    }
  }

  // Eight bytes an entry is the format. Rounding down would produce a vector
  // that looks plausible and is wrong.
  test("a row that is not a whole number of entries is refused") {
    withVector(Seq(Array[Byte](1, 2, 3, 4, 5))) { vector =>
      val err = intercept[IllegalStateException] {
        MilvusSparseVectorColumn(vector).getMap(0)
      }
      err.getMessage should include("not a multiple of 8")
    }
  }
}
