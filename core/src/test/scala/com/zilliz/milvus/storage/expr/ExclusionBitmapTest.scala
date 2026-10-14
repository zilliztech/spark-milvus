package com.zilliz.milvus.storage.expr

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ExclusionBitmapTest extends AnyFunSuite with Matchers {

  test("or combines exclusion bits without changing either input") {
    val left = ExclusionBitmap.fromWords(70, Array(1L, 0L))
    val right = ExclusionBitmap.fromWords(70, Array(2L, 1L << 5))

    val combined = left.or(right)

    combined.excludedCount shouldBe 3
    combined.isExcluded(0) shouldBe true
    combined.isExcluded(1) shouldBe true
    combined.isExcluded(69) shouldBe true
    left.excludedCount shouldBe 1
    right.excludedCount shouldBe 2
  }

  test("empty bitmap validates row indexes and OR sizes") {
    val empty = ExclusionBitmap.empty(3)
    empty.isEmpty shouldBe true
    empty.isFull shouldBe false

    intercept[IndexOutOfBoundsException](empty.isExcluded(-1))
    intercept[IndexOutOfBoundsException](empty.isExcluded(3))
    intercept[IllegalArgumentException](empty.or(ExclusionBitmap.empty(2)))
  }

  test("bitmap equality compares row count and bits") {
    ExclusionBitmap.empty(2) shouldBe ExclusionBitmap.empty(2)
    ExclusionBitmap.empty(2) should not be ExclusionBitmap.empty(3)
    ExclusionBitmap.fromWords(2, Array(1L)) should not be ExclusionBitmap.empty(
      2
    )
  }
}
