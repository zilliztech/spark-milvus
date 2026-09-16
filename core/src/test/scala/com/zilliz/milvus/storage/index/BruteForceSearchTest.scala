package com.zilliz.milvus.storage.index

import org.scalatest.funsuite.AnyFunSuite

class BruteForceSearchTest extends AnyFunSuite {
  test(
    "merge keeps the best L2 hits across batches and copies only retained values"
  ) {
    val top = new BruteForceSearch.TopK[String](2, "L2")
    top.add(0, 5)("first")
    top.add(4, 2)("second")
    top.add(5, 9)(fail("A rejected row must not be copied"))
    top.add(7, 1)("last")
    assert(top.results.map(_.value) == Vector("last", "second"))
    assert(top.results.map(_.rowOffset) == Vector(7L, 4L))
  }

  test("IP and COSINE keep larger scores including negative values") {
    Seq("IP", "COSINE").foreach { metric =>
      val top = new BruteForceSearch.TopK[Int](2, metric)
      top.add(0, -2)(0)
      top.add(1, -3)(1)
      top.add(2, -1)(2)
      assert(top.results.map(_.value) == Vector(2, 0))
    }
  }

  test("equal returned scores are ordered by physical row offset") {
    val top = new BruteForceSearch.TopK[Int](2, "L2")
    top.add(8, 1)(8)
    top.add(4, 1)(4)
    top.add(6, 1)(6)
    assert(top.results.map(_.value) == Vector(4, 6))
  }

  test("invalid requests fail before native loading or allocation") {
    intercept[IllegalArgumentException](
      new BruteForceSearch[Int](Array.emptyFloatArray, 1, "L2")
    )
    intercept[IllegalArgumentException](
      new BruteForceSearch[Int](Array(Float.NaN), 1, "L2")
    )
    intercept[IllegalArgumentException](
      new BruteForceSearch[Int](Array(1f), 0, "L2")
    )
    intercept[IllegalArgumentException](
      new BruteForceSearch[Int](Array(1f), 1, "HAMMING")
    )
  }

  test("empty and excluded batches have no results and release their memory") {
    val search = new BruteForceSearch[Int](Array(1f, 0f), 3, "L2")
    try {
      search.addBatch(0)(_ => fail("empty batch"), _ => fail("empty batch"))
      search.addBatch(9)(
        _ => null,
        _ => fail("Excluded rows must not be copied")
      )
      assert(search.results.isEmpty)
      assert(search.allocatedBytes == 0)
    } finally search.close()
    search.close()
    intercept[IllegalArgumentException](search.results)
  }

  test(
    "invalid vectors fail with their physical row and release native buffers"
  ) {
    Seq(Array(1f), Array(Float.PositiveInfinity, 1f)).foreach { vector =>
      val search = new BruteForceSearch[Int](Array(0f, 0f), 1, "L2")
      try {
        search.addBatch(2)(_ => null, _ => fail("excluded"))
        val error = intercept[IllegalArgumentException] {
          search.addBatch(1)(_ => vector, _ => fail("invalid"))
        }
        assert(error.getMessage.contains("row 2"))
        assert(search.allocatedBytes == 0)
      } finally search.close()
    }
  }

  test("a vector conversion failure is propagated and buffers are released") {
    val search = new BruteForceSearch[Int](Array(0f), 1, "L2")
    val expected = new IllegalStateException("broken input batch")
    try {
      val error = intercept[IllegalStateException] {
        search.addBatch(1)(_ => throw expected, identity)
      }
      assert(error eq expected)
      assert(search.allocatedBytes == 0)
    } finally search.close()
  }
}
