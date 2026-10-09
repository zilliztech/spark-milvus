package com.zilliz.milvus.storage.index

import org.scalatest.funsuite.AnyFunSuite

/** What an index probe does with the parameters it is given. */
class IndexProbeTest extends AnyFunSuite {

  test("each index family is widened by its own parameter") {
    assert(IndexProbe.searchWidth("HNSW", 5, Map.empty).contains(64))
    assert(IndexProbe.searchWidth("HNSW", 100, Map.empty).contains(100))
    assert(IndexProbe.searchWidth("HNSW", 5, Map("ef" -> "128")).contains(128))
    assert(IndexProbe.searchWidth("IVF", 5, Map.empty).contains(16))
    assert(
      IndexProbe.searchWidth("IVF", 5, Map("nprobe" -> "64")).contains(64)
    )
    assert(IndexProbe.searchWidth("FLAT", 5, Map.empty).isEmpty)
    // A parameter of the other family, or one a flat index has no use for.
    intercept[IllegalArgumentException](
      IndexProbe.searchWidth("IVF", 5, Map("ef" -> "64"))
    )
    intercept[IllegalArgumentException](
      IndexProbe.searchWidth("HNSW", 5, Map("nprobe" -> "64"))
    )
    intercept[IllegalArgumentException](
      IndexProbe.searchWidth("FLAT", 5, Map("nprobe" -> "64"))
    )
    Seq("0", "-1", "1.5", "2147483648").foreach { value =>
      intercept[IllegalArgumentException](
        IndexProbe.searchWidth("IVF", 5, Map("nprobe" -> value))
      )
    }
  }

  test("unsupported search parameters cannot silently alter the query") {
    assert(IndexProbe.searchEf(5, Map.empty) == 64)
    assert(IndexProbe.searchEf(100, Map.empty) == 100)
    assert(IndexProbe.searchEf(5, Map("ef" -> "128")) == 128)
    Seq(
      Map("nprobe" -> "8"),
      Map("metric_type" -> "IP"),
      Map("ef" -> "0"),
      Map("ef" -> "4"),
      Map("ef" -> "-1"),
      Map("ef" -> "1.5"),
      Map("ef" -> "2147483648"),
      Map("ef" -> null)
    ).foreach { parameters =>
      intercept[IllegalArgumentException](
        IndexProbe.searchEf(5, parameters)
      )
    }
  }
}
