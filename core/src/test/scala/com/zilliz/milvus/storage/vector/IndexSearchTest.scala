package com.zilliz.milvus.storage.vector

import org.scalatest.funsuite.AnyFunSuite

/** What an indexed search does with the parameters it is given. */
class IndexSearchTest extends AnyFunSuite {

  test("each index family is widened by its own parameter") {
    assert(IndexSearch.searchWidth("HNSW", 5, Map.empty).contains(64))
    assert(IndexSearch.searchWidth("HNSW", 100, Map.empty).contains(100))
    assert(IndexSearch.searchWidth("HNSW", 5, Map("ef" -> "128")).contains(128))
    assert(IndexSearch.searchWidth("IVF", 5, Map.empty).contains(16))
    assert(
      IndexSearch.searchWidth("IVF", 5, Map("nprobe" -> "64")).contains(64)
    )
    assert(IndexSearch.searchWidth("FLAT", 5, Map.empty).isEmpty)
    // A parameter of the other family, or one a flat index has no use for.
    intercept[IllegalArgumentException](
      IndexSearch.searchWidth("IVF", 5, Map("ef" -> "64"))
    )
    intercept[IllegalArgumentException](
      IndexSearch.searchWidth("HNSW", 5, Map("nprobe" -> "64"))
    )
    intercept[IllegalArgumentException](
      IndexSearch.searchWidth("FLAT", 5, Map("nprobe" -> "64"))
    )
    Seq("0", "-1", "1.5", "2147483648").foreach { value =>
      intercept[IllegalArgumentException](
        IndexSearch.searchWidth("IVF", 5, Map("nprobe" -> value))
      )
    }
  }

  test("unsupported search parameters cannot silently alter the query") {
    assert(IndexSearch.searchEf(5, Map.empty) == 64)
    assert(IndexSearch.searchEf(100, Map.empty) == 100)
    assert(IndexSearch.searchEf(5, Map("ef" -> "128")) == 128)
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
        IndexSearch.searchEf(5, parameters)
      )
    }
  }
}
