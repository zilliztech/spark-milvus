package com.zilliz.milvus.storage.index

import org.scalatest.funsuite.AnyFunSuite

/** What a search does with the parameters it is given. */
class PersistedIndexSearchTest extends AnyFunSuite {

  test("unsupported search parameters cannot silently alter the query") {
    assert(PersistedIndexSearch.searchEf(5, Map.empty) == 64)
    assert(PersistedIndexSearch.searchEf(100, Map.empty) == 100)
    assert(PersistedIndexSearch.searchEf(5, Map("ef" -> "128")) == 128)
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
        PersistedIndexSearch.searchEf(5, parameters)
      )
    }
  }
}
