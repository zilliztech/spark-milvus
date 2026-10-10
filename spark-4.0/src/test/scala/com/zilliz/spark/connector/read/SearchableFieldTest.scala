package com.zilliz.spark.connector.read

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.schema.{
  MetricType,
  VectorElementType,
  VectorLayout
}

/** Which fields a search takes and by which metrics, checked before the
  * snapshot's segments are planned.
  */
class SearchableFieldTest extends AnyFunSuite with Matchers {

  test("a float, float16, bfloat16 or int8 field takes L2, IP or COSINE") {
    Seq(
      VectorElementType.Float32,
      VectorElementType.Float16,
      VectorElementType.BFloat16,
      VectorElementType.Int8
    ).foreach { elementType =>
      val layout = VectorLayout(elementType, 8)
      Seq(MetricType.L2, MetricType.IP, MetricType.Cosine).foreach(
        MilvusSearch.checkSearchable(_, layout)
      )
      val failure = the[IllegalArgumentException] thrownBy MilvusSearch
        .checkSearchable(MetricType.Hamming, layout)
      failure.getMessage should include("COSINE or IP or L2, not HAMMING")
    }
  }

  test("a binary vector field is not searched, whatever the metric") {
    val binary = VectorLayout(VectorElementType.Bit, 16)

    Seq(MetricType.Hamming, MetricType.Jaccard, MetricType.L2).foreach {
      metric =>
        val failure = the[IllegalArgumentException] thrownBy MilvusSearch
          .checkSearchable(metric, binary)
        failure.getMessage should include("binary vector field is not searched")
    }
  }
}
