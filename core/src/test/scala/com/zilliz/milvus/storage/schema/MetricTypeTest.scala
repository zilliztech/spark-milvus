package com.zilliz.milvus.storage.schema

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class MetricTypeTest extends AnyFunSuite with Matchers {

  test("a metric is read by the name Milvus records, in any letter case") {
    MetricType.fromName("L2") shouldBe Some(MetricType.L2)
    MetricType.fromName("ip") shouldBe Some(MetricType.IP)
    MetricType.fromName("Cosine") shouldBe Some(MetricType.Cosine)
    MetricType.fromName("HAMMING") shouldBe Some(MetricType.Hamming)
    MetricType.fromName("jaccard") shouldBe Some(MetricType.Jaccard)
  }

  test("a name the connector does not rank by is no metric") {
    Seq("EUCLIDEAN", "BM25", "MHJACCARD", "", " L2", null).foreach { name =>
      MetricType.fromName(name) shouldBe None
    }
  }

  test("a metric reads back as the name Knowhere takes") {
    MetricType.values.foreach { metric =>
      metric.toString shouldBe metric.name
      MetricType.fromName(metric.name) shouldBe Some(metric)
    }
    MetricType.values.map(_.name) shouldBe
      Seq("L2", "IP", "COSINE", "HAMMING", "JACCARD")
  }

  test("distances rank smallest first, similarities largest first") {
    MetricType.values.filter(_.smallerIsBetter) shouldBe
      Seq(MetricType.L2, MetricType.Hamming, MetricType.Jaccard)
  }

  test("bits are compared by Hamming or Jaccard, other elements by the rest") {
    MetricType.forElementType(VectorElementType.Bit) shouldBe
      Seq(MetricType.Hamming, MetricType.Jaccard)
    Seq(
      VectorElementType.Float32,
      VectorElementType.Float16,
      VectorElementType.BFloat16,
      VectorElementType.Int8
    ).foreach { elementType =>
      MetricType.forElementType(elementType) shouldBe
        Seq(MetricType.L2, MetricType.IP, MetricType.Cosine)
    }
  }
}
