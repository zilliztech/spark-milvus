package com.zilliz.spark.connector.extensions

import java.lang.{Float => JavaFloat}

import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.schema.MetricType

/** The Float boundary precedes candidate selection, including the squared
  * distance scale used by L2. These checks need no native engine or storage.
  */
class VectorRankingTest extends AnyFunSuite with Matchers {
  test("IP and cosine keep the function's Float, NULL and signed zero") {
    for {
      metric <- Seq(MetricType.IP, MetricType.Cosine)
      result <- Seq[JavaFloat](
        null,
        -0.0f,
        0.0f,
        Float.NaN,
        Float.NegativeInfinity,
        Float.PositiveInfinity,
        1.0000001f
      )
    } {
      val ranking = new VectorRanking(metric, "constant", queryFirst = true) {
        override protected def value(
            first: ArrayData,
            second: ArrayData,
            name: UTF8String
        ): JavaFloat = result
      }
      val score = ranking.score(Array(0f), Array(0f))
      (score eq result) shouldBe true
    }
  }

  test("L2 rounds its double square to Float before ranking") {
    val ranking = ConnectorVectorRanking(
      MetricType.L2,
      "vector_l2_distance",
      queryFirst = true
    )
    ranking.score(Array(0f, 0f), Array(3f, 4f)) shouldBe 25f
    Seq(2e20f, 3e20f).foreach { distance =>
      ranking.score(Array(0f), Array(distance)) shouldBe Float.PositiveInfinity
    }
    Seq(1e-30f, 2e-30f, -0.0f, 0.0f).foreach { distance =>
      val score = ranking.score(Array(0f), Array(distance)).floatValue()
      JavaFloat.floatToRawIntBits(score) shouldBe 0
    }
    ranking.score(Array(0f), Array(Float.NaN)).isNaN shouldBe true
  }
}
