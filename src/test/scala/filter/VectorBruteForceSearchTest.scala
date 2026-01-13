package com.zilliz.spark.connector.filter

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.apache.spark.ml.linalg.{DenseVector, Vectors}

import VectorBruteForceSearch.DistanceType._

/**
 * Unit tests for VectorBruteForceSearch distance calculations
 */
class VectorBruteForceSearchTest extends AnyFunSuite with Matchers {

  // Tolerance for floating point comparisons
  val epsilon = 1e-6

  // ============ L2 Distance Tests ============

  test("L2 distance between identical vectors is 0") {
    val v1 = Vectors.dense(1.0, 2.0, 3.0)
    val v2 = Vectors.dense(1.0, 2.0, 3.0)

    val distance = VectorBruteForceSearch.calculateDistance(v1, v2, L2)

    distance shouldBe 0.0 +- epsilon
  }

  test("L2 distance calculation is correct") {
    val v1 = Vectors.dense(0.0, 0.0, 0.0)
    val v2 = Vectors.dense(3.0, 4.0, 0.0)

    val distance = VectorBruteForceSearch.calculateDistance(v1, v2, L2)

    // sqrt(3^2 + 4^2 + 0^2) = sqrt(9 + 16) = sqrt(25) = 5
    distance shouldBe 5.0 +- epsilon
  }

  test("L2 distance is symmetric") {
    val v1 = Vectors.dense(1.0, 2.0, 3.0)
    val v2 = Vectors.dense(4.0, 5.0, 6.0)

    val d1 = VectorBruteForceSearch.calculateDistance(v1, v2, L2)
    val d2 = VectorBruteForceSearch.calculateDistance(v2, v1, L2)

    d1 shouldBe d2 +- epsilon
  }

  test("L2 distance with negative values") {
    val v1 = Vectors.dense(-1.0, -2.0, -3.0)
    val v2 = Vectors.dense(1.0, 2.0, 3.0)

    val distance = VectorBruteForceSearch.calculateDistance(v1, v2, L2)

    // sqrt((1-(-1))^2 + (2-(-2))^2 + (3-(-3))^2) = sqrt(4 + 16 + 36) = sqrt(56)
    distance shouldBe math.sqrt(56.0) +- epsilon
  }

  // ============ Cosine Similarity Tests ============

  test("Cosine similarity of identical vectors is 1") {
    val v1 = Vectors.dense(1.0, 2.0, 3.0)
    val v2 = Vectors.dense(1.0, 2.0, 3.0)

    val similarity = VectorBruteForceSearch.calculateDistance(v1, v2, COSINE)

    similarity shouldBe 1.0 +- epsilon
  }

  test("Cosine similarity of proportional vectors is 1") {
    val v1 = Vectors.dense(1.0, 2.0, 3.0)
    val v2 = Vectors.dense(2.0, 4.0, 6.0)

    val similarity = VectorBruteForceSearch.calculateDistance(v1, v2, COSINE)

    similarity shouldBe 1.0 +- epsilon
  }

  test("Cosine similarity of orthogonal vectors is 0") {
    val v1 = Vectors.dense(1.0, 0.0, 0.0)
    val v2 = Vectors.dense(0.0, 1.0, 0.0)

    val similarity = VectorBruteForceSearch.calculateDistance(v1, v2, COSINE)

    similarity shouldBe 0.0 +- epsilon
  }

  test("Cosine similarity of opposite vectors is -1") {
    val v1 = Vectors.dense(1.0, 2.0, 3.0)
    val v2 = Vectors.dense(-1.0, -2.0, -3.0)

    val similarity = VectorBruteForceSearch.calculateDistance(v1, v2, COSINE)

    similarity shouldBe -1.0 +- epsilon
  }

  test("Cosine similarity is symmetric") {
    val v1 = Vectors.dense(1.0, 2.0, 3.0)
    val v2 = Vectors.dense(4.0, 5.0, 6.0)

    val s1 = VectorBruteForceSearch.calculateDistance(v1, v2, COSINE)
    val s2 = VectorBruteForceSearch.calculateDistance(v2, v1, COSINE)

    s1 shouldBe s2 +- epsilon
  }

  test("Cosine similarity with zero vector returns 0") {
    val v1 = Vectors.dense(1.0, 2.0, 3.0)
    val v2 = Vectors.dense(0.0, 0.0, 0.0)

    val similarity = VectorBruteForceSearch.calculateDistance(v1, v2, COSINE)

    similarity shouldBe 0.0
  }

  // ============ Inner Product Tests ============

  test("Inner product of orthogonal vectors is 0") {
    val v1 = Vectors.dense(1.0, 0.0, 0.0)
    val v2 = Vectors.dense(0.0, 1.0, 0.0)

    val ip = VectorBruteForceSearch.calculateDistance(v1, v2, IP)

    ip shouldBe 0.0 +- epsilon
  }

  test("Inner product calculation is correct") {
    val v1 = Vectors.dense(1.0, 2.0, 3.0)
    val v2 = Vectors.dense(4.0, 5.0, 6.0)

    val ip = VectorBruteForceSearch.calculateDistance(v1, v2, IP)

    // 1*4 + 2*5 + 3*6 = 4 + 10 + 18 = 32
    ip shouldBe 32.0 +- epsilon
  }

  test("Inner product is symmetric") {
    val v1 = Vectors.dense(1.0, 2.0, 3.0)
    val v2 = Vectors.dense(4.0, 5.0, 6.0)

    val ip1 = VectorBruteForceSearch.calculateDistance(v1, v2, IP)
    val ip2 = VectorBruteForceSearch.calculateDistance(v2, v1, IP)

    ip1 shouldBe ip2 +- epsilon
  }

  test("Inner product with zero vector is 0") {
    val v1 = Vectors.dense(1.0, 2.0, 3.0)
    val v2 = Vectors.dense(0.0, 0.0, 0.0)

    val ip = VectorBruteForceSearch.calculateDistance(v1, v2, IP)

    ip shouldBe 0.0 +- epsilon
  }

  test("Inner product with negative values") {
    val v1 = Vectors.dense(1.0, -2.0, 3.0)
    val v2 = Vectors.dense(-1.0, 2.0, -3.0)

    val ip = VectorBruteForceSearch.calculateDistance(v1, v2, IP)

    // 1*(-1) + (-2)*2 + 3*(-3) = -1 - 4 - 9 = -14
    ip shouldBe -14.0 +- epsilon
  }

  // ============ Edge Cases ============

  test("Distance calculation with single element vectors") {
    val v1 = Vectors.dense(5.0)
    val v2 = Vectors.dense(3.0)

    val l2 = VectorBruteForceSearch.calculateDistance(v1, v2, L2)
    val ip = VectorBruteForceSearch.calculateDistance(v1, v2, IP)
    val cosine = VectorBruteForceSearch.calculateDistance(v1, v2, COSINE)

    l2 shouldBe 2.0 +- epsilon
    ip shouldBe 15.0 +- epsilon
    cosine shouldBe 1.0 +- epsilon  // Same direction, magnitude doesn't matter
  }

  test("Distance calculation with high dimensional vectors") {
    val dim = 1000
    val v1 = Vectors.dense(Array.fill(dim)(1.0))
    val v2 = Vectors.dense(Array.fill(dim)(1.0))

    val l2 = VectorBruteForceSearch.calculateDistance(v1, v2, L2)
    val cosine = VectorBruteForceSearch.calculateDistance(v1, v2, COSINE)

    l2 shouldBe 0.0 +- epsilon
    cosine shouldBe 1.0 +- epsilon
  }

  test("Unsupported distance type for dense vectors throws exception") {
    val v1 = Vectors.dense(1.0, 2.0, 3.0)
    val v2 = Vectors.dense(4.0, 5.0, 6.0)

    an[IllegalArgumentException] should be thrownBy {
      VectorBruteForceSearch.calculateDistance(v1, v2, HAMMING)
    }
  }

  // ============ DistanceType Enumeration Tests ============

  test("DistanceType enumeration contains expected values") {
    VectorBruteForceSearch.DistanceType.values should contain allOf (
      COSINE, L2, IP, HAMMING, JACCARD, DEFAULT
    )
  }

  // ============ SearchType Enumeration Tests ============

  test("SearchType enumeration contains expected values") {
    import VectorBruteForceSearch.SearchType._
    VectorBruteForceSearch.SearchType.values should contain allOf (KNN, RANGE)
  }

  // ============ VectorType Enumeration Tests ============

  test("VectorType enumeration contains expected values") {
    import VectorBruteForceSearch.VectorType._
    VectorBruteForceSearch.VectorType.values should contain allOf (DENSE_FLOAT, BINARY, SPARSE)
  }

  // ============ Precision Tests ============

  test("L2 distance preserves precision for small differences") {
    val v1 = Vectors.dense(1.0, 1.0, 1.0)
    val v2 = Vectors.dense(1.0001, 1.0001, 1.0001)

    val distance = VectorBruteForceSearch.calculateDistance(v1, v2, L2)

    // sqrt(3 * 0.0001^2) = sqrt(0.00000003) ~ 0.000173
    distance should be > 0.0
    distance should be < 0.001
  }

  test("Cosine similarity handles very small vectors") {
    val v1 = Vectors.dense(0.0001, 0.0001, 0.0001)
    val v2 = Vectors.dense(0.0002, 0.0002, 0.0002)

    val similarity = VectorBruteForceSearch.calculateDistance(v1, v2, COSINE)

    // Proportional vectors should have similarity 1
    similarity shouldBe 1.0 +- epsilon
  }

  // ============ Known Mathematical Properties Tests ============

  test("L2 distance satisfies triangle inequality") {
    val v1 = Vectors.dense(0.0, 0.0, 0.0)
    val v2 = Vectors.dense(1.0, 1.0, 1.0)
    val v3 = Vectors.dense(2.0, 2.0, 2.0)

    val d12 = VectorBruteForceSearch.calculateDistance(v1, v2, L2)
    val d23 = VectorBruteForceSearch.calculateDistance(v2, v3, L2)
    val d13 = VectorBruteForceSearch.calculateDistance(v1, v3, L2)

    // d(v1, v3) <= d(v1, v2) + d(v2, v3)
    d13 should be <= (d12 + d23 + epsilon)
  }

  test("Cosine similarity is bounded between -1 and 1") {
    val vectors = Seq(
      Vectors.dense(1.0, 2.0, 3.0),
      Vectors.dense(-1.0, -2.0, -3.0),
      Vectors.dense(0.5, -0.5, 0.0),
      Vectors.dense(100.0, 200.0, 300.0)
    )

    for (v1 <- vectors; v2 <- vectors) {
      if (Vectors.norm(v1, 2.0) > 0 && Vectors.norm(v2, 2.0) > 0) {
        val similarity = VectorBruteForceSearch.calculateDistance(v1, v2, COSINE)
        similarity should be >= -1.0 - epsilon
        similarity should be <= 1.0 + epsilon
      }
    }
  }
}
