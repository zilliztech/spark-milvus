package com.zilliz.milvus.storage.index

import scala.util.Random

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** The result semantics of vector-search.html section 2.6. */
class TopKMergerTest extends AnyFunSuite with Matchers {

  private def merger(metric: String, queries: Int = 1, k: Int = 2) =
    new TopKMerger(queries, k, metric)

  private def places(candidates: Vector[Candidate]): Vector[(Long, Long)] =
    candidates.map(candidate => (candidate.segmentId, candidate.rowOffset))

  test("L2 keeps the smallest scores, other metrics the largest") {
    val l2 = merger("L2")
    val cosine = merger("COSINE")
    val candidates = Seq(
      Candidate(0, 101L, 5L, 0.10),
      Candidate(0, 101L, 9L, 0.30),
      Candidate(0, 102L, 7L, 0.20)
    )
    candidates.foreach(l2.add)
    candidates.foreach(cosine.add)

    places(l2.results(0)) shouldBe Vector((101L, 5L), (102L, 7L))
    places(cosine.results(0)) shouldBe Vector((101L, 9L), (102L, 7L))
  }

  test("an equal score is broken by segment id, then by row offset") {
    val merged = merger("L2", queries = 1, k = 3)
    Seq(
      Candidate(0, 102L, 7L, 0.10),
      Candidate(0, 101L, 9L, 0.10),
      Candidate(0, 101L, 5L, 0.10)
    ).foreach(merged.add)

    places(merged.results(0)) shouldBe Vector(
      (101L, 5L),
      (101L, 9L),
      (102L, 7L)
    )
  }

  test("each query keeps its own k") {
    val merged = merger("L2", queries = 2, k = 1)
    merged.add(Candidate(0, 101L, 1L, 0.50))
    merged.add(Candidate(0, 101L, 2L, 0.40))
    merged.add(Candidate(1, 102L, 3L, 0.90))

    merged.size shouldBe 2
    places(merged.results(0)) shouldBe Vector((101L, 2L))
    places(merged.results(1)) shouldBe Vector((102L, 3L))
    places(merged.candidates) shouldBe Vector((101L, 2L), (102L, 3L))
  }

  test("a query with fewer candidates than k returns all of them") {
    val merged = merger("IP", queries = 2, k = 4)
    merged.add(Candidate(1, 7L, 3L, 2.0))

    merged.results(0) shouldBe empty
    merged.results(1) should have size 1
  }

  test("the order candidates arrive in does not change the result") {
    val random = new Random(20260918L)
    val candidates = (0 until 200).map(index =>
      Candidate(
        index % 3,
        random.nextInt(4).toLong,
        random.nextInt(50).toLong,
        random.nextInt(10) / 10.0
      )
    )
    val straight = new TopKMerger(3, 5, "L2").addAll(candidates).candidates
    val shuffled =
      new TopKMerger(3, 5, "L2").addAll(random.shuffle(candidates)).candidates

    shuffled shouldBe straight
  }

  test("merging two mergers is the same as adding every candidate to one") {
    val candidates = (0 until 40).map(index =>
      Candidate(index % 2, (index % 3).toLong, index.toLong, index % 7 / 7.0)
    )
    val (left, right) = candidates.splitAt(17)
    val together = new TopKMerger(2, 3, "COSINE").addAll(candidates).candidates
    val apart = new TopKMerger(2, 3, "COSINE")
      .addAll(left)
      .merge(new TopKMerger(2, 3, "COSINE").addAll(right))
      .candidates

    apart shouldBe together
  }

  test("mergers that rank differently do not merge") {
    val failure = the[IllegalArgumentException] thrownBy new TopKMerger(
      2,
      3,
      "L2"
    ).merge(new TopKMerger(2, 3, "IP"))

    failure.getMessage should include("same query count")
  }

  test("a candidate outside the query set is refused") {
    val failure = the[IllegalArgumentException] thrownBy merger("L2")
      .add(Candidate(3, 1L, 1L, 0.1))

    failure.getMessage should include("query 3 of 1")
  }

  test("an unsupported metric is refused when the merger is built") {
    the[IllegalArgumentException] thrownBy new TopKMerger(
      1,
      1,
      "EUCLIDEAN"
    ) should have message "requirement failed: Unsupported vector search metric: EUCLIDEAN"
  }

  test("k must be positive and the query count must not be negative") {
    the[IllegalArgumentException] thrownBy new TopKMerger(1, 0, "L2")
    the[IllegalArgumentException] thrownBy new TopKMerger(-1, 1, "L2")
  }
}
