package com.zilliz.milvus.storage.index

import scala.util.Random

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.schema.MetricType

/** The result semantics of vector-search.html section 2.6. */
class TopKMergerTest extends AnyFunSuite with Matchers {

  private def merger(metric: MetricType, queries: Int = 1, k: Int = 2) =
    new TopKMerger(queries, k, metric)

  private def places(candidates: Vector[Candidate]): Vector[(Long, Long)] =
    candidates.map(candidate => (candidate.segmentId, candidate.rowOffset))

  /** Every query's packed candidates, queries in order: what a task sends on.
    */
  private def packedPlaces(merged: TopKMerger): Vector[(Long, Long)] = {
    val read = Vector.newBuilder[(Long, Long)]
    (0 until merged.queries).foreach(query =>
      CandidateBytes.foreach(merged.takePacked(query))((segment, offset, _) =>
        read += ((segment, offset))
      )
    )
    read.result()
  }

  /** Packing a query empties its heap, so what it held is read once. */
  private def packedTwice(merged: TopKMerger): (Int, Int) =
    (merged.takePacked(0).length, merged.takePacked(0).length)

  test("packing every query over shards gives what packing one by one gives") {
    val random = new Random(7)
    val queries = 37
    val (sharded, oneByOne) =
      (merger(MetricType.L2, queries, 5), merger(MetricType.L2, queries, 5))
    (0 until 4000).foreach { _ =>
      val candidate = Candidate(
        random.nextInt(queries),
        100L + random.nextInt(3),
        random.nextInt(50000).toLong,
        random.nextDouble()
      )
      sharded.add(candidate)
      oneByOne.add(candidate)
    }
    val packed = sharded.packAll(4)
    packed.length shouldBe queries
    (0 until queries).foreach { query =>
      packed(query).toSeq shouldBe oneByOne.takePacked(query).toSeq
    }
    sharded.size shouldBe 0
    // A merger packed on one thread and one packed on more agree too.
    val single = merger(MetricType.Cosine, 3, 2)
    val many = merger(MetricType.Cosine, 3, 2)
    Seq(
      Candidate(0, 1L, 1L, 0.9),
      Candidate(0, 1L, 2L, 0.8),
      Candidate(2, 1L, 3L, 0.1),
      Candidate(2, 2L, 3L, 0.2),
      Candidate(2, 2L, 4L, 0.3)
    ).foreach { c => single.add(c); many.add(c) }
    single.packAll(1).map(_.toSeq).toSeq shouldBe many
      .packAll(3)
      .map(_.toSeq)
      .toSeq
  }

  test("L2 keeps the smallest scores, other metrics the largest") {
    val l2 = merger(MetricType.L2)
    val cosine = merger(MetricType.Cosine)
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
    val merged = merger(MetricType.L2, queries = 1, k = 3)
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
    val merged = merger(MetricType.L2, queries = 2, k = 1)
    merged.add(Candidate(0, 101L, 1L, 0.50))
    merged.add(Candidate(0, 101L, 2L, 0.40))
    merged.add(Candidate(1, 102L, 3L, 0.90))

    merged.size shouldBe 2
    places(merged.results(0)) shouldBe Vector((101L, 2L))
    places(merged.results(1)) shouldBe Vector((102L, 3L))
    packedPlaces(merged) shouldBe Vector((101L, 2L), (102L, 3L))
  }

  test("a query with fewer candidates than k returns all of them") {
    val merged = merger(MetricType.IP, queries = 2, k = 4)
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
    val straight =
      packedPlaces(new TopKMerger(3, 5, MetricType.L2).addAll(candidates))
    val shuffled =
      packedPlaces(
        new TopKMerger(3, 5, MetricType.L2).addAll(random.shuffle(candidates))
      )

    shuffled shouldBe straight
  }

  test("merging two mergers is the same as adding every candidate to one") {
    val candidates = (0 until 40).map(index =>
      Candidate(index % 2, (index % 3).toLong, index.toLong, index % 7 / 7.0)
    )
    val (left, right) = candidates.splitAt(17)
    val together =
      packedPlaces(new TopKMerger(2, 3, MetricType.Cosine).addAll(candidates))
    val apart = packedPlaces(
      new TopKMerger(2, 3, MetricType.Cosine)
        .addAll(left)
        .merge(new TopKMerger(2, 3, MetricType.Cosine).addAll(right))
    )

    apart shouldBe together
  }

  test("packing a query releases it, so the second read is empty") {
    val merged = merger(MetricType.L2, queries = 1, k = 2)
    merged.add(Candidate(0, 101L, 5L, 0.1))
    merged.size shouldBe 1
    packedTwice(merged) shouldBe ((CandidateBytes.Width, 0))
    merged.size shouldBe 0
  }

  test("mergers that rank differently do not merge") {
    val failure = the[IllegalArgumentException] thrownBy new TopKMerger(
      2,
      3,
      MetricType.L2
    ).merge(new TopKMerger(2, 3, MetricType.IP))

    failure.getMessage should include("same query count")
  }

  test("a candidate outside the query set is refused") {
    val failure = the[IllegalArgumentException] thrownBy merger(MetricType.L2)
      .add(Candidate(3, 1L, 1L, 0.1))

    failure.getMessage should include("query 3 of 1")
  }

  test("an unsupported metric is refused before a merger is built") {
    // A metric the search does not rank by has no MetricType, so no merger
    // can be built for it.
    MetricType.fromName("EUCLIDEAN") shouldBe None
  }

  test("k must be positive and the query count must not be negative") {
    the[IllegalArgumentException] thrownBy new TopKMerger(1, 0, MetricType.L2)
    the[IllegalArgumentException] thrownBy new TopKMerger(-1, 1, MetricType.L2)
  }
}
