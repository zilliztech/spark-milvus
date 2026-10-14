package com.zilliz.milvus.storage.index

import java.lang.{Double => JavaDouble}
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

  private def unpack(query: Int, packed: Array[Byte]): Vector[Candidate] = {
    val read = Vector.newBuilder[Candidate]
    CandidateBytes.foreach(packed)((segment, offset, score) =>
      read += Candidate(query, segment, offset, score)
    )
    read.result()
  }

  private def fields(
      candidates: Vector[Candidate]
  ): Vector[(Int, Long, Long, Long)] =
    candidates.map(candidate =>
      (
        candidate.query,
        candidate.segmentId,
        candidate.rowOffset,
        JavaDouble.doubleToRawLongBits(candidate.score)
      )
    )

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

  /** Packing can read the same sorted query more than once. */
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
    sharded.size shouldBe queries * 5
    sharded.packAll(3).map(_.toSeq).toSeq shouldBe packed.map(_.toSeq).toSeq
    (0 until queries).foreach { query =>
      fields(sharded.results(query)) shouldBe fields(
        unpack(query, packed(query))
      )
    }
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
    merged.size shouldBe 2
    places(merged.results(1)) shouldBe Vector((102L, 3L))
    merged.size shouldBe 2
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

  test("packing a query preserves its candidates for repeated output") {
    val merged = merger(MetricType.L2, queries = 1, k = 2)
    merged.add(Candidate(0, 101L, 5L, 0.1))
    merged.size shouldBe 1
    packedTwice(merged) shouldBe ((CandidateBytes.Width, CandidateBytes.Width))
    merged.size shouldBe 1
    merged.results(0) shouldBe Vector(Candidate(0, 101L, 5L, 0.1))
  }

  test("both output formats keep the complete metric order and score bits") {
    val negativeInfinity = Candidate(0, 9L, 0L, Double.NegativeInfinity)
    val negative = Candidate(0, 8L, 0L, -2.0)
    val negativeZero = Candidate(0, 7L, 0L, -0.0)
    val positiveZero = Candidate(0, 6L, 0L, 0.0)
    val tieFirst = Candidate(0, 1L, 2L, 1.0)
    val tieSecond = Candidate(0, 1L, 3L, 1.0)
    val tieThird = Candidate(0, 2L, 0L, 1.0)
    val positiveInfinity = Candidate(0, 5L, 0L, Double.PositiveInfinity)
    val nan = Candidate(0, 4L, 0L, Double.NaN)
    // Explicit expected order does not call the production comparator or a
    // candidate encoder that would sort the output again.
    val ascending = Vector(
      negativeInfinity,
      negative,
      negativeZero,
      positiveZero,
      tieFirst,
      tieSecond,
      tieThird,
      positiveInfinity,
      nan
    )
    val descending = Vector(
      nan,
      positiveInfinity,
      tieFirst,
      tieSecond,
      tieThird,
      positiveZero,
      negativeZero,
      negative,
      negativeInfinity
    )
    val random = new Random(19L)
    for {
      metric <- Seq(MetricType.L2, MetricType.IP, MetricType.Cosine)
      k <- Seq(1, 4, ascending.size, ascending.size + 2)
    } withClue(s"metric=$metric k=$k: ") {
      val candidates = random.shuffle(ascending)
      val objects = merger(metric, k = k).addAll(candidates)
      val bytes = merger(metric, k = k).addAll(candidates)
      val expected =
        (if (metric == MetricType.L2) ascending else descending).take(k)

      fields(objects.results(0)) shouldBe fields(expected)
      fields(unpack(0, bytes.takePacked(0))) shouldBe fields(expected)
      fields(unpack(0, objects.takePacked(0))) shouldBe fields(expected)
      fields(bytes.results(0)) shouldBe fields(expected)
      fields(objects.results(0)) shouldBe fields(expected)
      fields(unpack(0, bytes.takePacked(0))) shouldBe fields(expected)
      objects.size shouldBe expected.size
      bytes.size shouldBe expected.size
    }
  }

  test("output fixes one query's order while other queries can still collect") {
    val merged = merger(MetricType.L2, queries = 2, k = 3)
    merged.countScanned(17)
    val first = Vector(
      Candidate(0, 1L, 1L, 1.0),
      Candidate(0, 1L, 2L, 2.0),
      Candidate(0, 1L, 3L, 3.0)
    )
    val other = Candidate(1, 2L, 4L, 0.5)
    merged.addAll(first.reverse)
    merged.add(other)
    merged.isFull(0) shouldBe true
    merged.worstScore(0) shouldBe 3.0
    merged.rejectsScore(0, 100.0) shouldBe true
    merged.rejectsScore(0, 2.0) shouldBe false

    val retained = merged.results(0)
    retained shouldBe first
    merged.size shouldBe 4
    merged.isFull(0) shouldBe true
    merged.worstScore(0) shouldBe 3.0
    merged.rejectsScore(0, 100.0) shouldBe true
    merged.rejectsScore(0, 2.0) shouldBe false
    merged.results(0) shouldBe first
    unpack(0, merged.takePacked(0)) shouldBe first
    merged.scannedRows shouldBe 17L
    val kept = Vector.newBuilder[(Long, Long)]
    merged.foreachKept((segment, offset) => kept += ((segment, offset)))
    kept.result() shouldBe places(first :+ other)
    the[IllegalStateException] thrownBy merged.add(Candidate(0, 3L, 7L, 0.0))

    val next = Vector(
      other,
      Candidate(1, 3L, 7L, 4.0),
      Candidate(1, 3L, 8L, 5.0)
    )
    merged.addAll(next.tail.reverse)
    merged.add(Candidate(1, 3L, 9L, 6.0))
    merged.worstScore(1) shouldBe 5.0
    unpack(1, merged.takePacked(1)) shouldBe next
    retained shouldBe first
    merged.results(0) shouldBe first
    merged.results(1) shouldBe next
    merged.size shouldBe 6
    merged.scannedRows shouldBe 17L
  }

  test("mutating packed output changes neither the merger nor another output") {
    val merged = merger(MetricType.IP, queries = 2, k = 3)
    val first = Vector(
      Candidate(1, 1L, 1L, 8.0),
      Candidate(1, 1L, 2L, 7.0),
      Candidate(1, 1L, 3L, 6.0)
    )
    merged.addAll(first.reverse)
    val packed = merged.takePacked(1)
    val retained = packed.clone()
    val another = merged.takePacked(1)
    val objects = merged.results(1)
    packed(0) = (packed(0) ^ 0xff).toByte

    another.toSeq shouldBe retained.toSeq
    merged.takePacked(1).toSeq shouldBe retained.toSeq
    objects shouldBe first
    merged.results(1) shouldBe first
    merged.worstScore(1) shouldBe 6.0
    merged.rejectsScore(1, 1.0) shouldBe true
    merged.rejectsScore(1, 7.0) shouldBe false
    the[IllegalStateException] thrownBy merged.add(Candidate(1, 2L, 0L, 9.0))
    merged.size shouldBe 3
  }

  test(
    "empty output is repeatable and also closes its query to new candidates"
  ) {
    Seq(false, true).foreach { packedFirst =>
      val merged = merger(MetricType.L2, queries = 2)
      if (packedFirst) merged.takePacked(0) shouldBe empty
      else merged.results(0) shouldBe empty
      merged.results(0) shouldBe empty
      merged.takePacked(0) shouldBe empty
      the[IllegalStateException] thrownBy merged.add(Candidate(0, 1L, 1L, 1.0))
      merged.size shouldBe 0
      val other = Candidate(1, 2L, 2L, 2.0)
      merged.add(other)
      merged.results(1) shouldBe Vector(other)
      merged.results(0) shouldBe empty
      merged.size shouldBe 1
    }
  }

  test("both output methods reject a query outside the allocated range") {
    val merged = merger(MetricType.L2, queries = 2)
    Seq(-1, 2).foreach { query =>
      the[IllegalArgumentException] thrownBy merged.results(query)
      the[IllegalArgumentException] thrownBy merged.takePacked(query)
    }
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
