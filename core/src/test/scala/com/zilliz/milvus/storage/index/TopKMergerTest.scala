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
      CandidateBytes.foreach(merged.sort(query).takePacked(query))(
        (segment, offset, _) => read += ((segment, offset))
      )
    )
    read.result()
  }

  /** Packing can read the same sorted query more than once. */
  private def packedTwice(merged: TopKMerger): (Int, Int) = {
    merged.sort(0)
    (merged.takePacked(0).length, merged.takePacked(0).length)
  }

  test(
    "sorting and packing query shards gives what doing them one by one gives"
  ) {
    def pack(merged: TopKMerger, threads: Int): Array[Array[Byte]] = {
      val packed = new Array[Array[Byte]](merged.queries)
      SearchThreads.shards(merged.queries, threads) { (_, from, until) =>
        var query = from
        while (query < until) {
          packed(query) = merged.sort(query).takePacked(query)
          query += 1
        }
      }
      packed
    }

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
    val packed = pack(sharded, 4)
    packed.length shouldBe queries
    (0 until queries).foreach { query =>
      packed(query).toSeq shouldBe oneByOne.sort(query).takePacked(query).toSeq
    }
    sharded.size shouldBe queries * 5
    pack(sharded, 3).map(_.toSeq).toSeq shouldBe packed.map(_.toSeq).toSeq
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
    pack(single, 1).map(_.toSeq).toSeq shouldBe pack(many, 3).map(_.toSeq).toSeq
  }

  test("ordinary reads preserve the heap and allow collection to continue") {
    val merged = merger(MetricType.L2, k = 3)
    val initial = Vector(
      Candidate(0, 1L, 1L, 1.0),
      Candidate(0, 1L, 3L, 3.0),
      Candidate(0, 1L, 2L, 2.0)
    )
    merged.addAll(initial)
    merged.countScanned(11)
    val snapshot = merged.results(0)
    snapshot.toSet shouldBe initial.toSet
    snapshot.head.score shouldBe 3.0
    merged.results(0) shouldBe snapshot
    merged.worstScore(0) shouldBe 3.0
    merged.rejectsScore(0, 4.0) shouldBe true

    val best = Candidate(0, 2L, 4L, 0.0)
    merged.add(best)
    val collected = merged.results(0)
    collected.toSet shouldBe Set(best, initial(0), initial(2))
    collected.head.score shouldBe 2.0
    merged.worstScore(0) shouldBe 2.0
    merged.rejectsScore(0, 3.0) shouldBe true
    merged.rejectsScore(0, 1.5) shouldBe false
    snapshot.toSet shouldBe initial.toSet
    snapshot.head.score shouldBe 3.0

    merged.sort(0) should be theSameInstanceAs merged
    val expected = Vector(best, initial(0), initial(2))
    merged.results(0) shouldBe expected
    merged.sort(0).results(0) shouldBe expected
    collected.head.score shouldBe 2.0
    collected.toSet shouldBe expected.toSet
    snapshot.toSet shouldBe initial.toSet
    merged.size shouldBe 3
    merged.scannedRows shouldBe 11L
    the[IllegalStateException] thrownBy merged.add(Candidate(0, 2L, 5L, -1.0))
  }

  test("empty and singleton reads do not finalize their query") {
    val merged = merger(MetricType.L2)
    val emptySnapshot = merged.results(0)
    emptySnapshot shouldBe empty
    val first = Candidate(0, 1L, 1L, 2.0)
    merged.add(first)
    val singleton = merged.results(0)
    singleton shouldBe Vector(first)
    val better = Candidate(0, 1L, 2L, 1.0)
    merged.add(better)
    merged.results(0).toSet shouldBe Set(first, better)
    emptySnapshot shouldBe empty
    singleton shouldBe Vector(first)
    merged.sort(0).results(0) shouldBe Vector(better, first)
    the[IllegalStateException] thrownBy merged.add(Candidate(0, 1L, 3L, 0.0))

    val single = merger(MetricType.L2).addAll(Vector(first))
    single.sort(0).results(0) shouldBe Vector(first)
    single.sort(0).takePacked(0).length shouldBe CandidateBytes.Width
    the[IllegalStateException] thrownBy single.add(better)
  }

  test(
    "packing requires explicit sorting even for empty and singleton queries"
  ) {
    Seq(0, 1, 3).foreach { count =>
      val merged = merger(MetricType.L2, k = 3)
      val candidates = (0 until count).map(index =>
        Candidate(0, 1L, index.toLong, index.toDouble + 1.0)
      )
      merged.addAll(candidates)
      val before = merged.results(0)
      the[IllegalStateException] thrownBy merged.takePacked(0)
      merged.results(0) shouldBe before
      val best = Candidate(0, 2L, 9L, 0.0)
      merged.add(best)
      val expected = (best +: candidates).take(3).toVector
      unpack(0, merged.sort(0).takePacked(0)) shouldBe expected
      merged.results(0) shouldBe expected
    }
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

    places(l2.sort(0).results(0)) shouldBe Vector((101L, 5L), (102L, 7L))
    places(cosine.sort(0).results(0)) shouldBe Vector((101L, 9L), (102L, 7L))
  }

  test("an equal score is broken by segment id, then by row offset") {
    val merged = merger(MetricType.L2, queries = 1, k = 3)
    Seq(
      Candidate(0, 102L, 7L, 0.10),
      Candidate(0, 101L, 9L, 0.10),
      Candidate(0, 101L, 5L, 0.10)
    ).foreach(merged.add)

    places(merged.sort(0).results(0)) shouldBe Vector(
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
    places(merged.sort(0).results(0)) shouldBe Vector((101L, 2L))
    merged.size shouldBe 2
    places(merged.sort(1).results(1)) shouldBe Vector((102L, 3L))
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

      fields(objects.sort(0).results(0)) shouldBe fields(expected)
      fields(unpack(0, bytes.sort(0).takePacked(0))) shouldBe fields(expected)
      fields(unpack(0, objects.takePacked(0))) shouldBe fields(expected)
      fields(bytes.results(0)) shouldBe fields(expected)
      fields(objects.sort(0).results(0)) shouldBe fields(expected)
      fields(unpack(0, bytes.sort(0).takePacked(0))) shouldBe fields(expected)
      objects.size shouldBe expected.size
      bytes.size shouldBe expected.size
    }
  }

  test(
    "sorting fixes one query's order while other queries can still collect"
  ) {
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

    val retained = merged.sort(0).results(0)
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
    unpack(1, merged.sort(1).takePacked(1)) shouldBe next
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
    val packed = merged.sort(1).takePacked(1)
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
    "explicitly sorting an empty query closes it and permits repeated output"
  ) {
    Seq(false, true).foreach { packedFirst =>
      val merged = merger(MetricType.L2, queries = 2)
      merged.sort(0)
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

  test(
    "sorting and both output methods reject queries outside the allocated range"
  ) {
    val merged = merger(MetricType.L2, queries = 2)
    Seq(-1, 2).foreach { query =>
      the[IllegalArgumentException] thrownBy merged.results(query)
      the[IllegalArgumentException] thrownBy merged.takePacked(query)
      the[IllegalArgumentException] thrownBy merged.sort(query)
    }
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
