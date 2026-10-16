package com.zilliz.milvus.storage.vector

import java.io.{
  ByteArrayInputStream,
  ByteArrayOutputStream,
  ObjectInputStream,
  ObjectOutputStream
}
import java.util.Arrays
import scala.util.Random

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.schema.MetricType

class CandidateRunsTest extends AnyFunSuite with Matchers {
  private val segmentIds = (0L to 104L)
  private val dictionary = CandidateDictionary(segmentIds)
  private val encoding = CandidateEncoding(dictionary, segmentIds)

  private val layouts =
    Seq(CandidateRuns.Plain, CandidateRuns.Carried)

  private def ordered(
      candidates: Seq[Candidate],
      metric: MetricType
  ): Seq[Candidate] = {
    val sorted = candidates.toArray
    Arrays.sort(sorted, Candidate.ranking(metric))
    sorted.toIndexedSeq
  }

  private def pack(
      candidates: Seq[Candidate],
      metric: MetricType,
      layout: CandidateRuns.Layout
  ): Array[Byte] = layout match {
    case CandidateRuns.Plain =>
      CandidateBytesFixture.pack(candidates, metric, encoding)
    case CandidateRuns.Carried =>
      CarriedCandidates.writeSorted(ordered(candidates, metric).map {
        candidate =>
          val length = ((candidate.segmentId + candidate.rowOffset) % 65L).toInt
          candidate -> Array.tabulate[Byte](length)(at =>
            (candidate.segmentId + candidate.rowOffset + at).toByte
          )
      })
  }

  private def checkBounds(state: CandidateRuns): Unit = {
    state.runCount should be < state.maxRuns
    assert(state.retainedBytes <= state.targetBytes || state.runCount == 1)
  }

  private def restored(state: CandidateRuns): CandidateRuns = {
    val bytes = new ByteArrayOutputStream()
    val output = new ObjectOutputStream(bytes)
    try output.writeObject(state)
    finally output.close()
    val input = new ObjectInputStream(
      new ByteArrayInputStream(bytes.toByteArray)
    )
    try input.readObject().asInstanceOf[CandidateRuns]
    finally input.close()
  }

  test(
    "empty and singleton states finish repeatedly without consuming the answer"
  ) {
    layouts.foreach { layout =>
      val state = new CandidateRuns(4, MetricType.L2, layout, 4, 1024L)
      state.append(CandidateBytes.Empty) should be theSameInstanceAs state
      state.finish() shouldBe empty
      state.runCount shouldBe 0
      state.retainedBytes shouldBe 0L
      val one = pack(Seq(Candidate(0, 1L, 0L, 2.0f)), MetricType.L2, layout)
      state.append(one)
      state.finish() should be theSameInstanceAs one
      state.finish() should be theSameInstanceAs one
      state.retainedBytes shouldBe one.length.toLong
    }
  }

  test("a full count batch merges together at exactly its byte target") {
    val k = 2
    val runBytes = k * CandidateBytes.Width
    val state =
      new CandidateRuns(
        k,
        MetricType.L2,
        CandidateRuns.Plain,
        4,
        4L * runBytes
      )
    (0 until 4).foreach { segment =>
      state.append(
        pack(
          (0 until k).map(row =>
            Candidate(0, segment.toLong, row.toLong, segment + row.toFloat)
          ),
          MetricType.L2,
          CandidateRuns.Plain
        )
      )
      state.runCount shouldBe (if (segment == 3) 1 else segment + 1)
      checkBounds(state)
    }
    state.retainedBytes shouldBe runBytes.toLong
  }

  test(
    "the byte trigger distinguishes below, exactly at and above its target"
  ) {
    val runs = (0 until 3).map(segment =>
      pack(
        Seq(Candidate(0, segment.toLong, 0L, segment.toFloat)),
        MetricType.L2,
        CandidateRuns.Plain
      )
    )
    Seq(35L -> 2, 36L -> 3, 37L -> 3).foreach { case (target, count) =>
      val state =
        new CandidateRuns(
          1,
          MetricType.L2,
          CandidateRuns.Plain,
          8,
          target
        )
      runs.foreach { run => state.append(run); checkBounds(state) }
      state.runCount shouldBe count
      state.finish().toSeq shouldBe runs.head.toSeq
    }
  }

  test("an oversized input and a wider merged carried answer stay intact") {
    val state =
      new CandidateRuns(2, MetricType.L2, CandidateRuns.Carried, 8, 200L)
    val left = CarriedCandidates.writeSorted(
      Seq(
        Candidate(0, 0L, 0L, 1.0f) -> new Array[Byte](100),
        Candidate(0, 0L, 1L, 30.0f) -> Array.emptyByteArray
      )
    )
    val right = CarriedCandidates.writeSorted(
      Seq(
        Candidate(0, 1L, 0L, 2.0f) -> new Array[Byte](100),
        Candidate(0, 1L, 1L, 40.0f) -> Array.emptyByteArray
      )
    )
    state.append(left).append(right)
    checkBounds(state)
    state.runCount shouldBe 1
    state.finish().length shouldBe 232
    state.retainedBytes should be > state.targetBytes
    state.finish().length should be > math.max(left.length, right.length)

    val tiny = new CandidateRuns(2, MetricType.L2, CandidateRuns.Carried, 8, 1L)
    tiny.append(left)
    tiny.finish() should be theSameInstanceAs left
    tiny.append(right).finish().toSeq shouldBe state.finish().toSeq
    checkBounds(tiny)
  }

  test(
    "finish then append preserves old output and agrees with one final top-k"
  ) {
    layouts.foreach { layout =>
      val first = Seq(Candidate(0, 1L, 0L, 2.0f), Candidate(0, 1L, 1L, 4.0f))
      val later = Seq(Candidate(0, 2L, 0L, 1.0f), Candidate(0, 2L, 1L, 3.0f))
      val state = new CandidateRuns(2, MetricType.L2, layout, 4, 4096L)
      state.append(pack(first, MetricType.L2, layout))
      val old = state.finish()
      val saved = old.clone()
      state.append(pack(later, MetricType.L2, layout))
      state.finish().toSeq shouldBe pack(
        ordered(first ++ later, MetricType.L2).take(2),
        MetricType.L2,
        layout
      ).toSeq
      state.finish() should be theSameInstanceAs state.finish()
      old.toSeq shouldBe saved.toSeq
    }
  }

  test(
    "permutations, intermediate finishes and absorb trees match an independent sort"
  ) {
    val random = new Random(913L)
    val special = Array(
      Float.NaN,
      Float.NegativeInfinity,
      -0.0f,
      0.0f,
      Float.PositiveInfinity
    )
    MetricType.values.foreach { metric =>
      layouts.foreach { layout =>
        val k = 7
        val source = (0 until 29).map { segment =>
          (0 until (segment % (k + 1))).map { row =>
            val score =
              if (row < special.length) special(row)
              else random.nextInt(4).toFloat
            Candidate(0, segment.toLong, row.toLong, score)
          }
        }
        val packed = source.map(pack(_, metric, layout))
        val saved = packed.map(_.clone())
        val expected =
          pack(ordered(source.flatten, metric).take(k), metric, layout)
        for (
          batch <- Seq(2, 4, 8, 16); target <- Seq(1L, 300L, Long.MaxValue)
        ) {
          val sequential = new CandidateRuns(k, metric, layout, batch, target)
          random.shuffle(packed).zipWithIndex.foreach { case (run, at) =>
            sequential.append(run)
            if (at % 5 == 0) sequential.finish()
            checkBounds(sequential)
          }
          sequential.finish().toSeq shouldBe expected.toSeq
          var level = random
            .shuffle(packed)
            .grouped(3)
            .map { group =>
              val state = new CandidateRuns(k, metric, layout, batch, target)
              group.foreach(state.append)
              state
            }
            .toVector
          while (level.size > 1) {
            level = level
              .grouped(2)
              .map { pair =>
                if (pair.size == 2) {
                  val rightBytes = pair(1).retainedBytes
                  val rightCount = pair(1).runCount
                  pair(0).absorb(pair(1))
                  pair(1).retainedBytes shouldBe rightBytes
                  pair(1).runCount shouldBe rightCount
                }
                checkBounds(pair.head)
                pair.head
              }
              .toVector
          }
          level.head.finish().toSeq shouldBe expected.toSeq
        }
        packed.zip(saved).foreach { case (actual, original) =>
          actual.toSeq shouldBe original.toSeq
        }
      }
    }
  }

  test("absorb preserves the other state's runs and answer") {
    layouts.foreach { layout =>
      val left = new CandidateRuns(2, MetricType.L2, layout, 8, 4096L)
      val right = new CandidateRuns(2, MetricType.L2, layout, 8, 4096L)
      val candidates =
        Seq(Candidate(0, 1L, 0L, 2.0f), Candidate(0, 2L, 0L, 3.0f))
      left.append(pack(Seq(Candidate(0, 0L, 0L, 1.0f)), MetricType.L2, layout))
      candidates.foreach(candidate =>
        right.append(pack(Seq(candidate), MetricType.L2, layout))
      )
      val rightBytes = right.retainedBytes
      left.absorb(right)
      right.runCount shouldBe 2
      right.retainedBytes shouldBe rightBytes
      right.finish().toSeq shouldBe pack(
        candidates,
        MetricType.L2,
        layout
      ).toSeq
      left.finish().toSeq shouldBe pack(
        Seq(Candidate(0, 0L, 0L, 1.0f), candidates.head),
        MetricType.L2,
        layout
      ).toSeq
    }
  }

  test(
    "serialized partial and finished states can continue appending and absorbing"
  ) {
    layouts.foreach { layout =>
      val all = (0 until 6).map(segment =>
        Candidate(0, segment.toLong, 0L, segment.toFloat)
      )
      val runs =
        all.map(candidate => pack(Seq(candidate), MetricType.IP, layout))
      Seq(false, true).foreach { finishBefore =>
        val initial = new CandidateRuns(3, MetricType.IP, layout, 4, 4096L)
        runs.take(2).foreach(initial.append)
        if (finishBefore) initial.finish()
        val copy = restored(initial)
        copy.runCount shouldBe initial.runCount
        copy.retainedBytes shouldBe initial.retainedBytes
        copy.append(runs(2))
        val other = new CandidateRuns(3, MetricType.IP, layout, 4, 4096L)
        runs.drop(3).foreach(other.append)
        copy.absorb(restored(other))
        copy.finish().toSeq shouldBe pack(
          ordered(all, MetricType.IP).take(3),
          MetricType.IP,
          layout
        ).toSeq
        checkBounds(copy)
      }
    }
  }

  test("invalid limits, incompatible states and self-absorption fail") {
    def state(
        k: Int = 2,
        metric: MetricType = MetricType.L2,
        layout: CandidateRuns.Layout = CandidateRuns.Plain,
        batch: Int = 4,
        bytes: Long = 1024L
    ): CandidateRuns =
      new CandidateRuns(k, metric, layout, batch, bytes)
    intercept[IllegalArgumentException](state(k = 0))
    intercept[IllegalArgumentException](state(metric = null))
    intercept[IllegalArgumentException](state(layout = null))
    intercept[IllegalArgumentException](state(batch = 1))
    intercept[IllegalArgumentException](state(bytes = 0L))
    val left = state()
    intercept[IllegalArgumentException](left.absorb(left))
    Seq(
      state(k = 3),
      state(metric = MetricType.IP),
      state(layout = CandidateRuns.Carried),
      state(batch = 8),
      state(bytes = 512L)
    ).foreach { right =>
      intercept[IllegalArgumentException](left.absorb(right))
    }
    left.runCount shouldBe 0
  }
}
