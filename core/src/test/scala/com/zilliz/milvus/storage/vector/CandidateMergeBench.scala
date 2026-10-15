package com.zilliz.milvus.storage.vector

import java.lang.{Double => JavaDouble, Long => JavaLong}
import java.lang.management.ManagementFactory
import java.nio.ByteBuffer
import java.util.{Arrays, SplittableRandom}

import com.sun.management.ThreadMXBean

import com.zilliz.milvus.storage.schema.MetricType

/** Manual comparison with the two-way merge at d1536627, without Spark or JNI.
  * Run with `core/Test/runMain
  * com.zilliz.milvus.storage.vector.CandidateMergeBench`. It is not a test
  * suite.
  *
  * CSV goes to stdout; environment and progress go to stderr. MERGE_BENCH_FORK
  * labels an independent JVM (default 0), MERGE_BENCH_WARMUPS defaults to 12,
  * MERGE_BENCH_ROUNDS to 15, and MERGE_BENCH_BATCH_BYTES to 4194304. Optional
  * MERGE_BENCH_K, MERGE_BENCH_RUNS and MERGE_BENCH_ROWS filter the default
  * matrix with comma-separated integers; row width 0 selects plain candidates.
  * MERGE_BENCH_B defaults to 2,4,8,16. MERGE_BENCH_METRIC is L2 or IP.
  *
  * Inputs and an independently sorted object oracle are prepared outside the
  * timed section. Each measurement includes state construction, append and
  * finish, retains every result, and validates every byte afterwards. Baseline
  * measurements are shared by the eight configurations of the same fixture;
  * their execution order rotates each round. Repeated reads of the same input
  * arrays measure a warm JVM, not shuffle, spill, peak live memory or cold IO.
  */
object CandidateMergeBench {
  @volatile private var sink: AnyRef = _

  private final case class Fixture(
      k: Int,
      runs: Int,
      rowBytes: Int,
      metric: MetricType,
      inputs: Array[Array[Byte]],
      expected: Array[Byte]
  ) {
    val runBytes: Long = inputs(0).length.toLong
    val layout: String = if (rowBytes == 0) "plain" else "carried"
  }

  private final case class Variant(maxRuns: Int, pressure: String, target: Long)
  private final case class Sample(nanos: Double, bytes: Double)

  private def env(name: String, fallback: String): String =
    sys.env.get(name).map(_.trim).filter(_.nonEmpty).getOrElse(fallback)

  private def numbers(name: String, fallback: String): Vector[Int] =
    env(name, fallback).split(",").map(_.trim.toInt).toVector

  private def selected(name: String, value: Int): Boolean =
    !sys.env.contains(name) || numbers(name, "").contains(value)

  private def encode(rows: Seq[Candidate], rowBytes: Int): Array[Byte] = {
    val width = if (rowBytes == 0) 24 else 28 + rowBytes
    val buffer = ByteBuffer.allocate(rows.size * width)
    rows.foreach { row =>
      buffer.putLong(row.segmentId).putLong(row.rowOffset).putDouble(row.score)
      if (rowBytes > 0) {
        buffer.putInt(rowBytes)
        var at = 0
        while (at < rowBytes) {
          buffer.put((row.segmentId * 31 + row.rowOffset * 7 + at).toByte)
          at += 1
        }
      }
    }
    buffer.array()
  }

  private def fixture(
      k: Int,
      runs: Int,
      rowBytes: Int,
      metric: MetricType
  ): Fixture = {
    val random = new SplittableRandom(4271023L + k * 37L + runs)
    val all = Vector.newBuilder[Candidate]
    val inputs = Array.tabulate(runs) { run =>
      val candidates = Vector
        .tabulate(k) { offset =>
          // All positions are unique; rounded scores also exercise tie-breakers.
          val score = random.nextInt(k * runs * 2).toDouble
          Candidate(0, run.toLong, offset.toLong, score)
        }
        .sorted(Candidate.ranking(metric))
      all ++= candidates
      encode(candidates, rowBytes)
    }
    val expected =
      encode(all.result().sorted(Candidate.ranking(metric)).take(k), rowBytes)
    Fixture(k, runs, rowBytes, metric, inputs, expected)
  }

  private def baseline(data: Fixture): Array[Byte] = {
    var result = data.inputs(0)
    var at = 1
    while (at < data.inputs.length) {
      result =
        if (data.rowBytes == 0)
          BaselinePlain.merge(result, data.inputs(at), data.k, data.metric)
        else BaselineCarried.merge(result, data.inputs(at), data.k, data.metric)
      at += 1
    }
    result
  }

  private def bounded(data: Fixture, variant: Variant): Array[Byte] = {
    val layout =
      if (data.rowBytes == 0) CandidateRuns.Plain else CandidateRuns.Carried
    val state = new CandidateRuns(
      data.k,
      data.metric,
      layout,
      variant.maxRuns,
      variant.target
    )
    var at = 0
    while (at < data.inputs.length) {
      state.append(data.inputs(at))
      at += 1
    }
    state.finish()
  }

  private def measure(
      bean: ThreadMXBean,
      thread: Long,
      repetitions: Int,
      expected: Array[Byte]
  )(run: () => Array[Byte]): Sample = {
    // The holder belongs to the harness, so its allocation is not charged.
    val outputs = new Array[Array[Byte]](repetitions)
    val allocatedBefore = bean.getThreadAllocatedBytes(thread)
    val start = System.nanoTime()
    var at = 0
    while (at < repetitions) {
      outputs(at) = run()
      at += 1
    }
    val elapsed = System.nanoTime() - start
    val allocated = bean.getThreadAllocatedBytes(thread) - allocatedBefore
    sink = outputs
    at = 0
    while (at < repetitions) {
      require(
        Arrays.equals(outputs(at), expected),
        s"Incorrect output at repetition $at"
      )
      at += 1
    }
    Sample(elapsed.toDouble / repetitions, allocated.toDouble / repetitions)
  }

  def main(args: Array[String]): Unit = {
    val bean = ManagementFactory.getThreadMXBean match {
      case supported: ThreadMXBean
          if supported.isThreadAllocatedMemorySupported =>
        supported
      case _ =>
        throw new IllegalStateException(
          "Thread allocation measurement is unavailable"
        )
    }
    bean.setThreadAllocatedMemoryEnabled(true)
    val thread = Thread.currentThread().getId
    val fork = env("MERGE_BENCH_FORK", "0").toInt
    val warmups = env("MERGE_BENCH_WARMUPS", "12").toInt
    val rounds = env("MERGE_BENCH_ROUNDS", "15").toInt
    val batchBytes = env("MERGE_BENCH_BATCH_BYTES", "4194304").toLong
    val batches = numbers("MERGE_BENCH_B", "2,4,8,16")
    val metric = env("MERGE_BENCH_METRIC", "L2") match {
      case "L2" => MetricType.L2
      case "IP" => MetricType.IP
      case other =>
        throw new IllegalArgumentException(
          s"Unsupported benchmark metric $other"
        )
    }
    require(warmups >= 0 && rounds > 0 && batchBytes > 0L)
    require(batches.nonEmpty && batches.forall(_ >= 2))
    val cells = (for {
      k <- Vector(100, 1000)
      rowBytes <- Vector(0, 64, 1024)
      runs <- Vector(1, 2, 8, 32, 128)
      if k == 100 || (rowBytes != 64 && (runs == 8 || runs == 128))
      if selected("MERGE_BENCH_K", k)
      if selected("MERGE_BENCH_RUNS", runs)
      if selected("MERGE_BENCH_ROWS", rowBytes)
    } yield (k, runs, rowBytes)).sortBy { case (k, runs, rowBytes) =>
      k.toLong * runs * (if (rowBytes == 0) 24 else rowBytes + 28)
    }
    require(cells.nonEmpty, "The benchmark filters selected no fixtures")
    System.err.println(
      s"baseline=d1536627 fork=$fork java=${System.getProperty("java.version")} " +
        s"metric=$metric fixtures=${cells.size} warmups=$warmups rounds=$rounds"
    )
    println(
      "fork,metric,layout,row_bytes,k,input_runs,max_runs,pressure,target_bytes,repetitions,round,baseline_ns,bounded_ns,baseline_allocated_bytes,bounded_allocated_bytes"
    )
    cells.foreach { case (k, runs, rowBytes) =>
      val data = fixture(k, runs, rowBytes, metric)
      val variants = for {
        b <- batches
        pressure <- Vector("none", "tight")
      } yield Variant(
        b,
        pressure,
        if (pressure == "none") b * data.runBytes else data.runBytes * 3 / 2
      )
      val baselineBytes = math.max(1L, (runs - 1L) * data.runBytes)
      val repetitions =
        math.min(2048L, math.max(1L, batchBytes / baselineBytes)).toInt
      System.err.println(
        s"k=$k runs=$runs rowBytes=$rowBytes repetitions=$repetitions"
      )
      val operations =
        (() => baseline(data)) +: variants.map(v => () => bounded(data, v))
      var round = -warmups
      while (round < rounds) {
        val samples = new Array[Sample](operations.size)
        var at = 0
        while (at < operations.size) {
          val index = (at + round + warmups + fork) % operations.size
          samples(index) =
            measure(bean, thread, repetitions, data.expected)(operations(index))
          at += 1
        }
        if (round >= 0) {
          variants.zipWithIndex.foreach { case (variant, index) =>
            val old = samples(0)
            val current = samples(index + 1)
            println(
              s"$fork,$metric,${data.layout},$rowBytes,$k,$runs,${variant.maxRuns},${variant.pressure}," +
                s"${variant.target},$repetitions,$round,${old.nanos},${current.nanos},${old.bytes},${current.bytes}"
            )
          }
          System.out.flush()
        }
        round += 1
      }
    }
  }

  // Preserve the d1536627 merge bodies and widths: later production changes
  // must not silently move this baseline. Unrelated encoding methods are omitted.
  private object BaselinePlain {
    private val Width = 24

    private def count(packed: Array[Byte]): Int = {
      require(
        packed.length % Width == 0,
        s"Packed candidates come in $Width bytes each, not ${packed.length}"
      )
      packed.length / Width
    }

    def merge(
        left: Array[Byte],
        right: Array[Byte],
        k: Int,
        metric: MetricType
    ): Array[Byte] = {
      require(k > 0, s"topK must be positive: $k")
      val sign = if (metric.smallerIsBetter) 1 else -1
      val leftSize = count(left)
      val rightSize = count(right)
      if (rightSize == 0)
        return if (leftSize <= k) left else left.take(k * Width)
      if (leftSize == 0)
        return if (rightSize <= k) right else right.take(k * Width)
      val size = math.min(k, leftSize + rightSize)
      val packed = new Array[Byte](size * Width)
      val target = ByteBuffer.wrap(packed)
      val leftBuffer = ByteBuffer.wrap(left)
      val rightBuffer = ByteBuffer.wrap(right)
      var leftAt = 0
      var rightAt = 0
      var at = 0
      while (at < size) {
        val takeLeft =
          if (leftAt >= leftSize) false
          else if (rightAt >= rightSize) true
          else better(sign, leftBuffer, leftAt, rightBuffer, rightAt) <= 0
        val source = if (takeLeft) leftBuffer else rightBuffer
        val from = (if (takeLeft) leftAt else rightAt) * Width
        val to = at * Width
        target.putLong(to, source.getLong(from))
        target.putLong(to + 8, source.getLong(from + 8))
        target.putDouble(to + 16, source.getDouble(from + 16))
        if (takeLeft) leftAt += 1 else rightAt += 1
        at += 1
      }
      packed
    }

    private def better(
        sign: Int,
        left: ByteBuffer,
        leftAt: Int,
        right: ByteBuffer,
        rightAt: Int
    ): Int = {
      val leftOffset = leftAt * Width
      val rightOffset = rightAt * Width
      val score = JavaDouble.compare(
        left.getDouble(leftOffset + 16),
        right.getDouble(rightOffset + 16)
      ) * sign
      if (score != 0) score
      else {
        val segment = JavaLong.compare(
          left.getLong(leftOffset),
          right.getLong(rightOffset)
        )
        if (segment != 0) segment
        else
          JavaLong.compare(
            left.getLong(leftOffset + 8),
            right.getLong(rightOffset + 8)
          )
      }
    }
  }

  private object BaselineCarried {
    private val Header = 28

    def merge(
        left: Array[Byte],
        right: Array[Byte],
        k: Int,
        metric: MetricType
    ): Array[Byte] = {
      require(k > 0, s"topK must be positive: $k")
      val better = if (metric.smallerIsBetter) 1 else -1
      val leftBuffer = ByteBuffer.wrap(left)
      val rightBuffer = ByteBuffer.wrap(right)
      def length(buffer: ByteBuffer, at: Int): Int =
        Header + buffer.getInt(at + 24)
      def compare(l: Int, r: Int): Int = {
        val byScore = JavaDouble.compare(
          leftBuffer.getDouble(l + 16),
          rightBuffer.getDouble(r + 16)
        ) * better
        if (byScore != 0) byScore
        else {
          val bySegment =
            JavaLong.compare(leftBuffer.getLong(l), rightBuffer.getLong(r))
          if (bySegment != 0) bySegment
          else
            JavaLong.compare(
              leftBuffer.getLong(l + 8),
              rightBuffer.getLong(r + 8)
            )
        }
      }
      val chosen = new Array[Long](k)
      var count = 0
      var l = 0
      var r = 0
      var bytes = 0
      while (count < k && (l < left.length || r < right.length)) {
        val fromLeft =
          r >= right.length || (l < left.length && compare(l, r) <= 0)
        if (fromLeft) {
          chosen(count) = l.toLong
          bytes += length(leftBuffer, l)
          l += length(leftBuffer, l)
        } else {
          chosen(count) = -(r.toLong + 1L)
          bytes += length(rightBuffer, r)
          r += length(rightBuffer, r)
        }
        count += 1
      }
      val out = new Array[Byte](bytes)
      var written = 0
      var at = 0
      while (at < count) {
        val (source, buffer, from) =
          if (chosen(at) >= 0L) (left, leftBuffer, chosen(at).toInt)
          else (right, rightBuffer, (-chosen(at) - 1L).toInt)
        val size = length(buffer, from)
        System.arraycopy(source, from, out, written, size)
        written += size
        at += 1
      }
      out
    }
  }
}
