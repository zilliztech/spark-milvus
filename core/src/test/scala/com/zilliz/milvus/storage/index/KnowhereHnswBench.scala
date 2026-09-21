package com.zilliz.milvus.storage.index

import java.nio.{ByteBuffer, ByteOrder}
import java.util.SplittableRandom

import com.zilliz.milvus.jni.vector.{NativeVectorLibrary, NativeVectorSearch}

import io.knowhere.{DType, Knowhere}

/** HNSW alone: how long one query takes and what it recalls, on two data
  * distributions of the same dimension. Separates "the data is hard" from "the
  * connector is slow": nothing here but the Knowhere Java binding.
  *
  * Distributions: `uniform` (every coordinate uniform in [-1, 1]) and
  * `clustered` (BENCH_CENTERS Gaussian centers, each vector a center plus noise
  * of BENCH_SIGMA per coordinate). Queries come from the same distribution.
  * Ground truth is Knowhere's own brute force.
  *
  * CSV on stdout. Environment: BENCH_DIM (1024), BENCH_ROWS (65536),
  * BENCH_QUERIES (4096), BENCH_TOPK (10), BENCH_EFS (64,128,256), BENCH_M (16),
  * BENCH_EFC (200), BENCH_CENTERS (256), BENCH_SIGMA (0.05), BENCH_DISTS
  * (uniform,clustered), BENCH_CALLERS (1): how many graphs are built and
  * searched concurrently, one caller thread per graph, as Spark tasks on one
  * executor do.
  */
object KnowhereHnswBench {

  private def env(name: String, fallback: String): String =
    sys.env.get(name).map(_.trim).filter(_.nonEmpty).getOrElse(fallback)

  def main(args: Array[String]): Unit = {
    val dim = env("BENCH_DIM", "1024").toInt
    val rows = env("BENCH_ROWS", "65536").toInt
    val nq = env("BENCH_QUERIES", "4096").toInt
    val topK = env("BENCH_TOPK", "10").toInt
    val efs = env("BENCH_EFS", "64,128,256").split(",").map(_.trim.toInt)
    val m = env("BENCH_M", "16").toInt
    val efc = env("BENCH_EFC", "200").toInt
    val centers = env("BENCH_CENTERS", "256").toInt
    val sigma = env("BENCH_SIGMA", "0.05").toFloat
    val dists = env("BENCH_DISTS", "uniform,clustered").split(",").map(_.trim)
    val callers = env("BENCH_CALLERS", "1").toInt
    // BENCH_MASK=true hands Knowhere an all-zero exclusion bitmap over every row,
    // as the connector's IndexProbe does, instead of null.
    val withMask = env("BENCH_MASK", "false").toBoolean
    // BENCH_RELOAD=true searches an index that was serialized and deserialized,
    // as the connector does after reading index files; BENCH_VERSION pins the
    // Knowhere index version (default: maximumIndexVersion()).
    val reload = env("BENCH_RELOAD", "false").toBoolean
    val threads = Runtime.getRuntime.availableProcessors()

    NativeVectorLibrary.load()
    val version =
      env("BENCH_VERSION", Knowhere.maximumIndexVersion().toString).toInt
    println(
      "distribution,rows,dim,queries,callers,mask,reload,version,ef,build_s,search_s,ms_per_query,core_ms_per_query,recall_at_k,brute_s"
    )
    dists.foreach { dist =>
      val rng = new SplittableRandom(7L)
      val centroids =
        Array.fill(centers)(Array.fill(dim)((rng.nextDouble() * 2 - 1).toFloat))
      def vector(target: ByteBuffer, index: Int): Unit = dist match {
        case "uniform" =>
          var d = 0
          while (d < dim) {
            target.putFloat((rng.nextDouble() * 2 - 1).toFloat); d += 1
          }
        case "clustered" =>
          val c = centroids(index % centers)
          var d = 0
          while (d < dim) {
            target.putFloat(c(d) + (gaussian(rng) * sigma).toFloat)
            d += 1
          }
        case other =>
          throw new IllegalArgumentException(s"unknown distribution $other")
      }
      val base = direct(rows.toLong * dim * 4)
      var i = 0
      while (i < rows) { vector(base, i); i += 1 }
      base.clear()
      val queries = direct(nq.toLong * dim * 4)
      i = 0
      while (i < nq) { vector(queries, i); i += 1 }
      queries.clear()

      // ground truth by brute force
      val truthIds = direct(nq.toLong * topK * 8)
      val truthDist = direct(nq.toLong * topK * 4)
      val t0 = System.nanoTime()
      NativeVectorSearch.bruteForce(
        DType.FLOAT32,
        base,
        rows.toLong,
        queries,
        nq.toLong,
        dim,
        topK,
        null,
        truthIds,
        truthDist,
        """{"metric_type":"L2"}"""
      )
      val bruteSeconds = (System.nanoTime() - t0) / 1e9
      val truth = (0 until nq).map { q =>
        (0 until topK).map(j => truthIds.getLong((q * topK + j) * 8)).toSet
      }

      // build `callers` graphs over the same base (same data, separate graphs)
      val b0 = System.nanoTime()
      val indexes = (0 until callers).map { _ =>
        val index = Knowhere.createIndex("HNSW", DType.FLOAT32, version)
        index.build(
          base,
          rows.toLong,
          dim,
          s"""{"metric_type":"L2","dim":$dim,"M":$m,"efConstruction":$efc}"""
        )
        if (!reload) index
        else {
          val blobs = index.serialize()
          index.close()
          val loaded = Knowhere.createIndex("HNSW", DType.FLOAT32, version)
          loaded.deserialize(blobs, s"""{"metric_type":"L2","dim":$dim}""")
          blobs.close()
          loaded
        }
      }
      val buildSeconds = (System.nanoTime() - b0) / 1e9 / callers

      efs.foreach { ef =>
        val outputs = indexes.map(_ =>
          (direct(nq.toLong * topK * 8), direct(nq.toLong * topK * 4))
        )
        val mask = if (withMask) direct((rows.toLong + 7) / 8) else null
        val maskBits = if (withMask) rows.toLong else 0L
        val pool = java.util.concurrent.Executors.newFixedThreadPool(callers)
        val s0 = System.nanoTime()
        val futures = indexes.zip(outputs).map { case (index, (ids, scores)) =>
          pool.submit(new Runnable {
            override def run(): Unit = index.search(
              queries,
              nq.toLong,
              dim,
              topK,
              mask,
              maskBits,
              ids,
              scores,
              s"""{"metric_type":"L2","ef":$ef}"""
            )
          })
        }
        futures.foreach(_.get())
        val searchSeconds = (System.nanoTime() - s0) / 1e9
        pool.shutdown()
        val (ids, _) = outputs.head
        val recall = (0 until nq).map { q =>
          val got =
            (0 until topK).map(j => ids.getLong((q * topK + j) * 8)).toSet
          truth(q).count(got).toDouble / topK
        }.sum / nq
        // per query: wall over all callers' queries; core-ms: machine-seconds per query
        val totalQueries = nq.toLong * callers
        println(
          f"$dist,$rows,$dim,$nq,$callers,$withMask,$reload,$version,$ef,$buildSeconds%.1f,$searchSeconds%.2f,${searchSeconds * 1000 / totalQueries}%.3f," +
            f"${searchSeconds * 1000 * threads / totalQueries}%.1f,$recall%.4f,$bruteSeconds%.2f"
        )
        System.out.flush()
      }
      indexes.foreach(_.close())
    }
  }

  private def gaussian(rng: SplittableRandom): Double = {
    // Box-Muller
    val u1 = math.max(rng.nextDouble(), 1e-12); val u2 = rng.nextDouble()
    math.sqrt(-2.0 * math.log(u1)) * math.cos(2.0 * math.Pi * u2)
  }

  private def direct(bytes: Long): ByteBuffer =
    ByteBuffer.allocateDirect(bytes.toInt).order(ByteOrder.nativeOrder())
}
