package com.zilliz.spark.connector.uat

import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{
  ArrayType,
  BinaryType,
  FloatType,
  LongType,
  ShortType,
  StructField,
  StructType
}

import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.spark.connector.options.MilvusOption
import com.zilliz.spark.connector.read.MilvusSearch

/** The chapter-2 correctness cases of the 2.0 test plan, run as one Spark
  * application on a cluster so that every case appears in the history server.
  *
  * Each case runs under its own job group, whose id is the case id, and reports
  * one line
  * {{{
  *   [CorrectnessJob] CASE R-01 PASS
  * }}}
  * The application writes `results.json` beside its results prefix and exits
  * non-zero when a case failed.
  *
  * Every expected value is recomputed from the row id with the generator the
  * datasets were loaded with (gen.py in the configmap
  * `spark-milvus-loader-scripts`), so no Milvus or connector output is the
  * baseline.
  *
  * {{{
  *   --group read|search|procedure|all   default all
  *   --cases R-01,S-03                   overrides --group
  *   --results s3a://.../correctness     default s3a://$CT_OUTPUT_BUCKET/$CT_OUTPUT_PREFIX
  * }}}
  * The datasets come from the environment: `MILVUS_JNI_S3_BUCKET`,
  * `MILVUS_JNI_S3_REGION`, `MILVUS_JNI_S3_ROOT_PATH`, the `DS_*_SNAPSHOT_*`
  * paths, `CT_PARTS_PARTITIONS`, and for the procedure group `MILVUS_UAT_URI`
  * and `MILVUS_UAT_TOKEN`. What the cases write goes to `CT_OUTPUT_BUCKET`
  * under `CT_OUTPUT_PREFIX` (default `spark-milvus-correctness`).
  */
object CorrectnessJob {

  private class Skipped(message: String) extends RuntimeException(message)

  final case class Case(
      id: String,
      group: String,
      title: String,
      body: () => Unit
  )

  final case class Result(
      id: String,
      group: String,
      title: String,
      status: String,
      message: String,
      seconds: Double
  )

  private def env(name: String): Option[String] =
    sys.env.get(name).map(_.trim).filter(_.nonEmpty)

  private def need(name: String): String =
    env(name).getOrElse(throw new Skipped(s"$name is not set"))

  private def ds(name: String): String = need(s"DS_$name")

  private def check(condition: Boolean, message: => String): Unit =
    if (!condition) throw new AssertionError(message)

  private def same(actual: Any, expected: Any, what: => String): Unit =
    if (actual != expected)
      throw new AssertionError(s"$what: $actual != $expected")

  private def near(
      actual: Double,
      expected: Double,
      tolerance: Double,
      what: => String
  ): Unit = {
    val allowed = tolerance * math.max(1.0, math.abs(expected))
    if (math.abs(actual - expected) > allowed)
      throw new AssertionError(
        f"$what: $actual%.6f != $expected%.6f (allowed $allowed%.6f)"
      )
  }

  private def failure(what: String)(body: => Any): Exception =
    try {
      body
      throw new AssertionError(s"$what did not fail")
    } catch {
      case error: AssertionError => throw error
      case NonFatal(error)       => error.asInstanceOf[Exception]
    }

  private var session: SparkSession = null
  private def spark: SparkSession = session

  // ---- reading ---------------------------------------------------------------

  private def storageOptions(): Map[String, String] = {
    val bucket = need("MILVUS_JNI_S3_BUCKET")
    val region = env("MILVUS_JNI_S3_REGION").getOrElse("us-west-2")
    Map(
      StorageProperties.BucketName -> bucket,
      StorageProperties.Address -> s"s3.$region.amazonaws.com",
      StorageProperties.Region -> region,
      StorageProperties.CloudProvider -> "aws",
      StorageProperties.UseSSL -> "true",
      StorageProperties.UseIam -> "true"
    ) ++ env("MILVUS_JNI_S3_ROOT_PATH").map(StorageProperties.RootPath -> _)
  }

  private def snapshot(
      path: String,
      columnar: Boolean = true,
      extra: Seq[(String, String)] = Seq.empty
  ): DataFrame = {
    var reader = spark.read
      .format("milvus")
      .option(MilvusOption.SnapshotPath, path)
      .option(MilvusOption.ReadColumnar, columnar.toString)
    (storageOptions() ++ extra).foreach { case (k, v) =>
      reader = reader.option(k, v)
    }
    reader.load()
  }

  private def ids(frame: DataFrame): Seq[Long] =
    frame
      .select(col("id").cast(LongType))
      .collect()
      .map(_.getLong(0))
      .sorted
      .toSeq

  private val extraColumns = Seq(
    MilvusOption.MilvusExtraColumns -> "_segment_id,_row_offset,_timestamp"
  )

  // ---- expected values, a port of gen.py --------------------------------------

  private def floatToHalfBits(value: Float): Int = {
    val bits = java.lang.Float.floatToIntBits(value)
    val sign = (bits >>> 16) & 0x8000
    val exponent = ((bits >>> 23) & 0xff) - 127 + 15
    val mantissa = bits & 0x7fffff
    if (exponent >= 31) sign | 0x7c00
    else if (exponent <= 0) sign
    else {
      var half = (exponent << 10) | (mantissa >>> 13)
      val round = (mantissa >>> 12) & 1
      val sticky = if ((mantissa & 0xfff) != 0) 1 else 0
      if (round == 1 && (sticky == 1 || (half & 1) == 1)) half += 1
      half | sign
    }
  }

  private def halfBitsToFloat(bits: Int): Float = {
    val sign = if ((bits & 0x8000) != 0) -1.0f else 1.0f
    val exponent = (bits >>> 10) & 0x1f
    val mantissa = bits & 0x3ff
    if (exponent == 0) sign * mantissa.toFloat * math.pow(2.0, -24).toFloat
    else if (exponent == 31)
      if (mantissa == 0) sign * Float.PositiveInfinity else Float.NaN
    else
      sign * (1.0f + mantissa / 1024.0f) * math.pow(2.0, exponent - 15).toFloat
  }

  private def float16(value: Float): Float = halfBitsToFloat(
    floatToHalfBits(value)
  )

  private def bfloat16(value: Float): Float = {
    val bits = java.lang.Float.floatToRawIntBits(value).toLong & 0xffffffffL
    val lsb = (bits >> 16) & 1L
    val rounded = ((bits + 0x7fffL + lsb) >> 16) & 0xffffL
    java.lang.Float.intBitsToFloat((rounded << 16).toInt)
  }

  private object AllTypes {
    def i32(id: Long): Int = ((id - 500) * 1000003L).toInt
    def fv(id: Long): Seq[Float] =
      (0 until 8).map(k => ((id * 8 + k) / 1024.0).toFloat)
    def fvNull(id: Long): Option[Seq[Float]] =
      if (id % 4 == 0) None
      else Some((0 until 8).map(k => ((id * 8 + k) / 1024.0 + 1.0).toFloat))
    def fv16(id: Long): Seq[Float] =
      (0 until 8).map(k => float16((((id % 128) * 8 + k) / 1024.0).toFloat))
    def bf16(id: Long): Seq[Float] =
      (0 until 8).map(k => bfloat16((((id % 32) * 8 + k) / 256.0).toFloat))
    def i8v(id: Long): Seq[Short] =
      (0 until 8).map(k => (((id + k) % 256) - 128).toShort)
    def bv(id: Long): Seq[Byte] = (0 until 8).map(k => ((id + k) & 0xff).toByte)
    def sv(id: Long): Map[Long, Float] =
      Map(
        (id % 50) -> 1.0f,
        (50 + id % 50) -> 0.5f,
        (100 + id % 17) -> ((id % 16) / 16.0).toFloat
      )
  }

  private val json = new ObjectMapper()

  private def allTypesMismatches(row: Row): Seq[String] = {
    val id = row.getAs[Long]("id")
    val out = Seq.newBuilder[String]
    def field(name: String, actual: Any, expected: Any): Unit =
      if (actual != expected) out += s"id=$id $name: $actual != $expected"
    def opt[T](name: String): Option[T] =
      if (row.isNullAt(row.fieldIndex(name))) None else Some(row.getAs[T](name))
    def floats(name: String): Option[Seq[Float]] =
      opt[scala.collection.Seq[Float]](name).map(_.toSeq)
    field("b", row.getAs[Boolean]("b"), id % 2 == 0)
    field("i8", row.getAs[Byte]("i8"), ((id % 256) - 128).toByte)
    field("i16", row.getAs[Short]("i16"), (((id * 37) % 65536) - 32768).toShort)
    field("i32", row.getAs[Int]("i32"), AllTypes.i32(id))
    field("i64", row.getAs[Long]("i64"), id * 6364136223846793005L)
    field("f", row.getAs[Float]("f"), (id * 0.25 - 100.0).toFloat)
    field("d", row.getAs[Double]("d"), id * 0.125 - 50.0)
    field("s", row.getAs[String]("s"), f"row-$id%04d")
    field(
      "s_null",
      opt[String]("s_null"),
      if (id % 5 == 0) None else Some(s"opt-$id")
    )
    val j = json.readTree(row.getAs[String]("j"))
    field("j.id", j.get("id").asLong(), id)
    field("j.even", j.get("even").asBoolean(), id % 2 == 0)
    field("j.tags", j.get("tags").get(0).asText(), s"t${id % 3}")
    field("j.nested.k", j.get("nested").get("k").asLong(), id * 2)
    field(
      "arr_i64",
      row.getSeq[Long](row.fieldIndex("arr_i64")).toSeq,
      (0L until 1 + id % 4).map(id + _)
    )
    field(
      "arr_str",
      row.getSeq[String](row.fieldIndex("arr_str")).toSeq,
      Seq(s"a$id", s"b$id").take(1 + (id % 2).toInt)
    )
    field(
      "opt_i32",
      opt[Int]("opt_i32"),
      if (id % 3 == 0) None else Some(id.toInt)
    )
    field(
      "opt_f",
      opt[Float]("opt_f"),
      if (id % 7 == 0) None else Some((id * 0.5).toFloat)
    )
    field("fv", floats("fv"), Some(AllTypes.fv(id)))
    field("fv_null", floats("fv_null"), AllTypes.fvNull(id))
    field("fv16", floats("fv16"), Some(AllTypes.fv16(id)))
    field("bf16", floats("bf16"), Some(AllTypes.bf16(id)))
    field(
      "i8v",
      opt[scala.collection.Seq[Short]]("i8v").map(_.toSeq),
      Some(AllTypes.i8v(id))
    )
    field("bv", opt[Array[Byte]]("bv").map(_.toSeq), Some(AllTypes.bv(id)))
    if (row.schema.fieldNames.contains("sv"))
      field(
        "sv",
        opt[scala.collection.Map[Long, Float]]("sv").map(_.toMap),
        Some(AllTypes.sv(id))
      )
    out.result()
  }

  // ---- search baselines --------------------------------------------------------

  private val clusterDim = 128

  private lazy val centroids: Array[Array[Float]] = Array.tabulate(200) { c =>
    val random = new java.util.Random(0x51ed270bL * (c + 1))
    Array.fill(clusterDim)(random.nextFloat() * 10f)
  }

  private def clustered(id: Long): Array[Float] = {
    val random = new java.util.Random(id * 0x9e3779b97f4a7c15L)
    val around = centroids((id % 200).toInt)
    Array.tabulate(clusterDim)(d =>
      around(d) + (random.nextGaussian().toFloat * 0.35f)
    )
  }

  private lazy val clusteredBase: Map[Long, Array[Float]] =
    (0L until 100000L).map(id => id -> clustered(id)).toMap

  private def binary(id: Long): Array[Byte] = {
    val random =
      new java.util.Random(id * 0x9e3779b97f4a7c15L + 0x2545f4914f6cdd1dL)
    Array.fill(64)((random.nextInt() & 0xff).toByte)
  }

  private def jitter(
      v: Array[Float],
      seed: Long,
      amount: Float
  ): Array[Float] = {
    val random = new java.util.Random(seed)
    v.map(x => x + (random.nextFloat() - 0.5f) * amount)
  }

  private def score(
      metric: String,
      q: Array[Float],
      v: Array[Float]
  ): Double = {
    var dot = 0.0d; var qq = 0.0d; var vv = 0.0d; var l2 = 0.0d
    var i = 0
    while (i < q.length) {
      val a = q(i).toDouble; val b = v(i).toDouble
      dot += a * b; qq += a * a; vv += b * b; l2 += (a - b) * (a - b)
      i += 1
    }
    metric match {
      case "L2"     => l2
      case "IP"     => dot
      case "COSINE" => dot / (math.sqrt(qq) * math.sqrt(vv))
    }
  }

  private def bitScore(
      metric: String,
      q: Array[Byte],
      v: Array[Byte]
  ): Double = {
    var differ = 0; var both = 0; var either = 0
    var i = 0
    while (i < q.length) {
      differ += Integer.bitCount((q(i) ^ v(i)) & 0xff)
      both += Integer.bitCount((q(i) & v(i)) & 0xff)
      either += Integer.bitCount((q(i) | v(i)) & 0xff)
      i += 1
    }
    metric match {
      case "HAMMING" => differ.toDouble
      case "JACCARD" => 1.0 - both.toDouble / either.toDouble
    }
  }

  private def smallerIsBetter(metric: String): Boolean =
    Set("L2", "HAMMING", "JACCARD").contains(metric)

  private def vectorQueries(queries: Seq[(Long, Array[Float])]): DataFrame =
    spark.createDataFrame(
      queries.map { case (id, v) => Row(id, v.toSeq) }.asJava,
      StructType(
        Seq(
          StructField("query_id", LongType, nullable = false),
          StructField("vector", ArrayType(FloatType, containsNull = false))
        )
      )
    )

  private def bitQueries(queries: Seq[(Long, Array[Byte])]): DataFrame =
    spark.createDataFrame(
      queries.map { case (id, v) => Row(id, v) }.asJava,
      StructType(
        Seq(
          StructField("query_id", LongType, nullable = false),
          StructField("vector", BinaryType)
        )
      )
    )

  private def search(
      path: String,
      queries: DataFrame,
      field: String,
      k: Int,
      metric: String,
      filter: Option[String] = None,
      output: Seq[String] = Seq("id")
  ): Map[Long, Seq[Row]] =
    MilvusSearch
      .search(
        spark,
        storageOptions() + (MilvusOption.SnapshotPath -> path),
        queries,
        field,
        k,
        metric,
        mode = "exact",
        filter = filter,
        outputColumns = output
      )
      .orderBy("query_id", "rank")
      .collect()
      .toSeq
      .groupBy(_.getAs[Long]("query_id"))

  private def checkHits(
      label: String,
      hits: Seq[Row],
      truth: Long => Double,
      candidates: Seq[Long],
      metric: String,
      k: Int,
      tolerance: Double
  ): Unit = {
    val better = if (smallerIsBetter(metric)) 1.0 else -1.0
    val expected = candidates.map(truth).sortBy(_ * better).take(k)
    val gotIds = hits.map(_.getAs[Long]("id"))
    val allowed = candidates.toSet
    same(
      hits.map(_.getAs[Int]("rank")),
      (1 to expected.size).toSeq,
      s"$label ranks"
    )
    same(gotIds.distinct.size, gotIds.size, s"$label returned a row twice")
    same(
      gotIds.filterNot(allowed),
      Seq.empty[Long],
      s"$label returned rows outside the candidates"
    )
    hits.map(_.getAs[Double]("_score")).zip(expected).zipWithIndex.foreach {
      case ((got, want), rank) =>
        near(got, want, tolerance, s"$label rank ${rank + 1} score")
    }
    hits.foreach { hit =>
      val id = hit.getAs[Long]("id")
      near(
        hit.getAs[Double]("_score"),
        truth(id),
        tolerance,
        s"$label score of id $id"
      )
    }
  }

  // ---- the cases ----------------------------------------------------------------

  private def readCases: Seq[Case] = Seq(
    Case(
      "R-01",
      "read",
      "all_types values on both outlets",
      () => {
        val path = ds("ALL_TYPES_SNAPSHOT_NODEL")
        val outlets = Seq(true, false).map { columnar =>
          val rows = snapshot(path, columnar).collect()
          same(rows.length, 1000, s"columnar=$columnar row count")
          same(
            rows.map(_.getAs[Long]("id")).sorted.toSeq,
            (0L until 1000L).toSeq,
            s"columnar=$columnar ids"
          )
          val mismatches = rows.toSeq.flatMap(allTypesMismatches)
          check(
            mismatches.isEmpty,
            s"columnar=$columnar value mismatches: ${mismatches.take(6).mkString("; ")}"
          )
          rows
            .map(r =>
              r.getAs[Long]("id") -> r.toSeq.map {
                case bytes: Array[Byte] => bytes.toSeq
                case other              => other
              }
            )
            .toMap
        }
        same(outlets(0), outlets(1), "the two outlets disagree")
      }
    ),
    Case(
      "R-02",
      "read",
      "all_types_vpk VarChar keys",
      () => {
        val rows = snapshot(ds("ALL_TYPES_VPK_SNAPSHOT_NODEL"))
          .select("id", "ord")
          .collect()
        same(rows.length, 1000, "row count")
        rows.foreach { row =>
          val ord = row.getAs[Long]("ord")
          same(
            row.getAs[String]("id"),
            f"pk-$ord%04d" + Seq("", "-é", "-\uD83D\uDE00")((ord % 3).toInt),
            s"key of ord $ord"
          )
        }
      }
    ),
    Case(
      "R-03",
      "read",
      "three delete states and apply.deletes=false",
      () => {
        Seq("ALL_TYPES" -> 1000L, "SEARCH_CL" -> 100000L, "PARTS" -> 3500L)
          .foreach { case (set, n) =>
            same(
              snapshot(ds(s"${set}_SNAPSHOT_NODEL")).count(),
              n,
              s"$set without deletes"
            )
            val l0 = snapshot(ds(s"${set}_SNAPSHOT_L0DEL"))
            same(l0.count(), n - 100, s"$set with L0 deletes")
            same(
              l0.filter(col("id") < 100).count(),
              0L,
              s"$set deleted ids came back"
            )
            val compacted = snapshot(ds(s"${set}_SNAPSHOT_COMPACTED"))
            same(compacted.count(), n - 100, s"$set compacted")
            same(
              compacted.filter(col("id") < 100).count(),
              0L,
              s"$set deleted ids came back after compaction"
            )
            same(
              snapshot(
                ds(s"${set}_SNAPSHOT_L0DEL"),
                extra = Seq(MilvusOption.ReadApplyDeletes -> "false")
              ).count(),
              n,
              s"$set with deletes switched off"
            )
          }
      }
    ),
    Case(
      "R-05",
      "read",
      "pushed predicates equal Spark's own filtering",
      () => {
        val base = snapshot(ds("ALL_TYPES_SNAPSHOT_NODEL"))
        val local =
          spark.createDataFrame(base.collect().toList.asJava, base.schema)
        Seq(
          "i32 > 0",
          "i64 <= 0",
          "s = 'row-0005'",
          "i16 in (-32768, -32731, 12)",
          "s_null is null",
          "s_null is not null",
          "s like 'row-00%'",
          "s like '%9'",
          "i32 > 0 and b",
          "i8 < 0 or opt_i32 is null",
          "not (opt_i32 > 100)",
          "opt_i32 = 3 or opt_i32 is null",
          "opt_f >= 10.5",
          "d between -10 and 10",
          "not (s_null = 'opt-1')",
          "not (i32 > 0 and opt_f < 100)"
        ).foreach { predicate =>
          same(
            ids(base.filter(expr(predicate))),
            ids(local.filter(expr(predicate))),
            s"predicate '$predicate'"
          )
        }
      }
    ),
    Case(
      "R-07",
      "read",
      "milvus.filter alone, combined, with deletes, and a syntax error",
      () => {
        val path = ds("ALL_TYPES_SNAPSHOT_NODEL")
        val expected =
          (0L until 1000L).filter(i => AllTypes.i32(i) > 0 && i % 2 == 0)
        same(
          ids(
            snapshot(
              path,
              extra = Seq(MilvusOption.MilvusFilter -> "i32 > 0 and b == true")
            )
          ),
          expected,
          "filter alone"
        )
        same(
          ids(
            snapshot(
              path,
              extra = Seq(MilvusOption.MilvusFilter -> "i32 > 0 and b == true")
            ).filter(col("id") < 700)
          ),
          expected.filter(_ < 700),
          "filter with a Spark predicate"
        )
        same(
          ids(
            snapshot(
              ds("ALL_TYPES_SNAPSHOT_L0DEL"),
              extra = Seq(MilvusOption.MilvusFilter -> "id < 150")
            )
          ),
          (100L until 150L).toSeq,
          "filter with deletes"
        )
        same(
          snapshot(path, extra = Seq(MilvusOption.MilvusFilter -> "id < 500"))
            .limit(5)
            .count(),
          5L,
          "filter with a limit"
        )
        failure("a filter with bad syntax") {
          snapshot(path, extra = Seq(MilvusOption.MilvusFilter -> "i32 >>> 0"))
            .count()
        }
      }
    ),
    Case(
      "R-09",
      "read",
      "metadata columns and _row_offset",
      () => {
        val nodel = snapshot(ds("PARTS_SNAPSHOT_NODEL"), extra = extraColumns)
          .select("id", "_segment_id", "_row_offset", "_timestamp")
          .collect()
        same(nodel.length, 3500, "row count")
        nodel.groupBy(_.getAs[Long]("_segment_id")).foreach {
          case (segment, rows) =>
            same(
              rows.map(_.getAs[Long]("_row_offset")).sorted.toSeq,
              (0L until rows.length.toLong).toSeq,
              s"offsets of segment $segment"
            )
        }
        check(
          nodel.forall(_.getAs[Long]("_timestamp") > 0L),
          "a _timestamp was not positive"
        )
        same(
          snapshot(
            ds("PARTS_SNAPSHOT_NODEL"),
            extra = Seq(MilvusOption.MilvusExtraColumns -> "_row_offset")
          ).select("_row_offset").count(),
          3500L,
          "_row_offset alone"
        )
        val byId = nodel
          .map(r =>
            r.getAs[Long]("id") -> (r.getAs[Long]("_segment_id"), r
              .getAs[Long]("_row_offset"))
          )
          .toMap
        val l0 = snapshot(ds("PARTS_SNAPSHOT_L0DEL"), extra = extraColumns)
          .select("id", "_segment_id", "_row_offset")
          .collect()
        same(l0.length, 3400, "row count after deletes")
        l0.foreach { r =>
          val id = r.getAs[Long]("id")
          same(
            (r.getAs[Long]("_segment_id"), r.getAs[Long]("_row_offset")),
            byId(id),
            s"position of id $id moved after deletes"
          )
        }
      }
    ),
    Case(
      "R-10",
      "read",
      "partition and segment selection",
      () => {
        val partitions = need("CT_PARTS_PARTITIONS")
          .split(',')
          .map(_.split(':'))
          .map(p => p(0) -> p(1))
          .toMap
        val path = ds("PARTS_SNAPSHOT_NODEL")
        def count(extra: (String, String)*): Long =
          snapshot(path, extra = extra).count()
        same(
          count(MilvusOption.MilvusPartitions -> partitions("_default")),
          1000L,
          "_default"
        )
        same(
          count(MilvusOption.MilvusPartitions -> partitions("p1")),
          1500L,
          "p1"
        )
        same(
          count(MilvusOption.MilvusPartitions -> partitions("p2")),
          1000L,
          "p2"
        )
        same(
          count(
            MilvusOption.MilvusPartitions -> s"${partitions("_default")},${partitions("p2")}"
          ),
          2000L,
          "_default and p2"
        )
        val segments = snapshot(path, extra = extraColumns)
          .groupBy("_segment_id")
          .count()
          .collect()
          .map(r => r.getLong(0) -> r.getLong(1))
          .toMap
        same(segments.values.sum, 3500L, "segment rows do not add up")
        segments.foreach { case (segment, rows) =>
          same(
            count(MilvusOption.MilvusSegments -> segment.toString),
            rows,
            s"segment $segment"
          )
        }
        val p1Segments = snapshot(
          path,
          extra =
            extraColumns :+ (MilvusOption.MilvusPartitions -> partitions("p1"))
        )
          .select("_segment_id")
          .distinct()
          .collect()
          .map(_.getLong(0))
        same(p1Segments.length, 2, "p1 segment count")
        same(
          count(
            MilvusOption.MilvusPartitions -> partitions("p1"),
            MilvusOption.MilvusSegments -> p1Segments.head.toString
          ),
          segments(p1Segments.head),
          "partition and segment together"
        )
        failure("a segment of another partition")(
          count(
            MilvusOption.MilvusPartitions -> partitions("p2"),
            MilvusOption.MilvusSegments -> p1Segments.head.toString
          )
        )
        failure("a segment id that is not in the snapshot")(
          count(MilvusOption.MilvusSegments -> "1")
        )
        same(
          snapshot(
            ds("PARTS_SNAPSHOT_L0DEL"),
            extra = Seq(MilvusOption.MilvusPartitions -> partitions("_default"))
          ).count(),
          900L,
          "deletes inside a selected partition"
        )
      }
    ),
    Case(
      "R-11",
      "read",
      "limit",
      () => {
        val path = ds("ALL_TYPES_SNAPSHOT_NODEL")
        same(snapshot(path).limit(7).count(), 7L, "limit 7")
        same(
          snapshot(path).select("id").limit(7).collect().length,
          7,
          "limit with projection"
        )
        same(
          snapshot(path, columnar = false).limit(3).collect().length,
          3,
          "limit on the row path"
        )
      }
    ),
    Case(
      "R-14",
      "read",
      "table statistics",
      () => {
        val stats = snapshot(
          ds("ALL_TYPES_SNAPSHOT_NODEL")
        ).queryExecution.optimizedPlan.stats
        same(stats.rowCount, Some(BigInt(1000)), "row count estimate")
        snapshot(
          ds("ALL_TYPES_SNAPSHOT_L0DEL")
        ).queryExecution.optimizedPlan.stats.rowCount.foreach { rows =>
          check(
            rows >= BigInt(900),
            s"the estimate $rows is below the 900 rows the snapshot has"
          )
        }
      }
    ),
    Case(
      "R-17",
      "read",
      "vector.raw bytes decode to the default read",
      () => {
        val path = ds("ALL_TYPES_SNAPSHOT_NODEL")
        val decoded = snapshot(path)
          .select("id", "fv", "fv16", "bf16", "i8v", "bv")
          .collect()
          .map(r => r.getAs[Long]("id") -> r)
          .toMap
        val raw =
          snapshot(path, extra = Seq(MilvusOption.ReadVectorRaw -> "true"))
            .select("id", "fv", "fv16", "bf16", "i8v", "bv")
            .collect()
        same(raw.length, 1000, "row count")
        raw.foreach { r =>
          val id = r.getAs[Long]("id")
          val d = decoded(id)
          val fv = ByteBuffer
            .wrap(r.getAs[Array[Byte]]("fv"))
            .order(ByteOrder.LITTLE_ENDIAN)
          same(
            (0 until 8).map(_ => fv.getFloat),
            d.getSeq[Float](d.fieldIndex("fv")).toSeq,
            s"fv of id $id"
          )
          val f16 = ByteBuffer
            .wrap(r.getAs[Array[Byte]]("fv16"))
            .order(ByteOrder.LITTLE_ENDIAN)
          same(
            (0 until 8).map(_ => halfBitsToFloat(f16.getShort & 0xffff)),
            AllTypes.fv16(id),
            s"fv16 of id $id"
          )
          val b16 = ByteBuffer
            .wrap(r.getAs[Array[Byte]]("bf16"))
            .order(ByteOrder.LITTLE_ENDIAN)
          same(
            (0 until 8).map(_ =>
              java.lang.Float.intBitsToFloat((b16.getShort & 0xffff) << 16)
            ),
            AllTypes.bf16(id),
            s"bf16 of id $id"
          )
          same(
            r.getAs[Array[Byte]]("i8v").toSeq.map(_.toShort),
            AllTypes.i8v(id),
            s"i8v of id $id"
          )
          same(
            r.getAs[Array[Byte]]("bv").toSeq,
            AllTypes.bv(id),
            s"bv of id $id"
          )
        }
      }
    ),
    Case(
      "R-19",
      "read",
      "small batches and a too small Arrow budget",
      () => {
        val path = ds("ALL_TYPES_SNAPSHOT_NODEL")
        def digest(frame: DataFrame): (Long, Long) = {
          val r =
            frame.agg(count(lit(1)), sum(col("i32").cast(LongType))).head()
          (r.getLong(0), r.getLong(1))
        }
        val reference = digest(snapshot(path))
        same(
          digest(
            snapshot(path, extra = Seq(MilvusOption.ReadBatchMaxRows -> "7"))
          ),
          reference,
          "batch.max.rows=7"
        )
        same(
          digest(
            snapshot(
              path,
              columnar = false,
              extra = Seq(MilvusOption.ReadBatchMaxRows -> "7")
            )
          ),
          reference,
          "row path with batch.max.rows=7"
        )
        same(
          digest(
            snapshot(
              path,
              extra = Seq(MilvusOption.ReadBatchMaxBytes -> "4096")
            )
          ),
          reference,
          "batch.max.bytes=4096"
        )
        val small = failure("arrow.max.bytes=4096") {
          snapshot(path, extra = Seq(MilvusOption.ReadArrowMaxBytes -> "4096"))
            .collect()
        }
        check(
          Option(small.getMessage).exists(m =>
            m.contains("arrow") || m.contains("Arrow") || m.contains("bytes")
          ),
          s"the failure does not name the budget: ${small.getClass.getSimpleName}: ${small.getMessage}"
        )
      }
    ),
    Case(
      "R-20",
      "read",
      "wide rows across column groups",
      () => {
        val rows = snapshot(ds("WIDE_SNAPSHOT_NODEL")).collect()
        same(rows.length, 1000, "row count")
        rows.foreach { r =>
          val id = r.getAs[Long]("id")
          same(
            r.getAs[String]("vc4k"),
            (f"$id%04d-abcdefghij" * 512).take(4096),
            s"vc4k of id $id"
          )
          val j = json.readTree(r.getAs[String]("j"))
          same(j.get("id").asLong(), id, s"json id of $id")
          same(j.get("ints").size(), 500, s"json ints of $id")
          same(
            r.getAs[String]("t"),
            (0 until 300).map(k => s"w${(id * 7 + k) % 997}").mkString(" "),
            s"text of id $id"
          )
          same(
            r.getSeq[Float](r.fieldIndex("v")).toSeq,
            Seq(id.toFloat, (id % 7).toFloat, (id % 11).toFloat, 1.0f),
            s"vector of id $id"
          )
        }
      }
    )
  )

  private def searchCases: Seq[Case] = Seq(
    Case(
      "S-01",
      "search",
      "exact L2, IP and COSINE against brute force",
      () => {
        val path = ds("SEARCH_CL_SNAPSHOT_L0DEL")
        val targets = Seq(1000L, 25000L, 77777L)
        val queries = targets.map(t => t -> jitter(clusteredBase(t), t, 0.5f))
        val candidates = (100L until 100000L).toSeq
        for (metric <- Seq("L2", "IP", "COSINE"); k <- Seq(10, 100)) {
          val found = search(path, vectorQueries(queries), "v", k, metric)
          queries.foreach { case (qid, q) =>
            checkHits(
              s"$metric k=$k query $qid",
              found(qid),
              id => score(metric, q, clusteredBase(id)),
              candidates,
              metric,
              k,
              1e-3
            )
          }
        }
      }
    ),
    Case(
      "S-03",
      "search",
      "binary vectors with HAMMING and JACCARD",
      () => {
        val path = ds("SEARCH_BIN_SNAPSHOT_L0DEL")
        val base = (0L until 100000L).map(id => id -> binary(id)).toMap
        val queries = Seq(1234L, 88888L).map { t =>
          val v = base(t).clone(); v(0) = (v(0) ^ 0x05).toByte;
          v(7) = (v(7) ^ 0x80).toByte
          t -> v
        }
        for (metric <- Seq("HAMMING", "JACCARD")) {
          val found = search(path, bitQueries(queries), "bv", 10, metric)
          queries.foreach { case (qid, q) =>
            checkHits(
              s"$metric query $qid",
              found(qid),
              id => bitScore(metric, q, base(id)),
              (100L until 100000L).toSeq,
              metric,
              10,
              1e-6
            )
          }
        }
      }
    ),
    Case(
      "S-04",
      "search",
      "Float16, BFloat16 and Int8 fields",
      () => {
        val path = ds("SEARCH_MIXED_SNAPSHOT_L0DEL")
        val candidates = (100L until 100000L).toSeq
        val q = jitter(clusteredBase(3131L), 3131L, 0.5f)
        Seq(
          ("v_f16", (v: Array[Float]) => v.map(float16)),
          ("v_bf16", (v: Array[Float]) => v.map(bfloat16))
        ).foreach { case (field, encode) =>
          val sample = snapshot(path)
            .filter(col("id").isin(1000L, 2000L, 3000L))
            .select("id", field)
            .collect()
          same(sample.length, 3, s"$field sample size")
          sample.foreach { row =>
            val id = row.getAs[Long]("id")
            same(
              row.getSeq[Float](row.fieldIndex(field)).toSeq,
              encode(clusteredBase(id)).toSeq,
              s"$field stored value of id $id"
            )
          }
          val encodedQuery = encode(q)
          val truth =
            (id: Long) => score("L2", encodedQuery, encode(clusteredBase(id)))
          val found =
            search(path, vectorQueries(Seq(1L -> q)), field, 10, "L2")(1L)
          val best15 = candidates
            .map(id => id -> truth(id))
            .sortBy(_._2)
            .take(15)
            .map(_._1)
            .toSet
          val outside = found.map(_.getAs[Long]("id")).filterNot(best15)
          same(
            outside,
            Seq.empty[Long],
            s"$field returned ids outside the true top 15"
          )
          val drift = found
            .map(hit =>
              math.abs(
                hit.getAs[Double]("_score") - truth(hit.getAs[Long]("id"))
              ) / math.max(1.0, truth(hit.getAs[Long]("id")))
            )
            .max
          check(
            drift <= 0.05,
            f"$field score drift against the decoded baseline is $drift%.4f"
          )
        }
        val i8 = (v: Array[Float]) =>
          v.map(x =>
            math.max(-128f, math.min(127f, math.rint(x * 10.0).toFloat))
          )
        val qi8 = i8(q)
        val int8Queries = spark.createDataFrame(
          Seq(Row(1L, qi8.toSeq.map(_.toShort))).asJava,
          StructType(
            Seq(
              StructField("query_id", LongType, nullable = false),
              StructField("vector", ArrayType(ShortType, containsNull = false))
            )
          )
        )
        val found = search(path, int8Queries, "v_i8", 10, "L2")
        checkHits(
          "v_i8",
          found(1L),
          id => score("L2", qi8, i8(clusteredBase(id))),
          candidates,
          "L2",
          10,
          1e-6
        )
      }
    ),
    Case(
      "S-05",
      "search",
      "a filter restricts the candidates",
      () => {
        val path = ds("SEARCH_CL_SNAPSHOT_L0DEL")
        val q = jitter(clusteredBase(4243L), 4243L, 0.5f)
        val labelled = search(
          path,
          vectorQueries(Seq(1L -> q)),
          "v",
          10,
          "L2",
          filter = Some("label == 3")
        )
        checkHits(
          "label == 3",
          labelled(1L),
          id => score("L2", q, clusteredBase(id)),
          (100L until 100000L).filter(_ % 10 == 3).toSeq,
          "L2",
          10,
          1e-3
        )
        val few = search(
          path,
          vectorQueries(Seq(2L -> q)),
          "v",
          10,
          "L2",
          filter = Some("id < 110 and label == 3")
        )
        same(
          few(2L).map(_.getAs[Long]("id")),
          Seq(103L),
          "fewer valid rows than k"
        )
      }
    ),
    Case(
      "S-06",
      "search",
      "a nullable vector field",
      () => {
        val path = ds("SEARCH_CL_SNAPSHOT_L0DEL")
        val q = jitter(clusteredBase(5550L), 5550L, 0.5f)
        val found =
          search(path, vectorQueries(Seq(1L -> q)), "v_null", 20, "L2")
        checkHits(
          "v_null",
          found(1L),
          id => score("L2", q, clusteredBase(id)),
          (100L until 100000L).filter(_ % 10 != 0).toSeq,
          "L2",
          20,
          1e-3
        )
      }
    ),
    Case(
      "S-10",
      "search",
      "output columns",
      () => {
        val path = ds("SEARCH_CL_SNAPSHOT_L0DEL")
        val queries = vectorQueries(Seq(1L -> clusteredBase(1000L)))
        val options = storageOptions() + (MilvusOption.SnapshotPath -> path)
        same(
          MilvusSearch
            .search(spark, options, queries, "v", 3, "L2", mode = "exact")
            .columns
            .toSeq,
          Seq("query_id", "rank", "_score", "_segment_id", "_row_offset"),
          "the five fixed columns"
        )
        same(
          MilvusSearch
            .search(
              spark,
              options,
              queries,
              "v",
              3,
              "L2",
              mode = "exact",
              outputColumns = Seq("label", "id")
            )
            .columns
            .toSeq
            .takeRight(2),
          Seq("label", "id"),
          "output columns keep their order"
        )
        failure("a reserved output column")(
          MilvusSearch.search(
            spark,
            options,
            queries,
            "v",
            3,
            "L2",
            mode = "exact",
            outputColumns = Seq("rank")
          )
        )
      }
    ),
    Case(
      "S-12",
      "search",
      "query sets the search cannot answer",
      () => {
        val path = ds("SEARCH_CL_SNAPSHOT_L0DEL")
        val good = clusteredBase(1000L)
        val options = storageOptions() + (MilvusOption.SnapshotPath -> path)
        def attempt(
            label: String,
            queries: DataFrame,
            k: Int = 3,
            metric: String = "L2"
        ): Unit =
          failure(label)(
            MilvusSearch
              .search(spark, options, queries, "v", k, metric, mode = "exact")
              .collect()
          )
        attempt(
          "duplicate query_id",
          vectorQueries(Seq(1L -> good, 1L -> good))
        )
        attempt("wrong dimension", vectorQueries(Seq(1L -> good.take(64))))
        attempt(
          "NaN element",
          vectorQueries(Seq(1L -> good.updated(0, Float.NaN)))
        )
        attempt("k = 0", vectorQueries(Seq(1L -> good)), k = 0)
        attempt(
          "unknown metric",
          vectorQueries(Seq(1L -> good)),
          metric = "MANHATTAN"
        )
        attempt(
          "null vector",
          spark.createDataFrame(
            Seq(Row(1L, null)).asJava,
            StructType(
              Seq(
                StructField("query_id", LongType, nullable = false),
                StructField(
                  "vector",
                  ArrayType(FloatType, containsNull = false)
                )
              )
            )
          )
        )
      }
    )
  )

  /** A procedure takes its connection as backquoted option arguments of the
    * CALL statement. The values come from the environment so that no token is
    * written into the application spec.
    */
  private def callOptions: String = {
    val options = storageOptions() ++ Map(
      MilvusOption.MilvusUri -> need("MILVUS_UAT_URI"),
      MilvusOption.MilvusToken -> need("MILVUS_UAT_TOKEN")
    )
    options.toSeq
      .sortBy(_._1)
      .map { case (key, value) =>
        s", `$key` => '${value.replace("'", "''")}'"
      }
      .mkString
  }

  private def procedureCases: Seq[Case] = Seq(
    Case(
      "A-01",
      "procedure",
      "create, list, describe and drop a snapshot",
      () => {
        val options = callOptions
        val collection = env("CT_PROCEDURE_COLLECTION").getOrElse("all_types")
        val name = s"spark_milvus_ct_${System.currentTimeMillis()}"
        val created = spark
          .sql(
            s"CALL milvus.system.create_snapshot(collection => '$collection', name => '$name', description => 'correctness run'$options)"
          )
          .collect()
        same(created.length, 1, "create_snapshot returned no metadata")
        val location = created.head.getAs[String]("s3_location")
        check(
          location != null && location.nonEmpty,
          "create_snapshot returned no s3_location"
        )
        val listed = spark
          .sql(
            s"CALL milvus.system.list_snapshots(collection => '$collection'$options)"
          )
          .collect()
        check(
          listed.exists(_.getAs[String]("snapshot") == name),
          s"list_snapshots does not show $name"
        )
        val described = spark
          .sql(
            s"CALL milvus.system.describe_snapshot(collection => '$collection', name => '$name'$options)"
          )
          .collect()
        same(described.length, 1, "describe_snapshot returned no row")
        same(
          described.head.getAs[String]("s3_location"),
          location,
          "describe_snapshot disagrees with create_snapshot"
        )
        // The snapshot just made is readable through the connector.
        same(
          snapshot(location).count(),
          1000L - 100L,
          "the new snapshot reads back the surviving rows"
        )
        spark
          .sql(
            s"CALL milvus.system.drop_snapshot(collection => '$collection', name => '$name'$options)"
          )
          .collect()
        val after = spark
          .sql(
            s"CALL milvus.system.list_snapshots(collection => '$collection'$options)"
          )
          .collect()
        check(
          !after.exists(_.getAs[String]("snapshot") == name),
          s"drop_snapshot left $name behind"
        )
      }
    ),
    Case(
      "A-04",
      "procedure",
      "describe a collection",
      () => {
        val collection = env("CT_PROCEDURE_COLLECTION").getOrElse("all_types")
        val described = spark
          .sql(
            s"CALL milvus.system.describe(collection => '$collection'${callOptions})"
          )
          .collect()
        check(described.nonEmpty, "describe returned nothing")
        val names = described.map(_.getAs[String]("field_name")).toSet
        check(
          Set("id", "fv", "bf16").subsetOf(names),
          s"describe is missing fields: $names"
        )
        same(
          described.map(_.getAs[Long]("collection_id")).distinct.length,
          1,
          "describe reports more than one collection id"
        )
        check(
          described.forall(_.getAs[Long]("segment_count") > 0L),
          "describe reports no segments"
        )
      }
    ),
    Case(
      "A-05",
      "procedure",
      "positional, named and backquoted arguments, and the errors",
      () => {
        val collection = env("CT_PROCEDURE_COLLECTION").getOrElse("all_types")
        val byName = spark
          .sql(
            s"CALL milvus.system.list_snapshots(collection => '$collection'${callOptions})"
          )
          .collect()
        val byPosition = spark
          .sql(
            s"CALL milvus.system.list_snapshots('$collection'${callOptions})"
          )
          .collect()
        same(
          byPosition.map(_.getAs[String]("snapshot")).toSeq,
          byName.map(_.getAs[String]("snapshot")).toSeq,
          "a positional argument does not equal the named one"
        )
        val byDatabase = spark
          .sql(
            s"CALL milvus.system.list_snapshots('default.$collection'${callOptions})"
          )
          .collect()
        same(
          byDatabase.map(_.getAs[String]("snapshot")).toSeq,
          byName.map(_.getAs[String]("snapshot")).toSeq,
          "db.collection does not equal the bare collection name"
        )
        def rejects(label: String, statement: String): Unit = {
          val message = Option(
            failure(label)(spark.sql(statement).collect()).getMessage
          ).getOrElse("")
          check(message.nonEmpty, s"$label failed without a message")
        }
        rejects(
          "an unknown argument name",
          s"CALL milvus.system.list_snapshots(collection => '$collection', who => 'x'${callOptions})"
        )
        rejects(
          "a positional argument after a named one",
          s"CALL milvus.system.list_snapshots(collection => '$collection', '$collection'${callOptions})"
        )
        rejects(
          "the same argument twice",
          s"CALL milvus.system.list_snapshots(collection => '$collection', collection => '$collection'${callOptions})"
        )
        rejects(
          "an unknown procedure",
          s"CALL milvus.system.describe_collection(collection => '$collection'${callOptions})"
        )
        rejects(
          "no connection option",
          s"CALL milvus.system.list_snapshots(collection => '$collection')"
        )
        // The message of the unknown-argument error lists what the procedure takes.
        val unknown = Option(
          failure("unknown argument message")(
            spark
              .sql(
                s"CALL milvus.system.create_snapshot(collection => '$collection', snapshot => 'x'${callOptions})"
              )
              .collect()
          ).getMessage
        ).getOrElse("")
        check(
          unknown.contains("name") && unknown.contains("collection"),
          s"the error does not list the parameters: $unknown"
        )
      }
    ),
    Case(
      "A-02",
      "procedure",
      "create_index waiting and not waiting, then drop_index",
      () => {
        withOwnCollection { collection =>
          // CREATE TABLE already made the index v_auto on v; drop it so create_index has the field.
          // (describe cannot list indexes here: Zilliz Cloud denies GetPersistentSegmentInfo, A-04.)
          val dropped = spark
            .sql(
              s"CALL milvus.system.drop_index(collection => '$collection', index_name => 'v_auto'${callOptions})"
            )
            .collect()
          same(
            dropped.head.getAs[String]("status"),
            "dropped",
            "drop_index of the CREATE TABLE index did not report dropped"
          )
          val waited = spark
            .sql(
              s"CALL milvus.system.create_index(collection => '$collection', field => 'v', index_name => 'v_idx'," +
                s" index_type => 'HNSW', metric_type => 'COSINE', params => '{\"M\":8,\"efConstruction\":64}'," +
                s" wait => true, timeout_seconds => 180${callOptions})"
            )
            .collect()
          same(waited.length, 1, "create_index returned no row")
          same(
            waited.head.getAs[String]("state"),
            "Finished",
            s"create_index state ${waited.head.getAs[String]("state")}"
          )
          // A second index on the same field is refused while v_idx exists.
          failure("a second index on the same field")(
            spark
              .sql(
                s"CALL milvus.system.create_index(collection => '$collection', field => 'v', index_name => 'v_other'," +
                  s" index_type => 'IVF_FLAT', metric_type => 'L2', params => '{\"nlist\":16}', wait => true${callOptions})"
              )
              .collect()
          )
          val droppedIdx = spark
            .sql(
              s"CALL milvus.system.drop_index(collection => '$collection', index_name => 'v_idx'${callOptions})"
            )
            .collect()
          same(
            droppedIdx.head.getAs[String]("status"),
            "dropped",
            "drop_index did not report dropped"
          )
          val notWaited = spark
            .sql(
              s"CALL milvus.system.create_index(collection => '$collection', field => 'v', index_name => 'v_idx'," +
                s" index_type => 'HNSW', metric_type => 'COSINE', wait => false${callOptions})"
            )
            .collect()
          same(
            notWaited.length,
            1,
            "create_index without waiting returned no row"
          )
          same(
            notWaited.head.getAs[String]("state"),
            "submitted",
            s"create_index without waiting reports ${notWaited.head.getAs[String]("state")}"
          )
          spark
            .sql(
              s"CALL milvus.system.drop_index(collection => '$collection', index_name => 'v_idx'${callOptions})"
            )
            .collect()
        }
      }
    ),
    Case(
      "A-03",
      "procedure",
      "load, release, flush and compact",
      () => {
        withOwnCollection { collection =>
          // The load state is read from the procedures' own rows: describe cannot
          // run against Zilliz Cloud (A-04).
          val loaded = spark
            .sql(
              s"CALL milvus.system.load(collection => '$collection', wait => true, timeout_seconds => 180${callOptions})"
            )
            .collect()
          same(loaded.length, 1, "load returned no row")
          same(
            loaded.head.getAs[String]("state"),
            "LoadStateLoaded",
            s"load with wait reports ${loaded.head.getAs[String]("state")}"
          )
          // While loaded, Milvus refuses to drop the index; that shows the load took effect.
          failure("drop_index while loaded")(
            spark
              .sql(
                s"CALL milvus.system.drop_index(collection => '$collection', index_name => 'v_auto'${callOptions})"
              )
              .collect()
          )
          val released = spark
            .sql(
              s"CALL milvus.system.release(collection => '$collection'${callOptions})"
            )
            .collect()
          same(
            released.head.getAs[String]("status"),
            "released",
            "release did not report released"
          )
          val submitted = spark
            .sql(
              s"CALL milvus.system.load(collection => '$collection', wait => false${callOptions})"
            )
            .collect()
          same(
            submitted.head.getAs[String]("state"),
            "submitted",
            s"load without wait reports ${submitted.head.getAs[String]("state")}"
          )
          spark
            .sql(
              s"CALL milvus.system.release(collection => '$collection'${callOptions})"
            )
            .collect()
          val flushed = spark
            .sql(
              s"CALL milvus.system.flush(collection => '$collection'${callOptions})"
            )
            .collect()
          same(flushed.length, 1, "flush returned no row")
          val compacted = spark
            .sql(
              s"CALL milvus.system.compact(collection => '$collection', wait => true, timeout_seconds => 180${callOptions})"
            )
            .collect()
          same(compacted.length, 1, "compact returned no row")
          check(
            compacted.head.getAs[Long]("plan_count") >= 0L,
            "compact returned no plan count"
          )
        }
      }
    ),
    Case(
      "W-12",
      "procedure",
      "CREATE, SHOW, DESCRIBE and DROP TABLE through the catalog",
      () => {
        configureCatalog()
        withOwnCollection { collection =>
          val tables = spark
            .sql("SHOW TABLES IN milvus.default")
            .collect()
            .map(_.getAs[String]("tableName"))
            .toSet
          check(
            tables.contains(collection),
            s"SHOW TABLES does not list $collection"
          )
          val namespaces = spark
            .sql("SHOW NAMESPACES IN milvus")
            .collect()
            .map(_.getString(0))
            .toSet
          check(
            namespaces.contains("default"),
            s"SHOW NAMESPACES does not list default: $namespaces"
          )
          // DESCRIBE TABLE of the new collection is not attempted: a table loads
          // only once Milvus has produced a snapshot for it (reference-en.md).
          spark
            .sql(
              "DROP TABLE IF EXISTS milvus.default.no_such_table_spark_milvus_ct"
            )
            .collect()
          failure("DROP TABLE of an absent table without IF EXISTS")(
            spark
              .sql("DROP TABLE milvus.default.no_such_table_spark_milvus_ct")
              .collect()
          )
        }
        // The collection is gone once withOwnCollection drops it.
        val left = spark
          .sql("SHOW TABLES IN milvus.default")
          .collect()
          .map(_.getAs[String]("tableName"))
        check(
          !left.exists(_.startsWith("spark_milvus_ct_")),
          s"a spark_milvus_ct_ collection was left behind: ${left.filter(_.startsWith("spark_milvus_ct_")).mkString(",")}"
        )
      }
    )
  )

  /** Creates a collection of this run's own, hands its name to the body, and
    * drops it however the body ends.
    */
  private def withOwnCollection[A](body: String => A): A = {
    configureCatalog()
    // A collection an earlier attempt left behind is dropped first.
    spark
      .sql("SHOW TABLES IN milvus.default")
      .collect()
      .map(_.getAs[String]("tableName"))
      .filter(_.startsWith("spark_milvus_ct_"))
      .foreach(left =>
        spark.sql(s"DROP TABLE IF EXISTS milvus.default.$left").collect()
      )
    val name = s"spark_milvus_ct_${System.currentTimeMillis()}"
    spark.sql(s"""CREATE TABLE milvus.default.$name (id BIGINT NOT NULL, label INT, v ARRAY<FLOAT>)
         |TBLPROPERTIES (
         |  'milvus.primary.key' = 'id',
         |  'milvus.field.v.data_type' = 'float_vector',
         |  'milvus.field.v.dim' = '8',
         |  'milvus.index.v' = '{"index_type":"HNSW","metric_type":"COSINE","index_name":"v_auto","M":8,"efConstruction":64}'
         |)""".stripMargin)
    try body(name)
    finally spark.sql(s"DROP TABLE milvus.default.$name")
  }

  /** The catalog the Catalog-side cases go through. Its connection comes from
    * the environment so that no token is written into the application spec.
    */
  private def configureCatalog(): Unit = {
    spark.conf.set(
      "spark.sql.catalog.milvus",
      "com.zilliz.spark.connector.catalog.MilvusCatalog"
    )
    spark.conf.set(
      "spark.sql.catalog.milvus.milvus.uri",
      need("MILVUS_UAT_URI")
    )
    spark.conf.set(
      "spark.sql.catalog.milvus.milvus.token",
      need("MILVUS_UAT_TOKEN")
    )
    storageOptions().foreach { case (key, value) =>
      spark.conf.set(s"spark.sql.catalog.milvus.$key", value)
    }
  }

  // ---- write and index cases ---------------------------------------------------
  // Everything these cases write goes under this run's own prefix in the
  // artifact bucket; the snapshot URL keeps naming the instance bucket the data
  // is read from.

  private def outputBucket: String =
    need("CT_OUTPUT_BUCKET")
  private def outputRoot: String =
    env("CT_OUTPUT_PREFIX").getOrElse("spark-milvus-correctness")
  private def region: String =
    env("MILVUS_JNI_S3_REGION").getOrElse("us-west-2")
  private def outputPrefix(caseId: String): String =
    s"$outputRoot/${spark.sparkContext.applicationId}/${caseId.toLowerCase}"

  private def outputStorageOptions(caseId: String): Map[String, String] =
    storageOptions() ++ Map(
      StorageProperties.BucketName -> outputBucket,
      StorageProperties.RootPath -> outputPrefix(caseId)
    )

  private def optionArguments(options: Map[String, String]): String =
    options.toSeq
      .sortBy(_._1)
      .map { case (key, value) =>
        s", `$key` => '${value.replace("'", "''")}'"
      }
      .mkString

  private def s3a(key: String): Path = new Path(s"s3a://$outputBucket/$key")
  private def exists(key: String): Boolean =
    s3a(key)
      .getFileSystem(spark.sparkContext.hadoopConfiguration)
      .exists(s3a(key))
  private def entries(key: String): Seq[String] = {
    val path = s3a(key)
    val fs = path.getFileSystem(spark.sparkContext.hadoopConfiguration)
    if (!fs.exists(path)) Seq.empty
    else fs.listStatus(path).map(_.getPath.getName).toSeq.sorted
  }

  private def appendWrite(
      frame: DataFrame,
      source: String,
      caseId: String,
      extra: (String, String)*
  ): Unit = {
    var writer = frame.write
      .format("milvus")
      .mode("append")
      .option(MilvusOption.SnapshotPath, source)
    (outputStorageOptions(caseId) ++ extra).foreach { case (k, v) =>
      writer = writer.option(k, v)
    }
    writer.save()
  }

  private def searchWith(
      options: Map[String, String],
      queries: DataFrame,
      field: String,
      k: Int,
      metric: String,
      mode: String
  ): Map[Long, Seq[Long]] =
    MilvusSearch
      .search(
        spark,
        options,
        queries,
        field,
        k,
        metric,
        mode = mode,
        outputColumns = Seq("id")
      )
      .orderBy("query_id", "rank")
      .collect()
      .toSeq
      .groupBy(_.getAs[Long]("query_id"))
      .map { case (query, rows) => query -> rows.map(_.getAs[Long]("id")) }

  /** Twenty query vectors taken from the snapshot's own rows. */
  private def queriesFrom(path: String, field: String): DataFrame = {
    val rows = snapshot(path)
      .select("id", field)
      .filter(col("id") >= 500)
      .limit(20)
      .collect()
    vectorQueries(
      rows.toSeq.map(r => r.getAs[Long]("id") -> r.getSeq[Float](1).toArray)
    )
  }

  /** Builds an index over a snapshot, writes the snapshot that carries it, and
    * returns the URL of that snapshot.
    */
  private def buildAndSnapshot(
      caseId: String,
      source: String,
      collection: String,
      field: String,
      indexType: String,
      metric: String,
      params: String
  ): (String, Map[String, String]) = {
    val storage = outputStorageOptions(caseId)
    val arguments = optionArguments(
      storage ++ Map(
        MilvusOption.SnapshotPath -> source,
        MilvusOption.MilvusUri -> need("MILVUS_UAT_URI"),
        MilvusOption.MilvusToken -> need("MILVUS_UAT_TOKEN")
      )
    )
    val output = s"${outputPrefix(caseId)}/index"
    val paramsArgument = if (params.isEmpty) "" else s", params => '$params'"
    val built = spark
      .sql(
        s"CALL milvus.system.build_index('$collection', field => '$field', output => '$output'," +
          s" index_type => '$indexType', metric => '$metric'$paramsArgument$arguments)"
      )
      .collect()
    check(built.nonEmpty, "build_index returned no segment row")
    check(
      built.forall(_.getAs[Long]("objects") > 0L),
      "build_index reports a segment with no index objects"
    )
    val jobs = built.map(_.getAs[String]("job_id")).distinct
    same(jobs.length, 1, s"build_index reports ${jobs.length} job ids")
    val written = spark
      .sql(
        s"CALL milvus.system.write_snapshot('$collection', job => '${jobs.head}', input => '$output', restorable => false$arguments)"
      )
      .collect()
    same(written.length, 1, "write_snapshot returned no row")
    same(
      written.head.getAs[Long]("segments"),
      built.length.toLong,
      "write_snapshot counts other segments than build_index"
    )
    same(
      written.head.getAs[Long]("indexes"),
      built.length.toLong,
      "write_snapshot counts other indexes than build_index"
    )
    val key = written.head.getAs[String]("snapshot")
    check(exists(key), s"write_snapshot named $key but it does not exist")
    (s"https://s3.$region.amazonaws.com/$outputBucket/$key", storage)
  }

  private def recall(
      truth: Map[Long, Seq[Long]],
      got: Map[Long, Seq[Long]],
      k: Int
  ): Double = {
    same(
      got.keySet,
      truth.keySet,
      "the index search answered other queries than the exact one"
    )
    truth.map { case (query, ids) =>
      ids.toSet.intersect(got(query).toSet).size.toDouble / k
    }.sum / truth.size
  }

  private def writeCases: Seq[Case] = Seq(
    Case(
      "W-01",
      "write",
      "append write of all_types stages committed segments",
      () => {
        val source = ds("ALL_TYPES_SNAPSHOT_NODEL")
        val frame = snapshot(source)
        appendWrite(frame, source, "W-01")
        val staging = s"${outputPrefix("W-01")}/staging"
        val jobs = entries(staging)
        same(
          jobs.length,
          1,
          s"staging holds ${jobs.length} job directories: $jobs"
        )
        val job = s"$staging/${jobs.head}"
        Seq("manifest.json", "_committed", "owner.json").foreach { file =>
          check(exists(s"$job/$file"), s"$file is missing under $job")
        }
        val others = entries(job).filterNot(
          Set("manifest.json", "_committed", "owner.json", "_heartbeat")
        )
        check(others.nonEmpty, s"no segment data under $job: ${entries(job)}")
        // A second write of the same rows is a second job, and the first stays intact.
        appendWrite(frame.filter(col("id") < 10), source, "W-01")
        same(
          entries(staging).length,
          2,
          "the second write did not stage a job of its own"
        )
        check(
          exists(s"$job/_committed"),
          "the second write disturbed the first job"
        )
      }
    ),
    Case(
      "W-02",
      "write",
      "a write the collection cannot take is refused before any task",
      () => {
        val source = ds("ALL_TYPES_SNAPSHOT_NODEL")
        val frame = snapshot(source)
        // Each refusal has to name the offending part; any other failure (for
        // example the storage binding) is not the refusal the case is after.
        def rejected(label: String, names: String*)(body: => Unit): Unit = {
          val message = Option(failure(label)(body).getMessage).getOrElse("")
          check(
            names.exists(message.contains),
            s"$label was refused for another reason: $message"
          )
          check(
            entries(s"${outputPrefix("W-02")}/staging").isEmpty,
            s"$label left files in staging"
          )
        }
        rejected("a missing column", "fv")(
          appendWrite(frame.drop("fv"), source, "W-02")
        )
        rejected("a column of another type", "fv")(
          appendWrite(
            frame.withColumn("fv", col("fv").cast("string")),
            source,
            "W-02"
          )
        )
        rejected("a column that is no field", "extra")(
          appendWrite(frame.withColumn("extra", lit(1)), source, "W-02")
        )
        rejected("mode overwrite", "overwrite", "Overwrite")(
          frame.write
            .format("milvus")
            .mode("overwrite")
            .option(MilvusOption.SnapshotPath, source)
            .options(outputStorageOptions("W-02").asJava)
            .save()
        )
        rejected("a vector of the wrong dimension", "fv", "dimension")(
          appendWrite(
            frame.withColumn("fv", slice(col("fv"), 1, 4)),
            source,
            "W-02"
          )
        )
        rejected("an Int8 element out of range", "i8v", "Int8", "range")(
          appendWrite(
            frame.withColumn(
              "i8v",
              array((0 until 8).map(_ => lit(200.toShort)): _*)
            ),
            source,
            "W-02"
          )
        )
      }
    )
  )

  private def indexCases: Seq[Case] = Seq(
    Case(
      "I-01",
      "index",
      "FLAT answers like exact; HNSW recall against exact",
      () => {
        val flatSource = ds("ALL_TYPES_SNAPSHOT_NODEL")
        val flatQueries = queriesFrom(flatSource, "fv")
        val (flatSnapshot, flatStorage) = buildAndSnapshot(
          "I-01-flat",
          flatSource,
          "default.all_types",
          "fv",
          "FLAT",
          "L2",
          ""
        )
        val exactFlat = searchWith(
          storageOptions() + (MilvusOption.SnapshotPath -> flatSource),
          flatQueries,
          "fv",
          10,
          "L2",
          "exact"
        )
        val indexFlat = searchWith(
          flatStorage + (MilvusOption.SnapshotPath -> flatSnapshot),
          flatQueries,
          "fv",
          10,
          "L2",
          "index"
        )
        val flatRecall = recall(exactFlat, indexFlat, 10)
        check(
          flatRecall == 1.0,
          f"FLAT returns other ids than exact: recall $flatRecall%.3f"
        )
        val hnswSource = ds("SEARCH_CL_SNAPSHOT_NODEL")
        val hnswQueries = queriesFrom(hnswSource, "v")
        val (hnswSnapshot, hnswStorage) =
          buildAndSnapshot(
            "I-01-hnsw",
            hnswSource,
            "default.search_cl",
            "v",
            "HNSW",
            "L2",
            "M=16,efConstruction=200"
          )
        val exactHnsw = searchWith(
          storageOptions() + (MilvusOption.SnapshotPath -> hnswSource),
          hnswQueries,
          "v",
          10,
          "L2",
          "exact"
        )
        val indexHnsw = searchWith(
          hnswStorage + (MilvusOption.SnapshotPath -> hnswSnapshot),
          hnswQueries,
          "v",
          10,
          "L2",
          "index"
        )
        val hnswRecall = recall(exactHnsw, indexHnsw, 10)
        check(hnswRecall >= 0.95, f"HNSW recall@10 is $hnswRecall%.3f")
      }
    ),
    Case(
      "I-12",
      "index",
      "an index over a snapshot with deletes never returns a deleted row",
      () => {
        val source = ds("ALL_TYPES_SNAPSHOT_L0DEL")
        val queries = queriesFrom(source, "fv")
        val (built, storage) = buildAndSnapshot(
          "I-12",
          source,
          "default.all_types",
          "fv",
          "FLAT",
          "L2",
          ""
        )
        val exact = searchWith(
          storageOptions() + (MilvusOption.SnapshotPath -> source),
          queries,
          "fv",
          10,
          "L2",
          "exact"
        )
        val index = searchWith(
          storage + (MilvusOption.SnapshotPath -> built),
          queries,
          "fv",
          10,
          "L2",
          "index"
        )
        val deleted = index.values.flatten.filter(_ < 100L).toSeq
        same(deleted, Seq.empty[Long], "the index search returned deleted ids")
        val r = recall(exact, index, 10)
        check(
          r == 1.0,
          f"FLAT over the deleted snapshot returns other ids than exact: recall $r%.3f"
        )
      }
    )
  )

  private def allCases: Seq[Case] =
    readCases ++ searchCases ++ procedureCases ++ writeCases ++ indexCases

  def main(args: Array[String]): Unit = {
    val arguments = args
      .sliding(2, 2)
      .collect { case Array(k, v) => k.stripPrefix("--") -> v }
      .toMap
    session = SparkSession.builder().getOrCreate()
    val applicationId = spark.sparkContext.applicationId
    val selected = arguments.get("cases") match {
      case Some(list) =>
        val wanted = list.split(',').map(_.trim).filter(_.nonEmpty).toSet
        allCases.filter(c => wanted(c.id))
      case None =>
        arguments.getOrElse("group", "all") match {
          case "all" => allCases
          case group => allCases.filter(_.group == group)
        }
    }
    println(
      s"[CorrectnessJob] application $applicationId runs ${selected.size} case(s): ${selected.map(_.id).mkString(",")}"
    )

    val results = selected.map { one =>
      spark.sparkContext.setJobGroup(one.id, one.title)
      val started = System.nanoTime()
      val (status, message) =
        try {
          one.body()
          ("PASS", "")
        } catch {
          case skipped: Skipped => ("SKIP", skipped.getMessage)
          case NonFatal(error) =>
            val text = Option(error.getMessage)
              .getOrElse("")
              .replace('\n', ' ')
              .take(400)
            ("FAIL", s"${error.getClass.getSimpleName}: $text")
        }
      spark.sparkContext.clearJobGroup()
      val result = Result(
        one.id,
        one.group,
        one.title,
        status,
        message,
        (System.nanoTime() - started) / 1e9
      )
      println(
        f"[CorrectnessJob] CASE ${result.id} ${result.status} ${result.seconds}%.1fs ${result.message}"
      )
      result
    }

    val summary = results
      .groupBy(_.status)
      .map { case (status, rows) => s"$status=${rows.size}" }
      .toSeq
      .sorted
      .mkString(" ")
    println(s"[CorrectnessJob] SUMMARY application=$applicationId $summary")

    val resultsRoot =
      arguments.getOrElse("results", s"s3a://$outputBucket/$outputRoot")
    val target = new Path(s"$resultsRoot/$applicationId/results.json")
    val body = results
      .map { r =>
        def quote(value: String) =
          "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\""
        f"""  {"case": ${quote(r.id)}, "group": ${quote(
            r.group
          )}, "title": ${quote(r.title)}, "status": ${quote(
            r.status
          )}, "seconds": ${r.seconds}%.1f, "message": ${quote(r.message)}}"""
      }
      .mkString(
        s"""{"application_id": "$applicationId", "results": [\n""",
        ",\n",
        "\n]}\n"
      )
    val fs = target.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val stream = fs.create(target, true)
    try stream.write(body.getBytes(StandardCharsets.UTF_8))
    finally stream.close()
    println(s"[CorrectnessJob] results written to $target")

    spark.stop()
    // The driver JVM does not exit on its own after a native search (issue 22),
    // so the harness ends the process with its verdict.
    System.exit(if (results.exists(_.status == "FAIL")) 1 else 0)
  }
}
