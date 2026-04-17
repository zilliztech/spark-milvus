package com.zilliz.spark.connector.operations.backfill

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{
  IntegerType,
  LongType,
  StringType,
  StructField,
  StructType
}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

import com.zilliz.spark.connector.MilvusOption

/** Tests for the new `--mode` backfill parameter: CLI/config validation and the
  * `performJoin` merge semantics (overwrite vs coalesce).
  */
class BackfillModeTest
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("BackfillModeTest")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  // ============ Config / CLI parsing ============

  test("BackfillConfig defaults to overwrite mode") {
    val config = BackfillConfig(
      s3Endpoint = "localhost:9000",
      s3BucketName = "a-bucket",
      s3AccessKey = "ak",
      s3SecretKey = "sk"
    )
    config.mode shouldBe MilvusOption.BackfillModeOverwrite
    config.validate() shouldBe Right(())
  }

  test("BackfillConfig accepts coalesce mode") {
    val config = BackfillConfig(
      s3Endpoint = "localhost:9000",
      s3BucketName = "a-bucket",
      s3AccessKey = "ak",
      s3SecretKey = "sk",
      mode = MilvusOption.BackfillModeCoalesce
    )
    config.validate() shouldBe Right(())
  }

  test("BackfillConfig rejects unknown mode") {
    val config = BackfillConfig(
      s3Endpoint = "localhost:9000",
      s3BucketName = "a-bucket",
      s3AccessKey = "ak",
      s3SecretKey = "sk",
      mode = "bogus"
    )
    val err = config.validate().left.toOption.get
    err should include("mode must be")
    err should include("bogus")
  }

  test("parseArgs accepts --mode coalesce") {
    val parsed = BackfillApp.parseArgs(
      Array(
        "--parquet",
        "/x",
        "--snapshot",
        "/y",
        "--s3-endpoint",
        "m:9000",
        "--s3-bucket",
        "b",
        "--s3-access-key",
        "ak",
        "--s3-secret-key",
        "sk",
        "--mode",
        "coalesce"
      )
    )
    parsed("mode") shouldBe "coalesce"
  }

  test("parseArgs rejects typo'd --modes") {
    val ex = intercept[IllegalArgumentException] {
      BackfillApp.parseArgs(Array("--modes", "coalesce"))
    }
    ex.getMessage should include("Unknown argument")
  }

  // ============ performJoin semantics ============

  private def buildOriginal(
      rows: Seq[(Int, java.lang.Integer, java.lang.String, Long, Long)]
  ): DataFrame = {
    // columns: pk, f1 (nullable Int), f2 (nullable String), segment_id, row_offset
    val schema = StructType(
      Seq(
        StructField("pk", IntegerType, nullable = false),
        StructField("f1", IntegerType, nullable = true),
        StructField("f2", StringType, nullable = true),
        StructField("segment_id", LongType, nullable = false),
        StructField("row_offset", LongType, nullable = false)
      )
    )
    val javaRows = rows.map { case (pk, f1, f2, seg, off) =>
      Row(pk, f1, f2, seg, off)
    }
    spark.createDataFrame(spark.sparkContext.parallelize(javaRows), schema)
  }

  private def buildBackfill(
      rows: Seq[(Int, java.lang.Integer, java.lang.String)]
  ): DataFrame = {
    val schema = StructType(
      Seq(
        StructField("pk", IntegerType, nullable = false),
        StructField("f1", IntegerType, nullable = true),
        StructField("f2", StringType, nullable = true)
      )
    )
    val javaRows = rows.map { case (pk, f1, f2) => Row(pk, f1, f2) }
    spark.createDataFrame(spark.sparkContext.parallelize(javaRows), schema)
  }

  test("overwrite mode: source has only PK, backfill values win") {
    val originalSchema = StructType(
      Seq(
        StructField("pk", IntegerType, nullable = false),
        StructField("segment_id", LongType, nullable = false),
        StructField("row_offset", LongType, nullable = false)
      )
    )
    val originalRows = Seq(Row(1, 10L, 0L), Row(2, 10L, 1L), Row(3, 10L, 2L))
    val original = spark.createDataFrame(
      spark.sparkContext.parallelize(originalRows),
      originalSchema
    )
    val backfill = buildBackfill(
      Seq(
        (1, Int.box(100), "A"),
        (2, null, "B")
        // pk=3 missing from backfill → left join produces nulls
      )
    )

    val joined = MilvusBackfill.performJoin(
      original,
      backfill,
      "pk",
      Seq("f1", "f2"),
      MilvusOption.BackfillModeOverwrite
    )

    val byPk = joined
      .orderBy("pk")
      .collect()
      .map(r =>
        (
          r.getAs[Int]("pk"),
          Option(r.get(r.fieldIndex("f1"))).map(_.asInstanceOf[Int]),
          Option(r.getAs[String]("f2"))
        )
      )
      .toSeq

    byPk shouldBe Seq(
      (1, Some(100), Some("A")),
      (2, None, Some("B")),
      (3, None, None)
    )
  }

  test(
    "coalesce mode: per-field independent, source-null filled from backfill"
  ) {
    // pk=1 — both source fields non-null; keep source values, ignore backfill.
    // pk=2 — f1 null in source, f2 non-null; fill f1 from backfill, keep f2.
    // pk=3 — both source fields null; fill both from backfill.
    // pk=4 — no backfill row; keep whatever source has.
    val original = buildOriginal(
      Seq(
        (1, Int.box(1), "src1", 10L, 0L),
        (2, null, "src2", 10L, 1L),
        (3, null, null, 10L, 2L),
        (4, Int.box(4), null, 10L, 3L)
      )
    )
    val backfill = buildBackfill(
      Seq(
        (1, Int.box(100), "BF1"),
        (2, Int.box(200), "BF2"),
        (3, Int.box(300), "BF3")
      )
    )

    val joined = MilvusBackfill.performJoin(
      original,
      backfill,
      "pk",
      Seq("f1", "f2"),
      MilvusOption.BackfillModeCoalesce
    )

    // The join should have the same columns as the source side plus the
    // backfill-match marker (the backfill-side target columns are dropped
    // after coalesce). The marker is consumed by processSegments for stats.
    joined.columns.toSet shouldBe Set(
      "pk",
      "f1",
      "f2",
      "segment_id",
      "row_offset",
      MilvusBackfill.MatchFlagCol
    )

    val byPk = joined
      .orderBy("pk")
      .collect()
      .map(r =>
        (
          r.getAs[Int]("pk"),
          Option(r.get(r.fieldIndex("f1"))).map(_.asInstanceOf[Int]),
          Option(r.getAs[String]("f2"))
        )
      )
      .toSeq

    byPk shouldBe Seq(
      (1, Some(1), Some("src1")), // source wins on both
      (2, Some(200), Some("src2")), // f1 filled, f2 kept
      (3, Some(300), Some("BF3")), // both filled
      (4, Some(4), None) // no backfill row, f2 stays null
    )
  }

  // ============ validateCoalesceTypes ============

  test("validateCoalesceTypes: matching types pass") {
    val backfillSchema = StructType(
      Seq(
        StructField("pk", IntegerType, nullable = false),
        StructField("f1", IntegerType, nullable = true),
        StructField("f2", StringType, nullable = true)
      )
    )
    val extras = Seq(
      ("f1", 101L, StructField("f1", IntegerType, nullable = true)),
      ("f2", 102L, StructField("f2", StringType, nullable = true))
    )
    MilvusBackfill.validateCoalesceTypes(backfillSchema, extras) shouldBe Right(
      ()
    )
  }

  test("validateCoalesceTypes: mismatched type rejected with clear message") {
    // parquet sees IntegerType but snapshot says LongType — Spark's coalesce
    // would silently widen and produce a Long binlog, breaking Milvus reads.
    val backfillSchema = StructType(
      Seq(
        StructField("pk", IntegerType, nullable = false),
        StructField("f1", IntegerType, nullable = true)
      )
    )
    val extras = Seq(
      ("f1", 101L, StructField("f1", LongType, nullable = true))
    )
    val err = MilvusBackfill
      .validateCoalesceTypes(backfillSchema, extras)
      .left
      .toOption
      .get
    err shouldBe a[SchemaValidationError]
    err.message should include("to match snapshot field types")
    err.message should include("f1")
    err.message should include("snapshot=bigint")
    err.message should include("parquet=int")
  }

  test(
    "validateCoalesceTypes: backfill missing the field is not flagged here"
  ) {
    // performJoin/processSegments handles missing columns via the left join.
    // The type validator only complains when both sides have the column AND
    // the types disagree.
    val backfillSchema = StructType(
      Seq(StructField("pk", IntegerType, nullable = false))
    )
    val extras = Seq(
      ("f1", 101L, StructField("f1", LongType, nullable = true))
    )
    MilvusBackfill.validateCoalesceTypes(backfillSchema, extras) shouldBe Right(
      ()
    )
  }

  test("coalesce mode: empty source columns degrade to full overwrite") {
    // Simulates a just-added field with no prior data: source returns null
    // everywhere, so coalesce naturally falls back to the backfill value.
    val original = buildOriginal(
      Seq(
        (1, null, null, 10L, 0L),
        (2, null, null, 10L, 1L)
      )
    )
    val backfill = buildBackfill(
      Seq(
        (1, Int.box(10), "A"),
        (2, Int.box(20), "B")
      )
    )

    val joined = MilvusBackfill.performJoin(
      original,
      backfill,
      "pk",
      Seq("f1", "f2"),
      MilvusOption.BackfillModeCoalesce
    )

    val byPk = joined
      .orderBy("pk")
      .collect()
      .map(r => (r.getAs[Int]("pk"), r.getAs[Int]("f1"), r.getAs[String]("f2")))
      .toSeq

    byPk shouldBe Seq((1, 10, "A"), (2, 20, "B"))
  }
}
