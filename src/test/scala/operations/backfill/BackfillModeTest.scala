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

import com.zilliz.spark.connector.read.{V2ColumnGroup, V2SegmentInfo}
import com.zilliz.spark.connector.MilvusOption

/** Tests for the `--mode` backfill parameter: CLI/config validation and the
  * `performJoin` merge semantics (replace vs coalesce vs overwrite).
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

  test("BackfillConfig defaults to coalesce mode") {
    val config = BackfillConfig(
      s3Endpoint = "localhost:9000",
      s3BucketName = "a-bucket",
      s3AccessKey = "ak",
      s3SecretKey = "sk"
    )
    config.mode shouldBe MilvusOption.BackfillModeCoalesce
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
    // columns: pk, f1 (nullable Int), f2 (nullable String), $segment_id, $row_offset
    val schema = StructType(
      Seq(
        StructField("pk", IntegerType, nullable = false),
        StructField("f1", IntegerType, nullable = true),
        StructField("f2", StringType, nullable = true),
        StructField("$segment_id", LongType, nullable = false),
        StructField("$row_offset", LongType, nullable = false)
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

  test("replace mode: source has only PK, backfill values win") {
    val originalSchema = StructType(
      Seq(
        StructField("pk", IntegerType, nullable = false),
        StructField("$segment_id", LongType, nullable = false),
        StructField("$row_offset", LongType, nullable = false)
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
      MilvusOption.BackfillModeReplace
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
      (3, None, None) // unmatched source row: target columns become null
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
    // backfill-match marker and, in coalesce mode, per-field provenance
    // flags used downstream for the usedSource/usedDataFile counters.
    joined.columns.toSet shouldBe Set(
      "pk",
      "f1",
      "f2",
      "$segment_id",
      "$row_offset",
      MilvusBackfill.MatchFlagCol,
      MilvusBackfill.usedSrcCol("f1"),
      MilvusBackfill.usedBfCol("f1"),
      MilvusBackfill.usedSrcCol("f2"),
      MilvusBackfill.usedBfCol("f2")
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

  // ============ validateMergeableFieldTypes ============

  test("validateMergeableFieldTypes: matching types pass") {
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
    MilvusBackfill.validateMergeableFieldTypes(
      backfillSchema,
      extras,
      MilvusOption.BackfillModeCoalesce
    ) shouldBe Right(())
  }

  test(
    "validateMergeableFieldTypes: mismatched type rejected with clear message"
  ) {
    // parquet sees IntegerType but snapshot says LongType — Spark's coalesce
    // / when-otherwise would silently widen and produce a Long binlog,
    // breaking Milvus reads.
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
      .validateMergeableFieldTypes(
        backfillSchema,
        extras,
        MilvusOption.BackfillModeOverwrite
      )
      .left
      .toOption
      .get
    err shouldBe a[SchemaValidationError]
    err.message should include("to match snapshot field types")
    err.message should include("f1")
    err.message should include("snapshot=bigint")
    err.message should include("parquet=int")
    // Error surfaces the active mode so users know which flag to fix.
    err.message should include(MilvusOption.BackfillModeOverwrite)
  }

  test(
    "validateMergeableFieldTypes: backfill missing the field is not flagged here"
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
    MilvusBackfill.validateMergeableFieldTypes(
      backfillSchema,
      extras,
      MilvusOption.BackfillModeCoalesce
    ) shouldBe Right(())
  }

  test("coalesce mode: per-field provenance flags mark source vs datafile") {
    // pk=1 — f1 non-null src → usedSrc(f1); f2 null src + bf non-null → usedBf(f2)
    // pk=2 — f1 null src + bf non-null → usedBf(f1); f2 non-null src → usedSrc(f2)
    // pk=3 — no backfill row; f1 non-null src → usedSrc(f1); f2 null src + no
    //        bf row → neither flag (output is null).
    val original = buildOriginal(
      Seq(
        (1, Int.box(1), null, 10L, 0L),
        (2, null, "src2", 10L, 1L),
        (3, Int.box(3), null, 10L, 2L)
      )
    )
    val backfill = buildBackfill(
      Seq(
        (1, Int.box(100), "BF1"),
        (2, Int.box(200), "BF2")
      )
    )

    val joined = MilvusBackfill.performJoin(
      original,
      backfill,
      "pk",
      Seq("f1", "f2"),
      MilvusOption.BackfillModeCoalesce
    )

    val flags = joined
      .orderBy("pk")
      .collect()
      .map(r =>
        (
          r.getAs[Int]("pk"),
          r.getAs[Boolean](MilvusBackfill.usedSrcCol("f1")),
          r.getAs[Boolean](MilvusBackfill.usedBfCol("f1")),
          r.getAs[Boolean](MilvusBackfill.usedSrcCol("f2")),
          r.getAs[Boolean](MilvusBackfill.usedBfCol("f2"))
        )
      )
      .toSeq

    flags shouldBe Seq(
      (1, true, false, false, true),
      (2, false, true, true, false),
      (3, true, false, false, false)
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

  // ============ overwrite mode (matched rows take file, unmatched keep src) ============

  test(
    "overwrite mode: matched rows take backfill, unmatched rows keep source"
  ) {
    // pk=1 — matched, backfill has non-null values → take backfill for both
    //        fields (differs from coalesce: even where src is non-null, file
    //        wins when matched).
    // pk=2 — matched, backfill has null f1 → write null (differs from
    //        coalesce: coalesce would keep src; overwrite overrides).
    // pk=3 — no backfill row → keep source values (differs from replace,
    //        which would null them out).
    val original = buildOriginal(
      Seq(
        (1, Int.box(1), "src1", 10L, 0L),
        (2, Int.box(2), "src2", 10L, 1L),
        (3, Int.box(3), "src3", 10L, 2L)
      )
    )
    val backfill = buildBackfill(
      Seq(
        (1, Int.box(100), "BF1"),
        (2, null, "BF2")
        // pk=3 missing
      )
    )

    val joined = MilvusBackfill.performJoin(
      original,
      backfill,
      "pk",
      Seq("f1", "f2"),
      MilvusOption.BackfillModeOverwrite
    )

    // Output layout mirrors coalesce: source-side columns + match flag +
    // per-field provenance flags.
    joined.columns.toSet shouldBe Set(
      "pk",
      "f1",
      "f2",
      "$segment_id",
      "$row_offset",
      MilvusBackfill.MatchFlagCol,
      MilvusBackfill.usedSrcCol("f1"),
      MilvusBackfill.usedBfCol("f1"),
      MilvusBackfill.usedSrcCol("f2"),
      MilvusBackfill.usedBfCol("f2")
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
      (1, Some(100), Some("BF1")), // matched: backfill wins on both
      (2, None, Some("BF2")), // matched: backfill's null clobbers src
      (3, Some(3), Some("src3")) // unmatched: source preserved
    )
  }

  test(
    "overwrite mode: per-field provenance — matched → usedBf, unmatched → usedSrc"
  ) {
    val original = buildOriginal(
      Seq(
        (1, Int.box(1), "src1", 10L, 0L),
        (2, Int.box(2), "src2", 10L, 1L)
      )
    )
    val backfill = buildBackfill(
      Seq(
        (1, Int.box(100), "BF1")
        // pk=2 missing — unmatched
      )
    )

    val joined = MilvusBackfill.performJoin(
      original,
      backfill,
      "pk",
      Seq("f1", "f2"),
      MilvusOption.BackfillModeOverwrite
    )

    val flags = joined
      .orderBy("pk")
      .collect()
      .map(r =>
        (
          r.getAs[Int]("pk"),
          r.getAs[Boolean](MilvusBackfill.usedSrcCol("f1")),
          r.getAs[Boolean](MilvusBackfill.usedBfCol("f1")),
          r.getAs[Boolean](MilvusBackfill.usedSrcCol("f2")),
          r.getAs[Boolean](MilvusBackfill.usedBfCol("f2"))
        )
      )
      .toSeq

    // Overwrite's per-field flags track the row-level match — same value
    // across every field for a given row (unlike coalesce, where each field
    // decides independently).
    flags shouldBe Seq(
      (1, false, true, false, true), // matched: backfill wins for both fields
      (2, true, false, true, false) // unmatched: source kept for both fields
    )
  }

  // ============ dedupColumnGroupsBySlot ============

  private def seg(groups: V2ColumnGroup*): V2SegmentInfo =
    V2SegmentInfo(
      segmentId = 1L,
      partitionId = 2L,
      numOfRows = 0L,
      storageVersion = 2L,
      columnGroups = groups.toSeq
    )

  test(
    "dedupColumnGroupsBySlot: single-field group at slot=fieldID strips that " +
      "fieldID from older multi-field group at slot < 100"
  ) {
    // Mirrors the production scenario: an original multi-field packed parquet
    // (slot 1) declares fields 102..114, but a prior addfield+backfill wrote
    // single-field groups at slot 113 / 114. Without dedup the reader sees
    // 113/114 in BOTH groups and the C++ packed reader picks an undefined source,
    // typically the older slot-1 file (mostly null). After dedup, slot 1's
    // fieldIds drops 113/114 so each fieldID resolves to one group.
    val input = seg(
      V2ColumnGroup(
        fieldIds = Seq(102L, 103L, 113L, 114L),
        filePaths = Seq("seg/1/old"),
        fileRowCounts = Seq(100L),
        slotFieldId = 1L
      ),
      V2ColumnGroup(
        fieldIds = Seq(113L),
        filePaths = Seq("seg/113/new"),
        fileRowCounts = Seq(100L),
        slotFieldId = 113L
      ),
      V2ColumnGroup(
        fieldIds = Seq(114L),
        filePaths = Seq("seg/114/new"),
        fileRowCounts = Seq(100L),
        slotFieldId = 114L
      )
    )

    val out = MilvusBackfill.dedupColumnGroupsBySlot(input)
    out.columnGroups.map(g => (g.slotFieldId, g.fieldIds)) shouldBe Seq(
      (1L, Seq(102L, 103L)),
      (113L, Seq(113L)),
      (114L, Seq(114L))
    )
  }

  test(
    "dedupColumnGroupsBySlot: drops a group entirely if every fieldID it carries " +
      "is owned by a larger slot"
  ) {
    val input = seg(
      V2ColumnGroup(
        fieldIds = Seq(113L),
        filePaths = Seq("p/old"),
        fileRowCounts = Seq(1L),
        slotFieldId = 1L
      ),
      V2ColumnGroup(
        fieldIds = Seq(113L),
        filePaths = Seq("p/new"),
        fileRowCounts = Seq(1L),
        slotFieldId = 113L
      )
    )
    val out = MilvusBackfill.dedupColumnGroupsBySlot(input)
    out.columnGroups should have size 1
    out.columnGroups.head.slotFieldId shouldBe 113L
    out.columnGroups.head.fieldIds shouldBe Seq(113L)
  }

  test(
    "dedupColumnGroupsBySlot: leaves single-group / no-conflict layouts untouched"
  ) {
    val input = seg(
      V2ColumnGroup(
        fieldIds = Seq(100L, 0L, 1L),
        filePaths = Seq("p/0"),
        fileRowCounts = Seq(1L),
        slotFieldId = 0L // Real slot 0 = RowID; legal AVRO value.
      )
    )
    val out = MilvusBackfill.dedupColumnGroupsBySlot(input)
    out shouldBe input
  }

  test(
    "dedupColumnGroupsBySlot: slot 0 (RowID) is a real slot id, not the unknown " +
      "sentinel — must NOT short-circuit dedup when other groups conflict"
  ) {
    // Regression: an earlier version used 0L as the unknown-slot sentinel,
    // but RowID's AVRO entry legitimately has field_id=0. That made dedup
    // skip every AVRO-loaded segment, defeating the fix entirely. The
    // sentinel is now -1L; slot 0L participates in dedup like any other slot.
    val input = seg(
      V2ColumnGroup(
        fieldIds = Seq(0L), // RowID
        filePaths = Seq("p/rowid"),
        fileRowCounts = Seq(1L),
        slotFieldId = 0L
      ),
      V2ColumnGroup(
        fieldIds = Seq(102L, 113L),
        filePaths = Seq("p/multi"),
        fileRowCounts = Seq(1L),
        slotFieldId = 1L
      ),
      V2ColumnGroup(
        fieldIds = Seq(113L),
        filePaths = Seq("p/single"),
        fileRowCounts = Seq(1L),
        slotFieldId = 113L
      )
    )
    val out = MilvusBackfill.dedupColumnGroupsBySlot(input)
    out.columnGroups.map(g => (g.slotFieldId, g.fieldIds)) shouldBe Seq(
      (0L, Seq(0L)),
      (1L, Seq(102L)),
      (113L, Seq(113L))
    )
  }

  test(
    "dedupColumnGroupsBySlot: skips dedup when ANY group's slotFieldId is the " +
      "-1L sentinel (snapshot-JSON DTO path) — never silently drops a fieldID"
  ) {
    // Snapshot JSON deserialization currently doesn't surface AVRO slot ids,
    // so V2ColumnGroup.slotFieldId stays at its default -1L. In that mode we
    // must NOT dedup, otherwise we'd treat all groups as if they shared the
    // same unknown slot and arbitrarily strip fields.
    val input = seg(
      V2ColumnGroup(
        fieldIds = Seq(102L, 113L),
        filePaths = Seq("p/multi"),
        fileRowCounts = Seq(1L)
        // slotFieldId defaults to -1L
      ),
      V2ColumnGroup(
        fieldIds = Seq(113L),
        filePaths = Seq("p/single"),
        fileRowCounts = Seq(1L)
        // slotFieldId defaults to -1L
      )
    )
    val out = MilvusBackfill.dedupColumnGroupsBySlot(input)
    out shouldBe input
  }
}
