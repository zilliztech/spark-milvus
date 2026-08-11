package com.zilliz.spark.connector.operations.backfill

import com.fasterxml.jackson.databind.node.{IntNode, LongNode}
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

import com.zilliz.spark.connector.read.{
  CollectionSchema,
  Field,
  V2ColumnGroup,
  V2SegmentInfo
}
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

  test("backfill physical source reads never apply deletes") {
    MilvusBackfill.ApplyDeletesToSourceRows shouldBe false
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

  private def snapshotField(
      name: String,
      id: Long,
      dataType: Int,
      primary: Boolean = false,
      nullable: Option[Boolean] = None,
      partition: Boolean = false,
      dynamic: Boolean = false,
      functionOutput: Boolean = false
  ): Field =
    Field(
      fieldID = Some(LongNode.valueOf(id)),
      name = name,
      rawDataType = Some(IntNode.valueOf(dataType)),
      isPrimaryKey = Some(primary),
      isDynamic = Some(dynamic),
      isPartitionKey = Some(partition),
      nullable = nullable,
      isFunctionOutput = Some(functionOutput)
    )

  test("resolveJoinKey resolves the default collection primary key") {
    val schema = CollectionSchema(
      name = "c",
      fields = Seq(
        snapshotField("id", 100L, 5, primary = true),
        snapshotField("external_row_id", 101L, 21)
      )
    )

    val resolved = MilvusBackfill
      .resolveJoinKey(schema, BackfillJoinKey.PrimaryKey)
      .toOption
      .get

    resolved.kind shouldBe "primary_key"
    resolved.sourceColumns shouldBe Seq("id")
    resolved.fieldIds shouldBe Seq(100L)
  }

  test("resolveJoinKey rejects a default PK join when the schema has no PK") {
    val schema = CollectionSchema(
      name = "c",
      fields = Seq(snapshotField("external_row_id", 101L, 21))
    )

    val error = MilvusBackfill
      .resolveJoinKey(schema, BackfillJoinKey.PrimaryKey)
      .left
      .toOption
      .get

    error.message should include("No primary key field")
  }

  test("resolveJoinKey accepts a physical field in a schema without a PK") {
    val schema = CollectionSchema(
      name = "c",
      fields = Seq(snapshotField("external_row_id", 101L, 21))
    )

    val resolved = MilvusBackfill
      .resolveJoinKey(
        schema,
        BackfillJoinKey.PhysicalField("external_row_id")
      )
      .toOption
      .get

    resolved.kind shouldBe "physical"
    resolved.sourceColumns shouldBe Seq("external_row_id")
    resolved.fieldIds shouldBe Seq(101L)
    resolved.components.head.sourceField.get.dataType shouldBe StringType
  }

  test("resolveJoinKey requires an exact physical field name") {
    val schema = CollectionSchema(
      name = "c",
      fields = Seq(snapshotField("External_Row_ID", 101L, 21))
    )

    val error = MilvusBackfill
      .resolveJoinKey(
        schema,
        BackfillJoinKey.PhysicalField("external_row_id")
      )
      .left
      .toOption
      .get

    error.message should include("was not found")
    error.message should include("External_Row_ID")
  }

  test("resolveJoinKey trims a programmatic physical field name") {
    val schema = CollectionSchema(
      name = "c",
      fields = Seq(snapshotField("external_row_id", 101L, 21))
    )

    val resolved = MilvusBackfill
      .resolveJoinKey(
        schema,
        BackfillJoinKey.PhysicalField("  external_row_id  ")
      )
      .toOption
      .get

    resolved.sourceColumns shouldBe Seq("external_row_id")
    resolved.fieldIds shouldBe Seq(101L)
  }

  test("resolveJoinKey rejects a nullable physical field") {
    val schema = CollectionSchema(
      name = "c",
      fields = Seq(
        snapshotField(
          "external_row_id",
          101L,
          21,
          nullable = Some(true)
        )
      )
    )

    val error = MilvusBackfill
      .resolveJoinKey(
        schema,
        BackfillJoinKey.PhysicalField("external_row_id")
      )
      .left
      .toOption
      .get

    error.message should include("nullable")
    error.message should include("do not match NULL")
  }

  test("resolveJoinKey supports stable scalar physical key types") {
    val cases = Seq(
      2 -> org.apache.spark.sql.types.ByteType,
      3 -> org.apache.spark.sql.types.ShortType,
      4 -> IntegerType,
      5 -> LongType,
      20 -> StringType,
      21 -> StringType
    )

    cases.zipWithIndex.foreach { case ((milvusType, sparkType), index) =>
      val name = s"key_$index"
      val schema = CollectionSchema(
        name = "c",
        fields = Seq(snapshotField(name, 100L + index, milvusType))
      )
      val resolved = MilvusBackfill
        .resolveJoinKey(schema, BackfillJoinKey.PhysicalField(name))
        .toOption
        .get

      resolved.components.head.sourceField.get.dataType shouldBe sparkType
    }
  }

  test("resolveJoinKey rejects unsafe physical key types") {
    Seq(
      1 -> "boolean",
      10 -> "float",
      11 -> "double",
      22 -> "array",
      23 -> "json",
      24 -> "geometry",
      25 -> "text",
      26 -> "timestamptz",
      27 -> "unknown",
      100 -> "vector"
    ).foreach { case (milvusType, label) =>
      val name = s"${label}_key"
      val schema = CollectionSchema(
        name = "c",
        fields = Seq(snapshotField(name, 101L, milvusType))
      )
      val error = MilvusBackfill
        .resolveJoinKey(schema, BackfillJoinKey.PhysicalField(name))
        .left
        .toOption
        .get

      error.message should include("unsupported type")
      error.message should include(name)
    }
  }

  test("resolveBackfillTargetFields accepts ordinary collection fields") {
    val schema = CollectionSchema(
      name = "c",
      fields = Seq(snapshotField("value", 101L, 21))
    )

    val resolved = MilvusBackfill
      .resolveBackfillTargetFields(schema, Seq("value"))
      .toOption
      .get

    resolved.keySet shouldBe Set("value")
    resolved("value").getFieldIDAsLong shouldBe 101L
  }

  test("resolveBackfillTargetFields rejects protected collection fields") {
    val cases = Seq(
      snapshotField("id", 100L, 5, primary = true) -> "primary key",
      snapshotField("tenant", 101L, 21, partition = true) -> "partition key",
      snapshotField("$meta", 102L, 23, dynamic = true) -> "dynamic field",
      snapshotField(
        "generated_text",
        103L,
        21,
        functionOutput = true
      ) -> "function output",
      snapshotField("RowID", 0L, 5) -> "system field",
      snapshotField("Timestamp", 1L, 5) -> "system field"
    )

    cases.foreach { case (field, expectedRole) =>
      val schema = CollectionSchema(name = "c", fields = Seq(field))
      val error = MilvusBackfill
        .resolveBackfillTargetFields(schema, Seq(field.name))
        .left
        .toOption
        .get

      error.message should include(field.name)
      error.message should include(expectedRole)
    }
  }

  test(
    "resolveBackfillTargetFields rejects a PK target selected beside a physical join key"
  ) {
    val schema = CollectionSchema(
      name = "c",
      fields = Seq(
        snapshotField("id", 100L, 5, primary = true),
        snapshotField("external_row_id", 101L, 21),
        snapshotField("value", 102L, 21)
      )
    )

    val error = MilvusBackfill
      .resolveBackfillTargetFields(schema, Seq("id", "value"))
      .left
      .toOption
      .get

    error.message should include("'id' (primary key)")
  }

  test("resolveBackfillTargetFields reports missing snapshot fields") {
    val schema = CollectionSchema(
      name = "c",
      fields = Seq(snapshotField("value", 101L, 21))
    )

    val error = MilvusBackfill
      .resolveBackfillTargetFields(schema, Seq("missing"))
      .left
      .toOption
      .get

    error.message should include("Fields not found in snapshot schema")
    error.message should include("missing")
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
        StructField(MilvusBackfill.SegmentIdCol, LongType, nullable = false),
        StructField(MilvusBackfill.RowOffsetCol, LongType, nullable = false)
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

  test("join-key cardinality accepts unique non-null keys") {
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1), Row(2))),
      StructType(Seq(StructField("join_key", IntegerType, nullable = false)))
    )

    MilvusBackfill.validateJoinKeyCardinality(
      df,
      Seq("join_key"),
      Seq("pk"),
      "Backfill parquet"
    ) shouldBe Right(JoinKeyStats(2L, 0L, 2L))
  }

  test("join-key cardinality rejects null keys separately from duplicates") {
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(Int.box(1)), Row(null))),
      StructType(Seq(StructField("join_key", IntegerType, nullable = true)))
    )

    val err = MilvusBackfill
      .validateJoinKeyCardinality(
        df,
        Seq("join_key"),
        Seq("pk"),
        "Backfill parquet"
      )
      .left
      .toOption
      .get

    err.message should include("null join key")
    err.message should include("(pk)")
  }

  test("join-key cardinality rejects duplicate keys") {
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1), Row(1))),
      StructType(Seq(StructField("join_key", IntegerType, nullable = false)))
    )

    val err = MilvusBackfill
      .validateJoinKeyCardinality(
        df,
        Seq("join_key"),
        Seq("pk"),
        "Backfill parquet"
      )
      .left
      .toOption
      .get

    err.message should include("duplicate join-key values")
    err.message should include("distinct=1")
  }

  test("join-key cardinality validates composite tuples") {
    val unique = spark.createDataFrame(
      spark.sparkContext.parallelize(
        Seq(Row(1, "a"), Row(1, "b"), Row(2, "a"))
      ),
      StructType(
        Seq(
          StructField("k1", IntegerType, nullable = false),
          StructField("k2", StringType, nullable = false)
        )
      )
    )
    val duplicate = unique.union(unique.limit(1))

    MilvusBackfill.validateJoinKeyCardinality(
      unique,
      Seq("k1", "k2"),
      Seq("file", "row"),
      "Backfill parquet"
    ) shouldBe Right(JoinKeyStats(3L, 0L, 3L))

    MilvusBackfill
      .validateJoinKeyCardinality(
        duplicate,
        Seq("k1", "k2"),
        Seq("file", "row"),
        "Backfill parquet"
      )
      .isLeft shouldBe true
  }

  test("source read projection keeps field IDs aligned with schema order") {
    val pkField = StructField("pk", IntegerType, nullable = false)
    val joinKey = ResolvedJoinKey.primaryKey("pk", 100L, Some(pkField))
    val projection = MilvusBackfill
      .buildSourceReadProjection(
        joinKey,
        Seq(
          ("pk", 100L, pkField),
          ("f1", 101L, StructField("f1", StringType, nullable = true))
        )
      )
      .toOption
      .get

    projection.fieldIds shouldBe Seq(100L, 101L)
    projection.schema.fieldNames.toSeq shouldBe Seq("pk", "f1")
  }

  test("source read projection starts with the selected physical field") {
    val physicalField = StructField(
      "external_row_id",
      StringType,
      nullable = false
    )
    val joinKey = ResolvedJoinKey.physicalField(
      "external_row_id",
      101L,
      physicalField
    )
    val projection = MilvusBackfill
      .buildSourceReadProjection(
        joinKey,
        Seq(
          ("id", 100L, StructField("id", LongType, nullable = false)),
          ("value", 102L, StructField("value", StringType, nullable = true))
        )
      )
      .toOption
      .get

    projection.fieldIds shouldBe Seq(101L, 100L, 102L)
    projection.schema.fieldNames.toSeq shouldBe Seq(
      "external_row_id",
      "id",
      "value"
    )
  }

  test("source join normalization preserves rename swaps in one projection") {
    val joinKey = ResolvedJoinKey(
      kind = "test_swap",
      components = Seq(
        ResolvedJoinComponent("a", 100L, None, "b"),
        ResolvedJoinComponent("b", 101L, None, "a")
      )
    )
    val source = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row("A", "B", 10L))),
      StructType(
        Seq(
          StructField("a", StringType, nullable = false),
          StructField("b", StringType, nullable = false),
          StructField(MilvusBackfill.SegmentIdCol, LongType, nullable = false)
        )
      )
    )

    val normalized = MilvusBackfill.normalizeSourceJoinColumns(source, joinKey)
    val row = normalized.collect().head

    row.getAs[String]("b") shouldBe "A"
    row.getAs[String]("a") shouldBe "B"
    row.getAs[Long](MilvusBackfill.SegmentIdCol) shouldBe 10L
  }

  test("join-key compatibility checks normalized component types") {
    val internal = ResolvedJoinKey.internalColumn(0)
    val joinKey = ResolvedJoinKey.primaryKey("pk", 100L, None)
    val source = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1))),
      StructType(Seq(StructField(internal, IntegerType, nullable = false)))
    )
    val matching = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1))),
      StructType(Seq(StructField(internal, IntegerType, nullable = false)))
    )
    val mismatched = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1L))),
      StructType(Seq(StructField(internal, LongType, nullable = false)))
    )

    MilvusBackfill.validateJoinKeyCompatibility(
      source,
      matching,
      joinKey
    ) shouldBe Right(())

    val err = MilvusBackfill
      .validateJoinKeyCompatibility(source, mismatched, joinKey)
      .left
      .toOption
      .get
    err.message should include("Join-key type mismatch")
    err.message should include("pk")
  }

  test("performJoin supports multiple resolved join components") {
    val original = spark.createDataFrame(
      spark.sparkContext.parallelize(
        Seq(Row(1, "a", 10L, 0L), Row(1, "b", 10L, 1L))
      ),
      StructType(
        Seq(
          StructField("k1", IntegerType, nullable = false),
          StructField("k2", StringType, nullable = false),
          StructField(MilvusBackfill.SegmentIdCol, LongType, nullable = false),
          StructField(MilvusBackfill.RowOffsetCol, LongType, nullable = false)
        )
      )
    )
    val backfill = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1, "a", "matched"))),
      StructType(
        Seq(
          StructField("k1", IntegerType, nullable = false),
          StructField("k2", StringType, nullable = false),
          StructField("value", StringType, nullable = true)
        )
      )
    )

    val joined = MilvusBackfill.performJoin(
      original,
      backfill,
      Seq("k1", "k2"),
      Seq("value"),
      MilvusOption.BackfillModeReplace
    )

    joined
      .orderBy("k2")
      .collect()
      .map(r => (r.getAs[String]("k2"), Option(r.getAs[String]("value"))))
      .toSeq shouldBe Seq(("a", Some("matched")), ("b", None))
  }

  test("performJoin allows repeated source rows on a non-PK physical key") {
    val physicalKey = ResolvedJoinKey.internalColumn(0)
    val original = spark.createDataFrame(
      spark.sparkContext.parallelize(
        Seq(
          Row(1L, "row-b", 10L, 0L),
          Row(2L, "row-a", 10L, 1L),
          Row(3L, "row-a", 10L, 2L)
        )
      ),
      StructType(
        Seq(
          StructField("id", LongType, nullable = false),
          StructField(physicalKey, StringType, nullable = false),
          StructField(MilvusBackfill.SegmentIdCol, LongType, nullable = false),
          StructField(MilvusBackfill.RowOffsetCol, LongType, nullable = false)
        )
      )
    )
    val backfill = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row("row-a", "matched"))),
      StructType(
        Seq(
          StructField(physicalKey, StringType, nullable = false),
          StructField("value", StringType, nullable = true)
        )
      )
    )

    val joined = MilvusBackfill.performJoin(
      original,
      backfill,
      Seq(physicalKey),
      Seq("value"),
      MilvusOption.BackfillModeReplace
    )

    joined.count() shouldBe original.count()
    joined
      .orderBy("id")
      .collect()
      .map(row => (row.getAs[Long]("id"), Option(row.getAs[String]("value"))))
      .toSeq shouldBe Seq(
      (1L, None),
      (2L, Some("matched")),
      (3L, Some("matched"))
    )
  }

  test("replace mode: source has only PK, backfill values win") {
    val originalSchema = StructType(
      Seq(
        StructField("pk", IntegerType, nullable = false),
        StructField(MilvusBackfill.SegmentIdCol, LongType, nullable = false),
        StructField(MilvusBackfill.RowOffsetCol, LongType, nullable = false)
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
      Seq("pk"),
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
      Seq("pk"),
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
      MilvusBackfill.SegmentIdCol,
      MilvusBackfill.RowOffsetCol,
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
      Seq("pk"),
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
      Seq("pk"),
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
      Seq("pk"),
      Seq("f1", "f2"),
      MilvusOption.BackfillModeOverwrite
    )

    // Output layout mirrors coalesce: source-side columns + match flag +
    // per-field provenance flags.
    joined.columns.toSet shouldBe Set(
      "pk",
      "f1",
      "f2",
      MilvusBackfill.SegmentIdCol,
      MilvusBackfill.RowOffsetCol,
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
      Seq("pk"),
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
