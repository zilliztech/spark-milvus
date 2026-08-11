package com.zilliz.spark.connector.operations.backfill

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{
  IntegerType,
  StringType,
  StructField,
  StructType
}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/** Unit tests for BackfillApp.parseColumnMapping and
  * MilvusBackfill.applyColumnMapping — the column-mapping helpers introduced
  * for backfill parquet schema translation.
  */
class ColumnMappingTest
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("ColumnMappingTest")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  // ============ parseColumnMapping ============

  test("parseColumnMapping parses a simple mapping") {
    BackfillApp.parseColumnMapping("a:x,b:y") shouldBe Map(
      "a" -> "x",
      "b" -> "y"
    )
  }

  test("parseColumnMapping tolerates whitespace around tokens") {
    BackfillApp.parseColumnMapping(" a : x , b:y ") shouldBe Map(
      "a" -> "x",
      "b" -> "y"
    )
  }

  test("parseColumnMapping tolerates trailing comma and empty segments") {
    BackfillApp.parseColumnMapping("a:x,,b:y,") shouldBe Map(
      "a" -> "x",
      "b" -> "y"
    )
  }

  test("parseColumnMapping rejects empty input") {
    val ex = intercept[IllegalArgumentException] {
      BackfillApp.parseColumnMapping("   ")
    }
    ex.getMessage should include("cannot be empty")
  }

  test("parseColumnMapping rejects malformed entries") {
    intercept[IllegalArgumentException](BackfillApp.parseColumnMapping("a"))
    intercept[IllegalArgumentException](BackfillApp.parseColumnMapping(":x"))
    intercept[IllegalArgumentException](BackfillApp.parseColumnMapping("a:"))
    val ex = intercept[IllegalArgumentException] {
      BackfillApp.parseColumnMapping("a:x,bogus")
    }
    ex.getMessage should include("bogus")
  }

  test("parseColumnMapping rejects duplicate source columns") {
    val ex = intercept[IllegalArgumentException] {
      BackfillApp.parseColumnMapping("a:x,a:y")
    }
    ex.getMessage should include("duplicate source")
    ex.getMessage should include("a")
  }

  // ============ applyColumnMapping ============

  private def buildDf(cols: Seq[String], rows: Seq[Seq[Any]]): DataFrame = {
    val schema = StructType(
      cols.map(c =>
        StructField(c, if (c == "pk" || c == "id") IntegerType else StringType)
      )
    )
    val javaRows = rows.map(r => Row.fromSeq(r))
    spark.createDataFrame(
      spark.sparkContext.parallelize(javaRows),
      schema
    )
  }

  private def withCaseSensitiveAnalysis[T](enabled: Boolean)(body: => T): T = {
    val previous = spark.conf.get("spark.sql.caseSensitive", "false")
    spark.conf.set("spark.sql.caseSensitive", enabled.toString)
    try {
      body
    } finally {
      spark.conf.set("spark.sql.caseSensitive", previous)
    }
  }

  test("applyColumnMapping: explicit mapping renames, drops unlisted columns") {
    val df = buildDf(
      Seq("pk", "vec", "extra"),
      Seq(Seq(1, "v1", "drop-me"), Seq(2, "v2", "drop-me-2"))
    )
    val mapping = Some(Map("pk" -> "id", "vec" -> "embedding"))
    val result = MilvusBackfill.applyColumnMapping(df, "id", mapping)
    result.isRight shouldBe true
    val out = result.toOption.get
    out.columns.toSeq should contain theSameElementsAs Seq("id", "embedding")
    val collected =
      out.orderBy("id").collect().map(r => (r.getInt(0), r.getString(1))).toSeq
    collected shouldBe Seq((1, "v1"), (2, "v2"))
  }

  test("applyColumnMapping: chain-rename {a→b, b→c} preserves data pairing") {
    val df = buildDf(
      Seq("a", "b", "pk"),
      Seq(Seq("A1", "B1", 1), Seq("A2", "B2", 2))
    )
    val mapping = Some(Map("a" -> "b", "b" -> "c", "pk" -> "id"))
    val result = MilvusBackfill.applyColumnMapping(df, "id", mapping)
    result.isRight shouldBe true
    val out = result.toOption.get
    out.columns.toSet shouldBe Set("b", "c", "id")
    val rows = out.orderBy("id").collect()
    // Column "b" must contain the original "a" values; column "c" must
    // contain the original "b" values. The buggy foldLeft would mix these.
    rows.map(r => r.getAs[String]("b")).toSeq shouldBe Seq("A1", "A2")
    rows.map(r => r.getAs[String]("c")).toSeq shouldBe Seq("B1", "B2")
  }

  test("applyColumnMapping: swap {a→b, b→a} preserves data pairing") {
    val df = buildDf(
      Seq("a", "b", "pk"),
      Seq(Seq("A1", "B1", 1))
    )
    val mapping = Some(Map("a" -> "b", "b" -> "a", "pk" -> "id"))
    val result = MilvusBackfill.applyColumnMapping(df, "id", mapping)
    result.isRight shouldBe true
    val out = result.toOption.get
    out.columns.toSet shouldBe Set("a", "b", "id")
    val row = out.collect().head
    row.getAs[String]("b") shouldBe "A1" // original a
    row.getAs[String]("a") shouldBe "B1" // original b
  }

  test("applyColumnMapping: legacy path renames 'pk' to pkName") {
    val df = buildDf(Seq("pk", "vec"), Seq(Seq(1, "v1")))
    val result = MilvusBackfill.applyColumnMapping(df, "id", None)
    result.isRight shouldBe true
    val out = result.toOption.get
    out.columns.toSeq should contain theSameElementsAs Seq("id", "vec")
  }

  test("applyColumnMapping: legacy path errors when 'pk' column missing") {
    val df = buildDf(Seq("id", "vec"), Seq(Seq(1, "v1")))
    val result = MilvusBackfill.applyColumnMapping(df, "id", None)
    result.isLeft shouldBe true
    result.left.toOption.get shouldBe a[SchemaValidationError]
    result.left.toOption.get.message should include("'pk' column")
  }

  test(
    "applyColumnMapping: legacy path errors when both 'pk' and pkName exist"
  ) {
    val df = buildDf(Seq("pk", "id", "vec"), Seq(Seq(1, 2, "v1")))
    val result = MilvusBackfill.applyColumnMapping(df, "id", None)
    result.isLeft shouldBe true
    val err = result.left.toOption.get
    err shouldBe a[SchemaValidationError]
    err.message should include("'pk'")
    err.message should include("'id'")
    err.message should include("--column-mapping")
  }

  test(
    "applyColumnMapping: explicit mapping with missing source column errors"
  ) {
    val df = buildDf(Seq("pk", "vec"), Seq(Seq(1, "v1")))
    val mapping = Some(Map("pk" -> "id", "missing" -> "x"))
    val result = MilvusBackfill.applyColumnMapping(df, "id", mapping)
    result.isLeft shouldBe true
    val err = result.left.toOption.get
    err shouldBe a[SchemaValidationError]
    err.message should include("missing")
  }

  test("applyColumnMapping: explicit mapping with duplicate targets errors") {
    val df = buildDf(Seq("pk", "a", "b"), Seq(Seq(1, "a1", "b1")))
    val mapping = Some(Map("pk" -> "id", "a" -> "x", "b" -> "x"))
    val result = MilvusBackfill.applyColumnMapping(df, "id", mapping)
    result.isLeft shouldBe true
    val err = result.left.toOption.get
    err shouldBe a[SchemaValidationError]
    err.message should include("duplicate targets")
  }

  test("applyColumnMapping: explicit mapping missing pkName as target errors") {
    val df = buildDf(Seq("pk", "vec"), Seq(Seq(1, "v1")))
    val mapping = Some(Map("pk" -> "wrong_pk", "vec" -> "embedding"))
    val result = MilvusBackfill.applyColumnMapping(df, "id", mapping)
    result.isLeft shouldBe true
    val err = result.left.toOption.get
    err shouldBe a[SchemaValidationError]
    err.message should include("primary key")
  }

  test("applyColumnMapping: explicit mapping with only pkName target errors") {
    val df = buildDf(Seq("pk"), Seq(Seq(1)))
    val mapping = Some(Map("pk" -> "id"))
    val result = MilvusBackfill.applyColumnMapping(df, "id", mapping)
    result.isLeft shouldBe true
    val err = result.left.toOption.get
    err shouldBe a[SchemaValidationError]
    err.message should include("non-PK")
  }

  // ============ prepareBackfillData ============

  private def resolvedPk(name: String): ResolvedJoinKey =
    ResolvedJoinKey.primaryKey(name, 100L, None)

  private def resolvedPhysical(name: String): ResolvedJoinKey =
    ResolvedJoinKey.physicalField(
      name,
      101L,
      StructField(name, StringType, nullable = false)
    )

  test(
    "prepareBackfillData keeps legacy pk mapping but uses internal join alias"
  ) {
    val df = buildDf(
      Seq("pk", "vec"),
      Seq(Seq(1, "v1"), Seq(2, "v2"))
    )

    val prepared = MilvusBackfill
      .prepareBackfillData(df, resolvedPk("id"), None)
      .toOption
      .get

    prepared.joinColumns shouldBe Seq(ResolvedJoinKey.internalColumn(0))
    prepared.targetFieldNames shouldBe Seq("vec")
    prepared.dataFrame.columns.toSeq shouldBe Seq(
      ResolvedJoinKey.internalColumn(0),
      "vec"
    )
    prepared.dataFrame
      .orderBy(ResolvedJoinKey.internalColumn(0))
      .collect()
      .map(r => (r.getInt(0), r.getString(1)))
      .toSeq shouldBe Seq((1, "v1"), (2, "v2"))
  }

  test(
    "prepareBackfillData separates explicitly mapped PK from target fields"
  ) {
    val df = buildDf(
      Seq("source_id", "vec", "drop_me"),
      Seq(Seq("1", "v1", "x"))
    )
    val mapping = Some(Map("source_id" -> "id", "vec" -> "embedding"))

    val prepared = MilvusBackfill
      .prepareBackfillData(df, resolvedPk("id"), mapping)
      .toOption
      .get

    prepared.dataFrame.columns.toSeq shouldBe Seq(
      ResolvedJoinKey.internalColumn(0),
      "embedding"
    )
    prepared.targetFieldNames shouldBe Seq("embedding")
  }

  test("prepareBackfillData consumes a same-name physical join field") {
    val df = buildDf(
      Seq("external_row_id", "vec"),
      Seq(Seq("row-1", "v1"))
    )

    val prepared = MilvusBackfill
      .prepareBackfillData(
        df,
        resolvedPhysical("external_row_id"),
        BackfillJoinKey.PhysicalField("external_row_id"),
        None,
        Seq("external_row_id", "vec")
      )
      .toOption
      .get

    prepared.dataFrame.columns.toSeq shouldBe Seq(
      ResolvedJoinKey.internalColumn(0),
      "vec"
    )
    prepared.targetFieldNames shouldBe Seq("vec")
    prepared.dataFrame.collect().head.getString(0) shouldBe "row-1"
  }

  test("prepareBackfillData maps a differently named physical input key") {
    val df = buildDf(
      Seq("source_row_id", "vec", "ignored"),
      Seq(Seq("row-1", "v1", "drop"))
    )
    val mapping = Some(
      Map("source_row_id" -> "external_row_id", "vec" -> "embedding")
    )

    val prepared = MilvusBackfill
      .prepareBackfillData(
        df,
        resolvedPhysical("external_row_id"),
        BackfillJoinKey.PhysicalField("external_row_id"),
        mapping,
        Seq("external_row_id", "embedding")
      )
      .toOption
      .get

    prepared.dataFrame.columns.toSeq shouldBe Seq(
      ResolvedJoinKey.internalColumn(0),
      "embedding"
    )
    prepared.targetFieldNames shouldBe Seq("embedding")
  }

  test("prepareBackfillData rejects a missing physical join field") {
    val df = buildDf(Seq("pk", "vec"), Seq(Seq(1, "v1")))

    val error = MilvusBackfill
      .prepareBackfillData(
        df,
        resolvedPhysical("external_row_id"),
        BackfillJoinKey.PhysicalField("external_row_id"),
        None,
        Seq("external_row_id", "vec")
      )
      .left
      .toOption
      .get

    error.message should include("physical join-key column")
    error.message should include("external_row_id")
  }

  test("prepareBackfillData requires the mapped physical join-key target") {
    val df = buildDf(
      Seq("source_row_id", "vec"),
      Seq(Seq("row-1", "v1"))
    )

    val error = MilvusBackfill
      .prepareBackfillData(
        df,
        resolvedPhysical("external_row_id"),
        BackfillJoinKey.PhysicalField("external_row_id"),
        Some(Map("source_row_id" -> "wrong_key", "vec" -> "embedding")),
        Seq("external_row_id", "embedding")
      )
      .left
      .toOption
      .get

    error.message should include("physical join-key field")
    error.message should include("external_row_id")
  }

  test("prepareBackfillData preserves a target named like the default alias") {
    val defaultAlias = ResolvedJoinKey.internalColumn(0)
    val allocatedAlias = ResolvedJoinKey.internalColumn(0, 1)
    val df = buildDf(
      Seq("pk", defaultAlias, "vec"),
      Seq(Seq(1, "reserved", "v1"))
    )

    val prepared = MilvusBackfill
      .prepareBackfillData(df, resolvedPk("id"), None)
      .toOption
      .get

    prepared.joinColumns shouldBe Seq(allocatedAlias)
    prepared.joinKey.internalColumns shouldBe Seq(allocatedAlias)
    prepared.targetFieldNames shouldBe Seq(defaultAlias, "vec")
    prepared.dataFrame.columns.toSeq shouldBe Seq(
      allocatedAlias,
      defaultAlias,
      "vec"
    )
    val row = prepared.dataFrame.collect().head
    row.getAs[Int](allocatedAlias) shouldBe 1
    row.getAs[String](defaultAlias) shouldBe "reserved"
  }

  test("prepareBackfillData avoids case variants under the default resolver") {
    withCaseSensitiveAnalysis(false) {
      val caseVariant = "__BF_JOIN_0__"
      val allocatedAlias = ResolvedJoinKey.internalColumn(0, 1)
      val df = buildDf(
        Seq("pk", caseVariant, "vec"),
        Seq(Seq(1, "target", "v1"))
      )

      val prepared = MilvusBackfill
        .prepareBackfillData(df, resolvedPk("id"), None)
        .toOption
        .get

      prepared.joinColumns shouldBe Seq(allocatedAlias)
      prepared.targetFieldNames shouldBe Seq(caseVariant, "vec")
      MilvusBackfill
        .validateJoinKeyCardinality(
          prepared.dataFrame,
          prepared.joinColumns,
          Seq("id"),
          "Backfill parquet"
        )
        .isRight shouldBe true
    }
  }

  test("prepareBackfillData honors Spark's case-sensitive resolver") {
    withCaseSensitiveAnalysis(true) {
      val caseVariant = "__BF_JOIN_0__"
      val defaultAlias = ResolvedJoinKey.internalColumn(0)
      val df = buildDf(
        Seq("pk", caseVariant, "vec"),
        Seq(Seq(1, "target", "v1"))
      )

      val prepared = MilvusBackfill
        .prepareBackfillData(df, resolvedPk("id"), None)
        .toOption
        .get

      prepared.joinColumns shouldBe Seq(defaultAlias)
      prepared.targetFieldNames shouldBe Seq(caseVariant, "vec")
      prepared.dataFrame.columns.toSeq shouldBe Seq(
        defaultAlias,
        caseVariant,
        "vec"
      )
    }
  }

  test(
    "prepareBackfillData permits a collection PK named like the internal alias"
  ) {
    val internal = ResolvedJoinKey.internalColumn(0)
    val allocatedAlias = ResolvedJoinKey.internalColumn(0, 1)
    val df = buildDf(Seq("pk", "vec"), Seq(Seq(1, "v1")))

    val prepared = MilvusBackfill
      .prepareBackfillData(df, resolvedPk(internal), None)
      .toOption
      .get

    prepared.joinColumns shouldBe Seq(allocatedAlias)
    prepared.dataFrame.columns.toSeq shouldBe Seq(allocatedAlias, "vec")
    prepared.targetFieldNames shouldBe Seq("vec")
  }

  test("prepareBackfillData avoids collection-only field-name collisions") {
    val defaultAlias = ResolvedJoinKey.internalColumn(0)
    val allocatedAlias = ResolvedJoinKey.internalColumn(0, 1)
    val df = buildDf(Seq("pk", "vec"), Seq(Seq(1, "v1")))

    val prepared = MilvusBackfill
      .prepareBackfillData(
        df,
        resolvedPk("id"),
        None,
        collectionFieldNames = Seq(defaultAlias)
      )
      .toOption
      .get

    prepared.joinColumns shouldBe Seq(allocatedAlias)
    prepared.dataFrame.columns.toSeq shouldBe Seq(allocatedAlias, "vec")
  }

  test("prepareBackfillData normalizes multiple internal join components") {
    val joinKey = ResolvedJoinKey(
      kind = "test_composite",
      components = Seq(
        ResolvedJoinComponent(
          "id",
          100L,
          None,
          ResolvedJoinKey.internalColumn(0)
        ),
        ResolvedJoinComponent(
          "row_number",
          101L,
          None,
          ResolvedJoinKey.internalColumn(1)
        )
      )
    )
    val df = buildDf(
      Seq("pk", "row_number", "vec"),
      Seq(Seq(1, "7", "v1"))
    )

    val prepared = MilvusBackfill
      .prepareBackfillData(df, joinKey, None)
      .toOption
      .get

    prepared.joinColumns shouldBe Seq(
      ResolvedJoinKey.internalColumn(0),
      ResolvedJoinKey.internalColumn(1)
    )
    prepared.targetFieldNames shouldBe Seq("vec")
  }
}
