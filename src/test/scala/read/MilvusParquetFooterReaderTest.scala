package com.zilliz.spark.connector.read

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Tests for [[MilvusParquetFooterReader]]. The actual footer read against S3
  * needs minio + hadoop-aws at runtime, so here we only cover the pure parser
  * logic. End-to-end footer reads are covered by the backfill integration test.
  */
class MilvusParquetFooterReaderTest extends AnyFunSuite with Matchers {

  test("parseGroupFieldIdList handles the multi-field sample") {
    val parsed =
      MilvusParquetFooterReader.parseGroupFieldIdList("100,0,1;101;102")
    parsed shouldBe Seq(Seq(100L, 0L, 1L), Seq(101L), Seq(102L))
  }

  test("parseGroupFieldIdList tolerates trailing semicolons") {
    MilvusParquetFooterReader.parseGroupFieldIdList(
      "103;"
    ) shouldBe Seq(Seq(103L))
  }

  test("parseGroupFieldIdList on null/empty returns empty") {
    MilvusParquetFooterReader.parseGroupFieldIdList(null) shouldBe Seq.empty
    MilvusParquetFooterReader.parseGroupFieldIdList("") shouldBe Seq.empty
  }

  test("parseGroupFieldIdList on single group of one field") {
    MilvusParquetFooterReader.parseGroupFieldIdList("103") shouldBe Seq(
      Seq(103L)
    )
  }

  test("parseGroupFieldIdList silently drops empty chunks") {
    // Milvus never emits `;;`, but being lenient avoids spurious failures on
    // trailing or duplicated separators.
    MilvusParquetFooterReader.parseGroupFieldIdList(
      "100,0,1;;102"
    ) shouldBe Seq(
      Seq(100L, 0L, 1L),
      Seq(102L)
    )
  }
}
