package com.zilliz.spark.connector.read

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{
  BinaryType,
  DoubleType,
  IntegerType,
  LongType,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.funsuite.AnyFunSuite

class RowFiltersTest extends AnyFunSuite {
  private val schema = StructType(
    Seq(
      StructField("id", LongType),
      StructField("n", IntegerType),
      StructField("d", DoubleType),
      StructField("s", StringType),
      StructField("b", BinaryType)
    )
  )
  private val row = InternalRow(
    7L,
    3,
    1.5d,
    UTF8String.fromString("abc"),
    Array[Byte](1, 2)
  )
  private val nullRow = InternalRow(null, null, null, null, null)

  private def holds(f: Filter, r: InternalRow = row): Boolean =
    RowFilters.evaluate(f, r, schema)

  test("comparisons on a long column accept an int literal") {
    assert(holds(EqualTo("id", 7)))
    assert(holds(GreaterThan("id", 6L)))
    assert(!holds(GreaterThan("id", 7)))
    assert(holds(GreaterThanOrEqual("id", 7)))
    assert(holds(LessThan("id", 8)))
    assert(holds(LessThanOrEqual("id", 7L)))
  }

  test("In, string and binary comparisons") {
    assert(holds(In("n", Array(1, 3))))
    assert(!holds(In("n", Array(1, 2))))
    assert(holds(EqualTo("s", "abc")))
    assert(holds(LessThan("s", "abd")))
    assert(holds(EqualTo("b", Array[Byte](1, 2))))
    assert(holds(GreaterThan("d", 1.0f)))
  }

  test("null handling: IsNull, IsNotNull and null sorts first") {
    assert(holds(IsNotNull("id")))
    assert(!holds(IsNull("id")))
    assert(holds(IsNull("id"), nullRow))
    assert(holds(LessThan("id", 0L), nullRow))
    assert(!holds(EqualTo("id", 7L), nullRow))
  }

  test("And and Or compose") {
    assert(holds(And(EqualTo("id", 7L), GreaterThan("n", 2))))
    assert(!holds(And(EqualTo("id", 7L), GreaterThan("n", 3))))
    assert(holds(Or(EqualTo("id", 0L), GreaterThan("n", 2))))
  }

  test("a column not in the schema and an unknown filter kind pass the row") {
    assert(holds(EqualTo("missing", 1)))
    assert(holds(StringStartsWith("s", "zzz")))
    assert(
      RowFilters.matches(Array(EqualTo("id", 7L), IsNotNull("s")), row, schema)
    )
    assert(
      !RowFilters.matches(Array(EqualTo("id", 7L), IsNull("s")), row, schema)
    )
  }
}
