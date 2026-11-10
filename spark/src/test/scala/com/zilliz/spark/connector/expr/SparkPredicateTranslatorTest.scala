package com.zilliz.spark.connector.expr

import org.apache.spark.sql.connector.expressions.{
  Expression,
  Expressions,
  GeneralScalarExpression,
  Literal => SparkLiteral
}
import org.apache.spark.sql.connector.expressions.filter.{
  And => SparkAnd,
  Not => SparkNot,
  Or => SparkOr,
  Predicate
}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.expr.{
  And,
  Comparison,
  ComparisonOperator,
  EndsWith,
  FieldRef,
  In,
  IsNotNull,
  IsNull,
  Not,
  Or,
  StartsWith
}
import com.zilliz.milvus.storage.expr.Literal._
import com.zilliz.milvus.storage.schema.FieldMetadata
import io.milvus.grpc.schema.{DataType => MilvusDataType}

class SparkPredicateTranslatorTest extends AnyFunSuite with Matchers {

  private def metadata(
      fieldId: Long,
      dataType: MilvusDataType
  ): Metadata =
    new MetadataBuilder()
      .putLong(FieldMetadata.MilvusFieldIdMetadataKey, fieldId)
      .putLong(FieldMetadata.MilvusDataTypeMetadataKey, dataType.value.toLong)
      .build()

  private def field(
      name: String,
      sparkType: DataType,
      fieldId: Long,
      milvusType: MilvusDataType
  ): StructField =
    StructField(
      name,
      sparkType,
      nullable = true,
      metadata(fieldId, milvusType)
    )

  private val schema = StructType(
    Seq(
      field("b", BooleanType, 10L, MilvusDataType.Bool),
      field("i8", ByteType, 11L, MilvusDataType.Int8),
      field("i16", ShortType, 12L, MilvusDataType.Int16),
      field("i32", IntegerType, 13L, MilvusDataType.Int32),
      field("i64", LongType, 14L, MilvusDataType.Int64),
      field("f", FloatType, 15L, MilvusDataType.Float),
      field("d", DoubleType, 16L, MilvusDataType.Double),
      field("s", StringType, 17L, MilvusDataType.VarChar),
      field("json", StringType, 18L, MilvusDataType.JSON),
      field(
        "array",
        ArrayType(IntegerType, containsNull = true),
        19L,
        MilvusDataType.Array
      ),
      field(
        "vector",
        ArrayType(FloatType, containsNull = false),
        20L,
        MilvusDataType.FloatVector
      ),
      StructField("synthetic", LongType),
      StructField(
        "bad_metadata",
        LongType,
        metadata = new MetadataBuilder()
          .putString(FieldMetadata.MilvusFieldIdMetadataKey, "bad")
          .putLong(
            FieldMetadata.MilvusDataTypeMetadataKey,
            MilvusDataType.Int64.value.toLong
          )
          .build()
      ),
      field("conflicting_type", LongType, 21L, MilvusDataType.Int32)
    )
  )

  private def reference(name: String): Expression = Expressions.column(name)

  private def literal(rawValue: Any, sparkType: DataType): Expression =
    new SparkLiteral[Any] {
      override def value(): Any = rawValue
      override def dataType(): DataType = sparkType
    }

  private def predicate(
      name: String,
      children: Expression*
  ): Predicate = new Predicate(name, children.toArray)

  private def translated(predicate: Predicate) =
    SparkPredicateTranslator.translate(predicate, schema)

  test("scalar equality preserves each supported Milvus type") {
    val cases = Seq(
      (
        "b",
        BooleanType,
        true,
        FieldRef(10L, MilvusDataType.Bool),
        BooleanValue(true)
      ),
      (
        "i8",
        ByteType,
        1.toByte,
        FieldRef(11L, MilvusDataType.Int8),
        IntegerValue(1L)
      ),
      (
        "i16",
        ShortType,
        2.toShort,
        FieldRef(12L, MilvusDataType.Int16),
        IntegerValue(2L)
      ),
      (
        "i32",
        IntegerType,
        3,
        FieldRef(13L, MilvusDataType.Int32),
        IntegerValue(3L)
      ),
      (
        "i64",
        LongType,
        4L,
        FieldRef(14L, MilvusDataType.Int64),
        IntegerValue(4L)
      ),
      (
        "f",
        FloatType,
        1.5f,
        FieldRef(15L, MilvusDataType.Float),
        FloatValue(1.5f)
      ),
      (
        "d",
        DoubleType,
        2.5d,
        FieldRef(16L, MilvusDataType.Double),
        DoubleValue(2.5d)
      ),
      (
        "s",
        StringType,
        UTF8String.fromString("abc"),
        FieldRef(17L, MilvusDataType.VarChar),
        StringValue("abc")
      )
    )

    cases.foreach { case (name, sparkType, value, ref, expected) =>
      val result = translated(
        predicate("=", reference(name), literal(value, sparkType))
      ).get
      result.expr shouldBe
        Comparison(ref, ComparisonOperator.EqualTo, expected)
      result.fieldIds shouldBe Set(ref.fieldId)
    }
  }

  test("ordered comparisons, null checks, IN and string predicates translate") {
    val longRef = FieldRef(14L, MilvusDataType.Int64)
    val operators = Seq(
      "<>" -> ComparisonOperator.NotEqualTo,
      "<=>" -> ComparisonOperator.EqualNullSafe,
      "<" -> ComparisonOperator.LessThan,
      "<=" -> ComparisonOperator.LessThanOrEqual,
      ">" -> ComparisonOperator.GreaterThan,
      ">=" -> ComparisonOperator.GreaterThanOrEqual
    )
    operators.foreach { case (name, operator) =>
      translated(
        predicate(name, reference("i64"), literal(7L, LongType))
      ).get.expr shouldBe Comparison(longRef, operator, IntegerValue(7L))
    }

    translated(predicate("IS_NULL", reference("i64"))).get.expr shouldBe
      IsNull(longRef)
    translated(predicate("IS_NOT_NULL", reference("i64"))).get.expr shouldBe
      IsNotNull(longRef)
    translated(
      predicate(
        "IN",
        reference("i64"),
        literal(1L, LongType),
        literal(null, NullType)
      )
    ).get.expr shouldBe In(longRef, Vector(IntegerValue(1L), NullValue))

    val stringRef = FieldRef(17L, MilvusDataType.VarChar)
    translated(
      predicate(
        "STARTS_WITH",
        reference("s"),
        literal(UTF8String.fromString("ab"), StringType)
      )
    ).get.expr shouldBe StartsWith(stringRef, "ab")
    translated(
      predicate("ENDS_WITH", reference("s"), literal("yz", StringType))
    ).get.expr shouldBe EndsWith(stringRef, "yz")
  }

  test("AND, OR and NOT are accepted only when every child translates") {
    val greater = predicate(">", reference("i64"), literal(1L, LongType))
    val prefix = predicate(
      "STARTS_WITH",
      reference("s"),
      literal("a", StringType)
    )
    val unsupported = predicate(
      "CONTAINS",
      reference("s"),
      literal("a", StringType)
    )

    translated(new SparkAnd(greater, prefix)).get.expr shouldBe And(
      Comparison(
        FieldRef(14L, MilvusDataType.Int64),
        ComparisonOperator.GreaterThan,
        IntegerValue(1L)
      ),
      StartsWith(FieldRef(17L, MilvusDataType.VarChar), "a")
    )
    translated(new SparkNot(greater)).get.expr shouldBe Not(
      Comparison(
        FieldRef(14L, MilvusDataType.Int64),
        ComparisonOperator.GreaterThan,
        IntegerValue(1L)
      )
    )
    translated(new SparkOr(greater, unsupported)) shouldBe None
    translated(new SparkNot(unsupported)) shouldBe None
  }

  test("unsupported types, shapes and malformed nodes remain residual") {
    Seq(
      "json",
      "array",
      "vector",
      "synthetic",
      "bad_metadata",
      "conflicting_type"
    )
      .foreach { name =>
        translated(
          predicate("IS_NULL", reference(name))
        ) shouldBe None
      }

    translated(
      predicate("<", reference("b"), literal(true, BooleanType))
    ) shouldBe None
    translated(
      predicate(
        "CONTAINS",
        reference("s"),
        literal("x", StringType)
      )
    ) shouldBe None
    translated(
      predicate(
        "=",
        new GeneralScalarExpression(
          "ADD",
          Array(reference("i64"), literal(1L, LongType))
        ),
        literal(2L, LongType)
      )
    ) shouldBe None
    translated(
      predicate(
        "=",
        Expressions.column("nested.i64"),
        literal(1L, LongType)
      )
    ) shouldBe None
    translated(predicate("=", reference("i64"))) shouldBe None

    val nullChildren = new Predicate("=", Array.empty) {
      override def children(): Array[Expression] = null
    }
    translated(nullChildren) shouldBe None
  }
}
