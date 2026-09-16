package com.zilliz.milvus.storage.expr

import java.nio.charset.StandardCharsets
import scala.jdk.CollectionConverters._

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{
  BigIntVector,
  BitVector,
  FieldVector,
  Float4Vector,
  Float8Vector,
  IntVector,
  SmallIntVector,
  TinyIntVector,
  VarCharVector,
  VectorSchemaRoot
}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.expr.ComparisonOperator._
import com.zilliz.milvus.storage.expr.Literal._
import io.milvus.grpc.schema.{DataType => MilvusDataType}

class PredicateEvaluatorTest extends AnyFunSuite with Matchers {

  private val fieldId = 10L

  test("Boolean equality keeps only TRUE results") {
    withBatch(MilvusDataType.Bool, Seq(Some(true), Some(false), None)) { root =>
      val result = evaluate(
        Comparison(
          FieldRef(fieldId, MilvusDataType.Bool),
          EqualTo,
          BooleanValue(true)
        ),
        root
      )

      excluded(result) shouldBe Seq(1, 2)
    }
  }

  test("all four integer vectors use signed comparison") {
    val types = Seq(
      MilvusDataType.Int8,
      MilvusDataType.Int16,
      MilvusDataType.Int32,
      MilvusDataType.Int64
    )

    types.foreach { dataType =>
      withBatch(dataType, Seq(Some(-2L), Some(0L), Some(3L), None)) { root =>
        val result = evaluate(
          Comparison(
            FieldRef(fieldId, dataType),
            GreaterThan,
            IntegerValue(0L)
          ),
          root
        )

        withClue(dataType.toString) {
          excluded(result) shouldBe Seq(0, 1, 3)
        }
      }
    }
  }

  test("ordered fields implement every comparison operator") {
    val field = FieldRef(fieldId, MilvusDataType.Int32)
    val values = Seq(Some(1L), Some(2L), Some(3L), None)
    val expected = Seq(
      EqualTo -> Seq(0, 2, 3),
      NotEqualTo -> Seq(1, 3),
      EqualNullSafe -> Seq(0, 2, 3),
      LessThan -> Seq(1, 2, 3),
      LessThanOrEqual -> Seq(2, 3),
      GreaterThan -> Seq(0, 1, 3),
      GreaterThanOrEqual -> Seq(0, 3)
    )

    withBatch(MilvusDataType.Int32, values) { root =>
      expected.foreach { case (operator, excludedRows) =>
        withClue(operator.toString) {
          excluded(
            evaluate(
              Comparison(field, operator, IntegerValue(2L)),
              root
            )
          ) shouldBe excludedRows
        }
      }
    }
  }

  test("ordinary comparison and NOT preserve UNKNOWN for null rows") {
    withBatch(
      MilvusDataType.Int32,
      Seq(None, Some(1L), Some(2L))
    ) { root =>
      val result = evaluate(
        Not(
          Comparison(
            FieldRef(fieldId, MilvusDataType.Int32),
            EqualTo,
            IntegerValue(1L)
          )
        ),
        root
      )

      excluded(result) shouldBe Seq(0, 1)
    }
  }

  test("AND and OR implement SQL three-valued logic") {
    val field = FieldRef(fieldId, MilvusDataType.Int32)
    withBatch(MilvusDataType.Int32, Seq(None, Some(1L), Some(2L), Some(3L))) {
      root =>
        val either = evaluate(
          Or(
            Comparison(field, EqualTo, IntegerValue(1L)),
            Comparison(field, EqualTo, IntegerValue(2L))
          ),
          root
        )
        excluded(either) shouldBe Seq(0, 3)

        val unknownOrTrue = evaluate(
          Or(Comparison(field, EqualTo, NullValue), IsNull(field)),
          root
        )
        excluded(unknownOrTrue) shouldBe Seq(1, 2, 3)

        val bounded = evaluate(
          And(
            Comparison(field, GreaterThan, IntegerValue(1L)),
            Comparison(field, LessThan, IntegerValue(3L))
          ),
          root
        )
        excluded(bounded) shouldBe Seq(0, 1, 3)
    }
  }

  test("null-safe equality is two-valued") {
    val field = FieldRef(fieldId, MilvusDataType.Int64)
    withBatch(MilvusDataType.Int64, Seq(None, Some(1L), Some(2L))) { root =>
      excluded(
        evaluate(Comparison(field, EqualNullSafe, NullValue), root)
      ) shouldBe Seq(1, 2)

      excluded(
        evaluate(Comparison(field, EqualNullSafe, IntegerValue(1L)), root)
      ) shouldBe Seq(0, 2)
    }
  }

  test("IN returns UNKNOWN after a miss when its list contains null") {
    val field = FieldRef(fieldId, MilvusDataType.Int32)
    withBatch(MilvusDataType.Int32, Seq(None, Some(1L), Some(2L))) { root =>
      val withNull = evaluate(
        In(field, Vector(IntegerValue(1L), NullValue)),
        root
      )
      excluded(withNull) shouldBe Seq(0, 2)

      val withoutNull = evaluate(In(field, Vector(IntegerValue(1L))), root)
      excluded(withoutNull) shouldBe Seq(0, 2)

      evaluate(In(field, Vector.empty), root).isFull shouldBe true
    }
  }

  test("IS NULL and IS NOT NULL are complementary") {
    val field = FieldRef(fieldId, MilvusDataType.VarChar)
    withBatch(MilvusDataType.VarChar, Seq(None, Some("x"))) { root =>
      excluded(evaluate(IsNull(field), root)) shouldBe Seq(1)
      excluded(evaluate(IsNotNull(field), root)) shouldBe Seq(0)
    }
  }

  test("Float comparisons follow Spark NaN and signed-zero ordering") {
    val field = FieldRef(fieldId, MilvusDataType.Float)
    withBatch(
      MilvusDataType.Float,
      Seq(Some(Float.NaN), Some(-0.0f), Some(0.0f), Some(1.0f))
    ) { root =>
      excluded(
        evaluate(Comparison(field, EqualTo, FloatValue(Float.NaN)), root)
      ) shouldBe Seq(1, 2, 3)

      excluded(
        evaluate(Comparison(field, EqualTo, FloatValue(0.0f)), root)
      ) shouldBe Seq(0, 3)

      excluded(
        evaluate(Comparison(field, GreaterThan, FloatValue(1.0f)), root)
      ) shouldBe Seq(1, 2, 3)
    }
  }

  test("Double comparisons follow Spark NaN and signed-zero ordering") {
    val field = FieldRef(fieldId, MilvusDataType.Double)
    withBatch(
      MilvusDataType.Double,
      Seq(Some(Double.NaN), Some(-0.0d), Some(0.0d), Some(1.0d))
    ) { root =>
      excluded(
        evaluate(Comparison(field, EqualTo, DoubleValue(Double.NaN)), root)
      ) shouldBe Seq(1, 2, 3)

      excluded(
        evaluate(Comparison(field, NotEqualTo, DoubleValue(0.0d)), root)
      ) shouldBe Seq(1, 2)
    }
  }

  test("string ordering is unsigned UTF-8 rather than JVM UTF-16") {
    val supplementary = "\uD800\uDC00" // U+10000
    val bmp = "\uE000"
    supplementary.compareTo(bmp) should be < 0

    val field = FieldRef(fieldId, MilvusDataType.String)
    withBatch(
      MilvusDataType.String,
      Seq(Some(supplementary), Some(bmp), None)
    ) { root =>
      val result = evaluate(
        Comparison(field, GreaterThan, StringValue(bmp)),
        root
      )

      excluded(result) shouldBe Seq(1, 2)
    }
  }

  test("string prefix and suffix operate on UTF-8 bytes") {
    val field = FieldRef(fieldId, MilvusDataType.Text)
    withBatch(
      MilvusDataType.Text,
      Seq(Some("éclair世界"), Some("école"), Some("clair世界"), None)
    ) { root =>
      excluded(evaluate(StartsWith(field, "é"), root)) shouldBe Seq(2, 3)
      excluded(evaluate(EndsWith(field, "世界"), root)) shouldBe Seq(1, 3)
    }
  }

  test("empty, all-surviving and all-excluded batches keep bitmap polarity") {
    val field = FieldRef(fieldId, MilvusDataType.Int64)
    withBatch(MilvusDataType.Int64, Seq.empty) { root =>
      val result = evaluate(IsNotNull(field), root)
      result.rowCount shouldBe 0
      result.isEmpty shouldBe true
      result.isFull shouldBe true
    }

    withBatch(MilvusDataType.Int64, Seq(Some(1L), Some(2L))) { root =>
      evaluate(IsNotNull(field), root).isEmpty shouldBe true
      evaluate(IsNull(field), root).isFull shouldBe true
    }
  }

  test("field ids are resolved through columnNameFor") {
    withBatch(
      MilvusDataType.Int64,
      Seq(Some(7L)),
      columnName = "physical-id-10"
    ) { root =>
      val result = PredicateEvaluator.evaluate(
        Comparison(
          FieldRef(fieldId, MilvusDataType.Int64),
          EqualTo,
          IntegerValue(7L)
        ),
        root,
        id => if (id == fieldId) Some("physical-id-10") else None
      )

      result.isEmpty shouldBe true
    }
  }

  test("missing field bindings and columns fail clearly") {
    val expression = IsNotNull(FieldRef(fieldId, MilvusDataType.Int64))
    withBatch(MilvusDataType.Int64, Seq(Some(1L))) { root =>
      val missingBinding = intercept[IllegalStateException] {
        PredicateEvaluator.evaluate(expression, root, _ => None)
      }
      missingBinding.getMessage should include("field 10")

      val missingColumn = intercept[IllegalStateException] {
        PredicateEvaluator.evaluate(expression, root, _ => Some("absent"))
      }
      missingColumn.getMessage should include("column 'absent'")
    }
  }

  test("an Arrow vector incompatible with the declared field type fails") {
    withBatch(MilvusDataType.Int32, Seq(Some(1L))) { root =>
      val error = intercept[IllegalStateException] {
        evaluate(IsNotNull(FieldRef(fieldId, MilvusDataType.Int64)), root)
      }

      error.getMessage should include("Int64")
      error.getMessage should include("IntVector")
    }
  }

  test(
    "malformed literals and unsupported operators fail before reading rows"
  ) {
    withBatch(MilvusDataType.Int8, Seq.empty) { root =>
      val wrongLiteral = intercept[IllegalArgumentException] {
        evaluate(
          Comparison(
            FieldRef(fieldId, MilvusDataType.Int8),
            EqualTo,
            StringValue("1")
          ),
          root
        )
      }
      wrongLiteral.getMessage should include("IntegerValue")

      val outOfRange = intercept[IllegalArgumentException] {
        evaluate(
          Comparison(
            FieldRef(fieldId, MilvusDataType.Int8),
            EqualTo,
            IntegerValue(128L)
          ),
          root
        )
      }
      outOfRange.getMessage should include("outside Int8 range")
    }

    withBatch(MilvusDataType.Bool, Seq.empty) { root =>
      val error = intercept[IllegalArgumentException] {
        evaluate(
          Comparison(
            FieldRef(fieldId, MilvusDataType.Bool),
            LessThan,
            BooleanValue(true)
          ),
          root
        )
      }
      error.getMessage should include("not supported for Bool")
    }
  }

  test("one field id cannot declare two Milvus types") {
    withBatch(MilvusDataType.Int32, Seq(Some(1L))) { root =>
      val error = intercept[IllegalArgumentException] {
        evaluate(
          And(
            IsNotNull(FieldRef(fieldId, MilvusDataType.Int32)),
            IsNotNull(FieldRef(fieldId, MilvusDataType.Int64))
          ),
          root
        )
      }

      error.getMessage should include("conflicting expression types")
    }
  }

  private def evaluate(
      expression: PredicateExpr,
      root: VectorSchemaRoot
  ): Bitmap =
    PredicateEvaluator.evaluate(expression, root, _ => Some("column"))

  private def excluded(bitmap: Bitmap): Seq[Int] =
    (0 until bitmap.rowCount).filter(bitmap.isExcluded)

  private def withBatch(
      dataType: MilvusDataType,
      values: Seq[Option[Any]],
      columnName: String = "column"
  )(body: VectorSchemaRoot => Unit): Unit = {
    val allocator = new RootAllocator(Long.MaxValue)
    val field = new Field(
      columnName,
      new FieldType(true, arrowType(dataType), null),
      java.util.Collections.emptyList[Field]()
    )
    val root = VectorSchemaRoot.create(new Schema(Seq(field).asJava), allocator)
    try {
      root.allocateNew()
      val vector = root.getVector(columnName)
      values.zipWithIndex.foreach { case (value, row) =>
        value match {
          case None        => vector.setNull(row)
          case Some(value) => set(vector, row, value)
        }
      }
      root.setRowCount(values.size)
      body(root)
    } finally {
      root.close()
      allocator.close()
    }
  }

  private def arrowType(dataType: MilvusDataType): ArrowType =
    dataType match {
      case MilvusDataType.Bool  => new ArrowType.Bool()
      case MilvusDataType.Int8  => new ArrowType.Int(8, true)
      case MilvusDataType.Int16 => new ArrowType.Int(16, true)
      case MilvusDataType.Int32 => new ArrowType.Int(32, true)
      case MilvusDataType.Int64 => new ArrowType.Int(64, true)
      case MilvusDataType.Float =>
        new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
      case MilvusDataType.Double =>
        new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
      case MilvusDataType.String | MilvusDataType.VarChar |
          MilvusDataType.Text =>
        new ArrowType.Utf8()
      case other => fail(s"test does not create $other vectors")
    }

  private def set(vector: FieldVector, row: Int, value: Any): Unit =
    vector match {
      case v: BitVector =>
        v.setSafe(row, if (value.asInstanceOf[Boolean]) 1 else 0)
      case v: TinyIntVector =>
        v.setSafe(row, value.asInstanceOf[Long].toByte)
      case v: SmallIntVector =>
        v.setSafe(row, value.asInstanceOf[Long].toShort)
      case v: IntVector =>
        v.setSafe(row, value.asInstanceOf[Long].toInt)
      case v: BigIntVector =>
        v.setSafe(row, value.asInstanceOf[Long])
      case v: Float4Vector =>
        v.setSafe(row, value.asInstanceOf[Float])
      case v: Float8Vector =>
        v.setSafe(row, value.asInstanceOf[Double])
      case v: VarCharVector =>
        v.setSafe(
          row,
          value.asInstanceOf[String].getBytes(StandardCharsets.UTF_8)
        )
      case other => fail(s"test cannot populate ${other.getClass.getName}")
    }
}
