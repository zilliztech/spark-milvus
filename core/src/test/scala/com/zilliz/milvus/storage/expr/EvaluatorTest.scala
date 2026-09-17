package com.zilliz.milvus.storage.expr

import java.nio.charset.StandardCharsets.UTF_8
import scala.jdk.CollectionConverters._

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{
  BigIntVector,
  FieldVector,
  IntVector,
  VarCharVector,
  VectorSchemaRoot
}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.milvus.grpc.schema.{DataType, FieldSchema}

class EvaluatorTest extends AnyFunSuite with Matchers {

  test("batch evaluation resolves logical fields to physical numeric columns") {
    withBatch(
      new ArrowType.Int(64, true),
      Seq(Some(1L), Some(3L), None),
      "physical-id"
    ) { root =>
      val result = Evaluator.evaluate(
        PlanParser.parse("id >= 2"),
        root,
        field => if (field == "id") "physical-id" else "absent"
      )

      excluded(result) shouldBe Seq(0, 2)
    }
  }

  test("batch string comparisons use unsigned UTF-8 ordering") {
    val supplementary = new String(Character.toChars(0x10000))
    val bmp = "\uE000"
    supplementary.compareTo(bmp) should be < 0

    withBatch(
      new ArrowType.Utf8(),
      Seq(Some(supplementary), Some(bmp), None)
    ) { root =>
      val result = Evaluator.evaluate(
        PlanParser.parse(s"value > '$bmp'"),
        root,
        _ => "column"
      )

      excluded(result) shouldBe Seq(1, 2)
    }
  }

  test("only TRUE rows survive null predicates") {
    withBatch(
      new ArrowType.Int(64, true),
      Seq(None, Some(1L))
    ) { root =>
      excluded(
        Evaluator.evaluate(
          PlanParser.parse("id IS NULL"),
          root,
          _ => "column"
        )
      ) shouldBe Seq(1)
    }
  }

  test("Text fields use the string validation and Arrow UTF-8 contracts") {
    val expression = PlanParser.parse("body == '\u5168\u6587'")
    Evaluator.validate(
      expression,
      Seq(FieldSchema(name = "body", dataType = DataType.Text))
    )

    withBatch(
      new ArrowType.Utf8(),
      Seq(Some("\u5168\u6587"), Some("other"), None)
    ) { root =>
      excluded(
        Evaluator.evaluate(expression, root, _ => "column")
      ) shouldBe Seq(
        1,
        2
      )
    }
  }

  test("a missing physical filter column fails before row evaluation") {
    withBatch(new ArrowType.Int(64, true), Seq(Some(1L))) { root =>
      val error = intercept[IllegalStateException] {
        Evaluator.evaluate(
          PlanParser.parse("id == 1"),
          root,
          _ => "absent"
        )
      }

      error.getMessage should include("column 'absent'")
    }
  }

  test("a filter vector shorter than the batch fails before row evaluation") {
    withBatch(
      new ArrowType.Int(64, true),
      Seq(Some(1L), Some(2L), Some(3L))
    ) { root =>
      root.getVector("column").setValueCount(2)

      val error = intercept[IllegalStateException] {
        Evaluator.evaluate(
          PlanParser.parse("id == 1"),
          root,
          _ => "column"
        )
      }

      error.getMessage should include("2 values")
      error.getMessage should include("3 rows")
    }
  }

  test("an incompatible physical vector fails even for an empty batch") {
    withBatch(new ArrowType.Int(32, true), Seq.empty) { root =>
      val error = intercept[IllegalStateException] {
        Evaluator.evaluate(
          PlanParser.parse("name == 'alice'"),
          root,
          _ => "column"
        )
      }

      error.getMessage should include("IntVector")
      error.getMessage should include("String")
    }
  }

  private def excluded(bitmap: Bitmap): Seq[Int] =
    (0 until bitmap.rowCount).filter(bitmap.isExcluded)

  private def withBatch(
      arrowType: ArrowType,
      values: Seq[Option[Any]],
      columnName: String = "column"
  )(body: VectorSchemaRoot => Unit): Unit = {
    val allocator = new RootAllocator(Long.MaxValue)
    val field = new Field(
      columnName,
      new FieldType(true, arrowType, null),
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

  private def set(vector: FieldVector, row: Int, value: Any): Unit =
    vector match {
      case v: BigIntVector => v.setSafe(row, value.asInstanceOf[Long])
      case v: IntVector    => v.setSafe(row, value.asInstanceOf[Long].toInt)
      case v: VarCharVector =>
        v.setSafe(row, value.asInstanceOf[String].getBytes(UTF_8))
      case other => fail(s"test does not write ${other.getClass.getSimpleName}")
    }
}
