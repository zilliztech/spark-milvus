package com.zilliz.milvus.storage.expr

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.milvus.grpc.schema.{DataType, FieldSchema}

class PlanParserTest extends AnyFunSuite with Matchers {

  private val fields = Seq(
    FieldSchema(name = "id", dataType = DataType.Int64),
    FieldSchema(name = "rating", dataType = DataType.Float),
    FieldSchema(name = "score", dataType = DataType.Double),
    FieldSchema(name = "active", dataType = DataType.Bool),
    FieldSchema(name = "category", dataType = DataType.VarChar),
    FieldSchema(name = "embedding", dataType = DataType.FloatVector)
  )

  private def evaluate(
      text: String,
      values: (String, Any)*
  ): Option[Boolean] = {
    val expression = PlanParser.parse(text)
    Evaluator.validate(expression, fields)
    val row = values.toMap
    expression.evaluate(name => row.getOrElse(name, null))
  }

  test(
    "comparison operators preserve Int64 precision and signed decimal limits"
  ) {
    evaluate("id == 9223372036854775807", "id" -> Long.MaxValue) shouldBe Some(
      true
    )
    evaluate("id == -9223372036854775808", "id" -> Long.MinValue) shouldBe Some(
      true
    )
    evaluate("id > 9007199254740992", "id" -> 9007199254740993L) shouldBe Some(
      true
    )
    evaluate("id >= +3", "id" -> 3L) shouldBe Some(true)
    evaluate("id <= 3", "id" -> 3L) shouldBe Some(true)
    evaluate("id < 3", "id" -> 3L) shouldBe Some(false)
    evaluate("id != 3", "id" -> 4L) shouldBe Some(true)
  }

  test("NOT, AND and OR obey precedence and parentheses") {
    val text = "not id == 1 and active == true or id in [2, 3]"
    evaluate(text, "id" -> 1L, "active" -> true) shouldBe Some(false)
    evaluate(text, "id" -> 2L, "active" -> false) shouldBe Some(true)
    evaluate(text, "id" -> 4L, "active" -> true) shouldBe Some(true)
    evaluate(
      "!(id == 1 || active == false) && category == 'books'",
      "id" -> 4L,
      "active" -> true,
      "category" -> "books"
    ) shouldBe Some(true)
    PlanParser.parse(text).fields shouldBe Set("id", "active")
  }

  test("all AND and OR combinations preserve three-valued null semantics") {
    val values = Vector(Some(true), Some(false), None)
    val conjunction = Vector(
      Vector(Some(true), Some(false), None),
      Vector(Some(false), Some(false), Some(false)),
      Vector(None, Some(false), None)
    )
    val disjunction = Vector(
      Vector(Some(true), Some(true), Some(true)),
      Vector(Some(true), Some(false), None),
      Vector(Some(true), None, None)
    )
    for (left <- values.indices; right <- values.indices) {
      val row = Map[String, Any](
        "left" -> values(left).fold[Any](null)(identity),
        "right" -> values(right).fold[Any](null)(identity)
      )
      val a = Expr.Compare("left", "==", true)
      val b = Expr.Compare("right", "==", true)
      Expr.And(a, b).evaluate(row) shouldBe conjunction(left)(right)
      Expr.Or(a, b).evaluate(row) shouldBe disjunction(left)(right)
      Expr.And(a, b).matches(row) shouldBe conjunction(left)(right).contains(
        true
      )
    }
    Expr.Not(Expr.Compare("left", "==", true)).evaluate(_ => null) shouldBe None
  }

  test("IN, NOT IN and IS NULL handle nullable rows before admission") {
    evaluate(
      "category IN ['books', 'games']",
      "category" -> "books"
    ) shouldBe Some(true)
    evaluate(
      "category not in ['books', 'games']",
      "category" -> "music"
    ) shouldBe Some(true)
    evaluate("category not in ['books']", "category" -> null) shouldBe None
    evaluate("category in []", "category" -> "books") shouldBe Some(false)
    evaluate("category in []", "category" -> null) shouldBe None
    evaluate("category IS NULL", "category" -> null) shouldBe Some(true)
    evaluate("category IS NOT NULL", "category" -> "") shouldBe Some(true)
    evaluate("category == 'null'", "category" -> "null") shouldBe Some(true)
    evaluate(
      "category IS NULL or category == 'books'",
      "category" -> null
    ) shouldBe Some(true)
    // The IR also preserves an unknown supplied by another expression producer.
    Expr
      .In("category", Vector("books", null))
      .evaluate(_ => "music") shouldBe None
  }

  test(
    "Float32 literals round through double exactly as Milvus scalar evaluation"
  ) {
    evaluate("rating == 1.00000011", "rating" -> 1.0000001f) shouldBe Some(true)
    evaluate("rating < 1.00000011", "rating" -> 1.0000001f) shouldBe Some(false)
    evaluate("rating in [1.00000011]", "rating" -> 1.0000001f) shouldBe Some(
      true
    )
    evaluate("rating == 16777217", "rating" -> 16777216f) shouldBe Some(true)
    evaluate("score == 1.00000011", "score" -> 1.0000001d) shouldBe Some(false)
    evaluate("score >= -2e1 and score < .5", "score" -> -20d) shouldBe Some(
      true
    )
  }

  test("floating comparisons preserve IEEE NaN and signed zero behavior") {
    evaluate("rating == 0", "rating" -> -0.0f) shouldBe Some(true)
    evaluate("rating == 1", "rating" -> Float.NaN) shouldBe Some(false)
    evaluate("rating != 1", "rating" -> Float.NaN) shouldBe Some(true)
    evaluate("rating >= 1", "rating" -> Float.NaN) shouldBe Some(false)
    evaluate("score > 1", "score" -> Double.PositiveInfinity) shouldBe Some(
      true
    )
    evaluate("score < 1", "score" -> Double.NegativeInfinity) shouldBe Some(
      true
    )
  }

  test("string quoting and supported escapes do not become parser operators") {
    evaluate("category == '书籍'", "category" -> "书籍") shouldBe Some(true)
    evaluate("category == 'and'", "category" -> "and") shouldBe Some(true)
    evaluate(
      """category == "a\nb\t\"c\\d"""",
      "category" -> "a\nb\t\"c\\d"
    ) shouldBe Some(true)
    evaluate("""category == 'a\'b'""", "category" -> "a'b") shouldBe Some(true)
  }

  test("string comparisons use Milvus UTF-8 byte ordering") {
    val supplementary = new String(Character.toChars(0x10000))
    val bmp = "\uE000"
    evaluate(s"category > '$bmp'", "category" -> supplementary) shouldBe Some(
      true
    )
    evaluate(s"category < '$supplementary'", "category" -> bmp) shouldBe Some(
      true
    )
    evaluate(
      s"category in ['$supplementary']",
      "category" -> supplementary
    ) shouldBe Some(true)
  }

  test(
    "planning rejects unknown fields, vectors and incompatible literal types"
  ) {
    Seq(
      "missing == 1",
      "embedding IS NULL",
      "id == '1'",
      "id == 1.0",
      "id in [1, 2.0]",
      "category == 1",
      "active == 1",
      "active > false"
    ).foreach { text =>
      intercept[IllegalArgumentException](
        Evaluator.validate(PlanParser.parse(text), fields)
      )
    }
  }

  test("unsupported or incomplete syntax fails during parsing") {
    Seq(
      "",
      " ",
      "id",
      "id ==",
      "id = 1",
      "id <> 1",
      "id == 1 trailing",
      "id == 1 and",
      "id in [1,]",
      "id in (1,2)",
      "id not == 1",
      "id == null",
      "id in [null]",
      "category is not true",
      "id + 1 > 2",
      "1 < id < 4",
      "text_match(category, 'books')",
      "category like 'book%'",
      "category[0] == 'x'",
      "category == 'unterminated",
      "category == '\\q'",
      "id \"<=\" 3",
      "id '==' 3",
      "category == 'a\nb'",
      "category == 'a\rb'"
    ).foreach { text =>
      withClue(text + ": ") {
        intercept[IllegalArgumentException](PlanParser.parse(text))
      }
    }
  }

  test("numeric overflow and unsupported integer bases fail during parsing") {
    Seq(
      "9223372036854775808",
      "-9223372036854775809",
      "1e309",
      "0x10",
      "010",
      "NaN"
    ).foreach { literal =>
      intercept[IllegalArgumentException](PlanParser.parse(s"id == $literal"))
    }
  }

  test(
    "nested and flat expression limits prevent recursive evaluator overflow"
  ) {
    intercept[IllegalArgumentException](
      PlanParser.parse("(" * 65 + "id == 1" + ")" * 65)
    )
    intercept[IllegalArgumentException](
      PlanParser.parse("not " * 65 + "id == 1")
    )
    val accepted =
      PlanParser.parse(Vector.fill(64)("id == 1").mkString(" and "))
    accepted.fields shouldBe Set("id")
    accepted.matches(_ => 1L) shouldBe true
    Seq("and", "or").foreach { operator =>
      intercept[IllegalArgumentException](
        PlanParser.parse(Vector.fill(65)("id == 1").mkString(s" $operator "))
      )
    }
    intercept[IllegalArgumentException](
      PlanParser.parse("category == '" + "x" * 65536 + "'")
    )
    intercept[IllegalArgumentException](
      PlanParser.parse("id in [" + Vector.fill(2100)("1").mkString(",") + "]")
    )
    intercept[IllegalArgumentException](PlanParser.parse(null))
  }
}
