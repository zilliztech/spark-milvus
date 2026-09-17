package com.zilliz.milvus.storage.expr

import java.nio.charset.StandardCharsets.UTF_8

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
import org.apache.arrow.vector.util.Text

import io.milvus.grpc.schema.{DataType, FieldSchema}

/** Validates and evaluates the scalar subset parsed by [[PlanParser]].
  *
  * Its literal coercion and floating-point behavior follow Milvus Plan.g4.
  * DataSource V2 predicates use [[PredicateExpr]] and [[PredicateEvaluator]],
  * whose contract follows Spark SQL instead.
  */
object Evaluator {
  def validate(expr: Expr, fields: Seq[FieldSchema]): Unit = {
    val byName = fields.map(f => f.name -> f).toMap
    val numeric = Set[DataType](
      DataType.Int8,
      DataType.Int16,
      DataType.Int32,
      DataType.Int64,
      DataType.Float,
      DataType.Double
    )
    val floating = Set[DataType](DataType.Float, DataType.Double)
    expr.fields.foreach { name =>
      val field = byName.getOrElse(
        name,
        throw new IllegalArgumentException(s"Unknown filter field $name")
      )
      require(
        numeric(field.dataType) || Set[DataType](
          DataType.Bool,
          DataType.VarChar,
          DataType.String,
          DataType.Text
        )(field.dataType),
        s"Unsupported filter field type ${field.dataType}: $name"
      )
    }
    def literal(name: String, value: Any): Unit = {
      val t = byName(name).dataType
      require(
        value == null || (value match {
          case number: BigDecimal =>
            floating(t) || (numeric(t) && number.isWhole && number.isValidLong)
          case _: Byte | _: Short | _: Int | _: Long => numeric(t)
          case _: Float | _: Double                  => floating(t)
          case _: Boolean                            => t == DataType.Bool
          case _: String =>
            t == DataType.VarChar || t == DataType.String ||
            t == DataType.Text
          case _ => false
        }),
        s"Filter literal does not match $name ($t)"
      )
    }
    def visit(e: Expr): Unit = e match {
      case Expr.Compare(f, op, value) =>
        literal(f, value)
        require(
          byName(f).dataType != DataType.Bool || Set("==", "!=")(op),
          "Boolean filters support equality comparisons only"
        )
      case Expr.In(f, values) => values.foreach(literal(f, _))
      case Expr.IsNull(_)     =>
      case Expr.Not(e)        => visit(e)
      case Expr.And(l, r)     => visit(l); visit(r)
      case Expr.Or(l, r)      => visit(l); visit(r)
    }
    visit(expr)
  }

  /** Evaluates one Arrow batch. A set bit excludes the corresponding row. */
  def evaluate(
      expr: Expr,
      batch: VectorSchemaRoot,
      column: String => String
  ): Bitmap = {
    require(expr != null, "filter expression must not be null")
    require(batch != null, "filter batch must not be null")
    require(column != null, "filter column binding must not be null")

    val vectors = prepare(expr, batch, column)
    val rowCount = batch.getRowCount
    val words =
      new Array[Long](if (rowCount == 0) 0 else ((rowCount - 1) >>> 6) + 1)
    var row = 0
    while (row < rowCount) {
      if (!expr.matches(field => value(vectors(field), row))) {
        words(row >>> 6) |= 1L << (row & 63)
      }
      row += 1
    }
    Bitmap.fromWords(rowCount, words)
  }

  def matches(
      expr: Expr,
      batch: VectorSchemaRoot,
      row: Int,
      column: String => String
  ): Boolean = expr.matches { field =>
    val vector = batch.getVector(column(field))
    require(vector != null, s"Filter column $field was not read")
    value(vector, row)
  }

  private def prepare(
      expr: Expr,
      batch: VectorSchemaRoot,
      column: String => String
  ): Map[String, FieldVector] = {
    val rowCount = batch.getRowCount
    val vectors = expr.fields.iterator.map { field =>
      val columnName = Option(column(field)).getOrElse {
        throw new IllegalStateException(
          s"Filter field $field has no physical column binding"
        )
      }
      val vector = batch.getVector(columnName)
      if (vector == null) {
        throw new IllegalStateException(
          s"Filter field $field maps to column '$columnName', but that column " +
            "was not read"
        )
      }
      validateVector(field, columnName, vector, rowCount)
      field -> vector
    }.toMap

    validateExpression(expr, vectors)
    vectors
  }

  private def validateExpression(
      expr: Expr,
      vectors: Map[String, FieldVector]
  ): Unit = expr match {
    case Expr.Compare(field, operator, literal) =>
      val vector = vectors(field)
      if (
        vector.isInstanceOf[BitVector] &&
        !Set("==", "!=").contains(operator)
      ) {
        throw new IllegalArgumentException(
          "Boolean filters support equality comparisons only"
        )
      }
      validateLiteral(field, vector, literal)
    case Expr.In(field, literals) =>
      literals.foreach(validateLiteral(field, vectors(field), _))
    case Expr.IsNull(_) =>
    case Expr.Not(child) =>
      validateExpression(child, vectors)
    case Expr.And(left, right) =>
      validateExpression(left, vectors)
      validateExpression(right, vectors)
    case Expr.Or(left, right) =>
      validateExpression(left, vectors)
      validateExpression(right, vectors)
  }

  private def validateLiteral(
      field: String,
      vector: FieldVector,
      literal: Any
  ): Unit = {
    val compatible = literal == null || (literal match {
      case number: BigDecimal =>
        isFloating(vector) ||
        (isIntegral(vector) && number.isWhole && number.isValidLong)
      case _: Byte | _: Short | _: Int | _: Long => isNumeric(vector)
      case _: Float | _: Double                  => isFloating(vector)
      case _: Boolean => vector.isInstanceOf[BitVector]
      case _: String  => vector.isInstanceOf[VarCharVector]
      case _          => false
    })
    if (!compatible) {
      throw new IllegalStateException(
        s"Filter field $field is backed by ${vector.getClass.getSimpleName}, " +
          s"which is incompatible with ${literalType(literal)}"
      )
    }
  }

  private def validateVector(
      field: String,
      columnName: String,
      vector: FieldVector,
      rowCount: Int
  ): Unit = {
    if (
      !(isNumeric(vector) || vector.isInstanceOf[BitVector] ||
        vector.isInstanceOf[VarCharVector])
    ) {
      throw new IllegalStateException(
        s"Filter field $field maps to column '$columnName' backed by " +
          s"unsupported ${vector.getClass.getSimpleName}"
      )
    }
    if (vector.getValueCount < rowCount) {
      throw new IllegalStateException(
        s"Filter column '$columnName' for field $field has " +
          s"${vector.getValueCount} values, fewer than the batch's " +
          s"$rowCount rows"
      )
    }
  }

  private def isNumeric(vector: FieldVector): Boolean =
    isIntegral(vector) || isFloating(vector)

  private def isIntegral(vector: FieldVector): Boolean =
    vector.isInstanceOf[TinyIntVector] ||
      vector.isInstanceOf[SmallIntVector] ||
      vector.isInstanceOf[IntVector] ||
      vector.isInstanceOf[BigIntVector]

  private def isFloating(vector: FieldVector): Boolean =
    vector.isInstanceOf[Float4Vector] ||
      vector.isInstanceOf[Float8Vector]

  private def literalType(literal: Any): String =
    if (literal == null) "null"
    else literal.getClass.getSimpleName

  private def value(vector: FieldVector, row: Int): Any =
    if (vector.isNull(row)) null
    else
      vector.getObject(row) match {
        case text: Text => text.toString
        case bytes: Array[Byte] =>
          new String(bytes, UTF_8)
        case value => value
      }
}
