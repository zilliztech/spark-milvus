package com.zilliz.milvus.storage.expr

import java.nio.charset.StandardCharsets.UTF_8

import org.apache.arrow.vector.util.Text
import org.apache.arrow.vector.VectorSchemaRoot

import io.milvus.grpc.schema.{DataType, FieldSchema}

/** Validates scalar types before execution and reads only fields named by the
  * expression. The same IR can be used by a future Spark predicate translator.
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
          DataType.String
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
          case _: String => t == DataType.VarChar || t == DataType.String
          case _         => false
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

  def matches(
      expr: Expr,
      batch: VectorSchemaRoot,
      row: Int,
      column: String => String
  ): Boolean = expr.matches { field =>
    val vector = batch.getVector(column(field))
    require(vector != null, s"Filter column $field was not read")
    if (vector.isNull(row)) null
    else
      vector.getObject(row) match {
        case text: Text => text.toString
        case bytes: Array[Byte] =>
          new String(bytes, UTF_8)
        case value => value
      }
  }
}
