package com.zilliz.milvus.storage.expr

import java.nio.charset.StandardCharsets
import scala.collection.mutable

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

import com.zilliz.milvus.storage.expr.ComparisonOperator._
import com.zilliz.milvus.storage.expr.Literal._
import io.milvus.grpc.schema.{DataType => MilvusDataType}

/** Evaluates [[PredicateExpr]] with SQL three-valued logic.
  *
  * Only TRUE rows survive a WHERE clause. FALSE and UNKNOWN therefore both set
  * their bit in the returned exclusion bitmap.
  */
object PredicateEvaluator {

  def evaluate(
      expression: PredicateExpr,
      batch: VectorSchemaRoot,
      columnNameFor: Long => Option[String]
  ): Bitmap = {
    require(expression != null, "expression must not be null")
    require(batch != null, "batch must not be null")
    require(columnNameFor != null, "columnNameFor must not be null")

    val context = prepare(expression, batch, columnNameFor)
    val rowCount = batch.getRowCount
    val words =
      new Array[Long](if (rowCount == 0) 0 else ((rowCount - 1) >>> 6) + 1)
    var row = 0
    while (row < rowCount) {
      if (truth(expression, row, context) ne TrueValue) {
        words(row >>> 6) |= 1L << (row & 63)
      }
      row += 1
    }
    Bitmap.fromWords(rowCount, words)
  }

  private sealed trait TruthValue
  private case object TrueValue extends TruthValue
  private case object FalseValue extends TruthValue
  private case object UnknownValue extends TruthValue

  private final case class ResolvedField(
      ref: FieldRef,
      columnName: String,
      vector: FieldVector
  )

  private final case class Context(
      fields: Map[Long, ResolvedField],
      utf8: Map[String, Array[Byte]]
  )

  private def prepare(
      expression: PredicateExpr,
      batch: VectorSchemaRoot,
      columnNameFor: Long => Option[String]
  ): Context = {
    val refs = mutable.LinkedHashMap.empty[Long, FieldRef]
    val strings = mutable.LinkedHashSet.empty[String]

    def addField(ref: FieldRef): Unit = {
      if (ref == null) {
        throw new IllegalArgumentException("expression contains a null field")
      }
      refs.get(ref.fieldId) match {
        case Some(existing) if existing.dataType != ref.dataType =>
          throw new IllegalArgumentException(
            s"field ${ref.fieldId} has conflicting expression types " +
              s"${existing.dataType} and ${ref.dataType}"
          )
        case _ => refs.update(ref.fieldId, ref)
      }
    }

    def addLiteral(field: FieldRef, literal: Literal): Unit = {
      if (literal == null) {
        throw new IllegalArgumentException(
          s"field ${field.fieldId} has a null Literal reference"
        )
      }
      validateLiteral(field, literal)
      literal match {
        case StringValue(value) =>
          requireString(value, s"literal for field ${field.fieldId}")
          strings += value
        case _ =>
      }
    }

    def visit(expr: PredicateExpr): Unit = {
      if (expr == null) {
        throw new IllegalArgumentException("expression contains a null child")
      }
      expr match {
        case Comparison(field, operator, literal) =>
          addField(field)
          validateOperator(field, operator)
          addLiteral(field, literal)
        case In(field, literals) =>
          addField(field)
          if (literals == null) {
            throw new IllegalArgumentException(
              s"IN values for field ${field.fieldId} must not be null"
            )
          }
          literals.foreach(addLiteral(field, _))
        case IsNull(field)    => addField(field)
        case IsNotNull(field) => addField(field)
        case StartsWith(field, prefix) =>
          addField(field)
          requireStringField(field, "startsWith")
          requireString(prefix, s"prefix for field ${field.fieldId}")
          strings += prefix
        case EndsWith(field, suffix) =>
          addField(field)
          requireStringField(field, "endsWith")
          requireString(suffix, s"suffix for field ${field.fieldId}")
          strings += suffix
        case And(left, right) =>
          visit(left)
          visit(right)
        case Or(left, right) =>
          visit(left)
          visit(right)
        case Not(child) => visit(child)
      }
    }

    visit(expression)

    val resolved = refs.iterator.map { case (fieldId, ref) =>
      val columnName = columnNameFor(fieldId).getOrElse {
        throw new IllegalStateException(
          s"no physical column name for predicate field $fieldId"
        )
      }
      if (columnName == null) {
        throw new IllegalStateException(
          s"physical column name for predicate field $fieldId is null"
        )
      }
      val vector = batch.getVector(columnName)
      if (vector == null) {
        throw new IllegalStateException(
          s"predicate field $fieldId maps to column '$columnName', but the " +
            s"batch has only ${batch.getSchema.getFields}"
        )
      }
      validateVector(ref, columnName, vector, batch.getRowCount)
      fieldId -> ResolvedField(ref, columnName, vector)
    }.toMap

    Context(
      resolved,
      strings.iterator
        .map(value => value -> value.getBytes(StandardCharsets.UTF_8))
        .toMap
    )
  }

  private def truth(
      expression: PredicateExpr,
      row: Int,
      context: Context
  ): TruthValue =
    expression match {
      case Comparison(field, operator, literal) =>
        comparison(
          context.fields(field.fieldId),
          row,
          operator,
          literal,
          context
        )
      case In(field, literals) =>
        in(context.fields(field.fieldId), row, literals, context)
      case IsNull(field) =>
        boolean(context.fields(field.fieldId).vector.isNull(row))
      case IsNotNull(field) =>
        boolean(!context.fields(field.fieldId).vector.isNull(row))
      case StartsWith(field, prefix) =>
        stringPredicate(context.fields(field.fieldId), row) { bytes =>
          startsWith(bytes, context.utf8(prefix))
        }
      case EndsWith(field, suffix) =>
        stringPredicate(context.fields(field.fieldId), row) { bytes =>
          endsWith(bytes, context.utf8(suffix))
        }
      case And(left, right) =>
        truth(left, row, context) match {
          case FalseValue => FalseValue
          case TrueValue  => truth(right, row, context)
          case UnknownValue =>
            truth(right, row, context) match {
              case FalseValue => FalseValue
              case _          => UnknownValue
            }
        }
      case Or(left, right) =>
        truth(left, row, context) match {
          case TrueValue  => TrueValue
          case FalseValue => truth(right, row, context)
          case UnknownValue =>
            truth(right, row, context) match {
              case TrueValue => TrueValue
              case _         => UnknownValue
            }
        }
      case Not(child) =>
        truth(child, row, context) match {
          case TrueValue    => FalseValue
          case FalseValue   => TrueValue
          case UnknownValue => UnknownValue
        }
    }

  private def comparison(
      field: ResolvedField,
      row: Int,
      operator: ComparisonOperator,
      literal: Literal,
      context: Context
  ): TruthValue = {
    val fieldIsNull = field.vector.isNull(row)
    val literalIsNull = literal == NullValue

    if (operator == EqualNullSafe) {
      if (fieldIsNull || literalIsNull)
        return boolean(fieldIsNull && literalIsNull)
    } else if (fieldIsNull || literalIsNull) {
      return UnknownValue
    }

    val order = compareNonNull(field, row, literal, context)
    boolean(
      operator match {
        case EqualTo            => order == 0
        case NotEqualTo         => order != 0
        case EqualNullSafe      => order == 0
        case LessThan           => order < 0
        case LessThanOrEqual    => order <= 0
        case GreaterThan        => order > 0
        case GreaterThanOrEqual => order >= 0
      }
    )
  }

  private def in(
      field: ResolvedField,
      row: Int,
      literals: Vector[Literal],
      context: Context
  ): TruthValue = {
    if (literals.isEmpty) return FalseValue
    if (field.vector.isNull(row)) return UnknownValue

    var sawNull = false
    var i = 0
    while (i < literals.length) {
      val literal = literals(i)
      if (literal == NullValue) sawNull = true
      else if (compareNonNull(field, row, literal, context) == 0)
        return TrueValue
      i += 1
    }
    if (sawNull) UnknownValue else FalseValue
  }

  private def compareNonNull(
      field: ResolvedField,
      row: Int,
      literal: Literal,
      context: Context
  ): Int =
    field.ref.dataType match {
      case MilvusDataType.Bool =>
        java.lang.Boolean.compare(
          field.vector.asInstanceOf[BitVector].get(row) != 0,
          literal.asInstanceOf[BooleanValue].value
        )
      case MilvusDataType.Int8 =>
        java.lang.Long.compare(
          field.vector.asInstanceOf[TinyIntVector].get(row).toLong,
          literal.asInstanceOf[IntegerValue].value
        )
      case MilvusDataType.Int16 =>
        java.lang.Long.compare(
          field.vector.asInstanceOf[SmallIntVector].get(row).toLong,
          literal.asInstanceOf[IntegerValue].value
        )
      case MilvusDataType.Int32 =>
        java.lang.Long.compare(
          field.vector.asInstanceOf[IntVector].get(row).toLong,
          literal.asInstanceOf[IntegerValue].value
        )
      case MilvusDataType.Int64 =>
        java.lang.Long.compare(
          field.vector.asInstanceOf[BigIntVector].get(row),
          literal.asInstanceOf[IntegerValue].value
        )
      case MilvusDataType.Float =>
        compareFloat(
          field.vector.asInstanceOf[Float4Vector].get(row),
          literal.asInstanceOf[FloatValue].value
        )
      case MilvusDataType.Double =>
        compareDouble(
          field.vector.asInstanceOf[Float8Vector].get(row),
          literal.asInstanceOf[DoubleValue].value
        )
      case MilvusDataType.String | MilvusDataType.VarChar |
          MilvusDataType.Text =>
        compareUtf8(
          field.vector.asInstanceOf[VarCharVector].get(row),
          context.utf8(literal.asInstanceOf[StringValue].value)
        )
      case other =>
        throw new IllegalStateException(
          s"unsupported predicate type reached evaluation: $other"
        )
    }

  private def validateOperator(
      field: FieldRef,
      operator: ComparisonOperator
  ): Unit = {
    if (operator == null) {
      throw new IllegalArgumentException(
        s"comparison operator for field ${field.fieldId} must not be null"
      )
    }
    field.dataType match {
      case MilvusDataType.Bool =>
        operator match {
          case EqualTo | NotEqualTo | EqualNullSafe =>
          case _ =>
            throw new IllegalArgumentException(
              s"operator $operator is not supported for Bool field ${field.fieldId}"
            )
        }
      case t if isOrdered(t) =>
      case other             => unsupportedField(field, other)
    }
  }

  private def validateLiteral(field: FieldRef, literal: Literal): Unit = {
    if (literal == NullValue) return
    field.dataType match {
      case MilvusDataType.Bool =>
        requireLiteral[BooleanValue](field, literal, "BooleanValue")
      case MilvusDataType.Int8 =>
        val value = integerLiteral(field, literal)
        requireRange(field, value, Byte.MinValue, Byte.MaxValue)
      case MilvusDataType.Int16 =>
        val value = integerLiteral(field, literal)
        requireRange(field, value, Short.MinValue, Short.MaxValue)
      case MilvusDataType.Int32 =>
        val value = integerLiteral(field, literal)
        requireRange(field, value, Int.MinValue.toLong, Int.MaxValue.toLong)
      case MilvusDataType.Int64 =>
        integerLiteral(field, literal)
      case MilvusDataType.Float =>
        requireLiteral[FloatValue](field, literal, "FloatValue")
      case MilvusDataType.Double =>
        requireLiteral[DoubleValue](field, literal, "DoubleValue")
      case MilvusDataType.String | MilvusDataType.VarChar |
          MilvusDataType.Text =>
        requireLiteral[StringValue](field, literal, "StringValue")
      case other => unsupportedField(field, other)
    }
  }

  private def requireLiteral[T <: Literal: Manifest](
      field: FieldRef,
      literal: Literal,
      expected: String
  ): Unit =
    if (!manifest[T].runtimeClass.isInstance(literal)) {
      throw new IllegalArgumentException(
        s"field ${field.fieldId} of type ${field.dataType} requires $expected, " +
          s"not ${literal.getClass.getSimpleName}"
      )
    }

  private def integerLiteral(field: FieldRef, literal: Literal): Long =
    literal match {
      case IntegerValue(value) => value
      case _ =>
        throw new IllegalArgumentException(
          s"field ${field.fieldId} of type ${field.dataType} requires " +
            s"IntegerValue, not ${literal.getClass.getSimpleName}"
        )
    }

  private def requireRange(
      field: FieldRef,
      value: Long,
      minimum: Long,
      maximum: Long
  ): Unit =
    if (value < minimum || value > maximum) {
      throw new IllegalArgumentException(
        s"literal $value is outside ${field.dataType} range for field ${field.fieldId}"
      )
    }

  private def validateVector(
      ref: FieldRef,
      columnName: String,
      vector: FieldVector,
      rowCount: Int
  ): Unit = {
    val valid = ref.dataType match {
      case MilvusDataType.Bool   => vector.isInstanceOf[BitVector]
      case MilvusDataType.Int8   => vector.isInstanceOf[TinyIntVector]
      case MilvusDataType.Int16  => vector.isInstanceOf[SmallIntVector]
      case MilvusDataType.Int32  => vector.isInstanceOf[IntVector]
      case MilvusDataType.Int64  => vector.isInstanceOf[BigIntVector]
      case MilvusDataType.Float  => vector.isInstanceOf[Float4Vector]
      case MilvusDataType.Double => vector.isInstanceOf[Float8Vector]
      case MilvusDataType.String | MilvusDataType.VarChar |
          MilvusDataType.Text =>
        vector.isInstanceOf[VarCharVector]
      case other =>
        unsupportedField(ref, other)
    }
    if (!valid) {
      throw new IllegalStateException(
        s"predicate field ${ref.fieldId} (${ref.dataType}) maps to column " +
          s"'$columnName' backed by ${vector.getClass.getSimpleName}"
      )
    }
    if (vector.getValueCount < rowCount) {
      throw new IllegalStateException(
        s"predicate column '$columnName' for field ${ref.fieldId} has " +
          s"${vector.getValueCount} values, fewer than the batch's $rowCount rows"
      )
    }
  }

  private def requireStringField(field: FieldRef, operation: String): Unit =
    if (!isString(field.dataType)) {
      throw new IllegalArgumentException(
        s"$operation requires a string field, but field ${field.fieldId} is " +
          field.dataType
      )
    }

  private def requireString(value: String, description: String): Unit =
    if (value == null) {
      throw new IllegalArgumentException(s"$description must not be null")
    }

  private def unsupportedField(field: FieldRef, dataType: MilvusDataType) =
    throw new IllegalArgumentException(
      s"predicate field ${field.fieldId} has unsupported type $dataType"
    )

  private def isOrdered(dataType: MilvusDataType): Boolean =
    dataType match {
      case MilvusDataType.Int8 | MilvusDataType.Int16 | MilvusDataType.Int32 |
          MilvusDataType.Int64 | MilvusDataType.Float | MilvusDataType.Double |
          MilvusDataType.String | MilvusDataType.VarChar |
          MilvusDataType.Text =>
        true
      case _ => false
    }

  private def isString(dataType: MilvusDataType): Boolean =
    dataType match {
      case MilvusDataType.String | MilvusDataType.VarChar |
          MilvusDataType.Text =>
        true
      case _ => false
    }

  private def stringPredicate(
      field: ResolvedField,
      row: Int
  )(predicate: Array[Byte] => Boolean): TruthValue =
    if (field.vector.isNull(row)) UnknownValue
    else {
      val value = field.vector.asInstanceOf[VarCharVector].get(row)
      boolean(predicate(value))
    }

  private def compareFloat(left: Float, right: Float): Int = {
    val leftNaN = java.lang.Float.isNaN(left)
    val rightNaN = java.lang.Float.isNaN(right)
    if (leftNaN && rightNaN) 0
    else if (leftNaN) 1
    else if (rightNaN) -1
    else if (left == right) 0
    else if (left < right) -1
    else 1
  }

  private def compareDouble(left: Double, right: Double): Int = {
    val leftNaN = java.lang.Double.isNaN(left)
    val rightNaN = java.lang.Double.isNaN(right)
    if (leftNaN && rightNaN) 0
    else if (leftNaN) 1
    else if (rightNaN) -1
    else if (left == right) 0
    else if (left < right) -1
    else 1
  }

  /** Spark orders UTF-8 strings by unsigned encoded bytes, not by UTF-16 code
    * units as `String.compareTo` does.
    */
  private def compareUtf8(left: Array[Byte], right: Array[Byte]): Int = {
    val common = math.min(left.length, right.length)
    var i = 0
    while (i < common) {
      val difference = (left(i) & 0xff) - (right(i) & 0xff)
      if (difference != 0) return difference
      i += 1
    }
    left.length - right.length
  }

  private def startsWith(value: Array[Byte], prefix: Array[Byte]): Boolean = {
    if (prefix.length > value.length) return false
    var i = 0
    while (i < prefix.length) {
      if (value(i) != prefix(i)) return false
      i += 1
    }
    true
  }

  private def endsWith(value: Array[Byte], suffix: Array[Byte]): Boolean = {
    if (suffix.length > value.length) return false
    val offset = value.length - suffix.length
    var i = 0
    while (i < suffix.length) {
      if (value(offset + i) != suffix(i)) return false
      i += 1
    }
    true
  }

  private def boolean(value: Boolean): TruthValue =
    if (value) TrueValue else FalseValue
}
