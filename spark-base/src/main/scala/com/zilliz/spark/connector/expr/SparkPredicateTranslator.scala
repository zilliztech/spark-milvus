package com.zilliz.spark.connector.expr

import scala.util.control.NonFatal

import org.apache.spark.sql.connector.expressions.{
  Expression => SparkExpression,
  Literal => SparkLiteral,
  NamedReference
}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.types.{
  BooleanType => SparkBooleanType,
  ByteType => SparkByteType,
  DataType => SparkDataType,
  DoubleType => SparkDoubleType,
  FloatType => SparkFloatType,
  IntegerType => SparkIntegerType,
  LongType => SparkLongType,
  NullType => SparkNullType,
  ShortType => SparkShortType,
  StringType => SparkStringType,
  StructField,
  StructType
}
import org.apache.spark.unsafe.types.UTF8String

import com.zilliz.milvus.storage.expr.{
  And,
  Comparison,
  ComparisonOperator,
  EndsWith,
  FieldRef,
  In,
  IsNotNull,
  IsNull,
  Literal,
  Not,
  Or,
  PredicateExpr,
  StartsWith
}
import com.zilliz.milvus.storage.expr.Literal.{
  BooleanValue,
  DoubleValue,
  FloatValue,
  IntegerValue,
  NullValue,
  StringValue
}
import com.zilliz.milvus.storage.schema.FieldMetadata
import io.milvus.grpc.schema.{DataType => MilvusDataType}

/** Exact translation from Spark's DataSource V2 predicate tree to the
  * Spark-free expression tree evaluated by core.
  *
  * Translation is deliberately all-or-nothing for each predicate Spark hands to
  * the scan builder. Returning `None` leaves that original predicate above the
  * scan. In particular, an unsupported child never turns an `OR` or `NOT` into
  * an approximation that could discard a valid row.
  */
object SparkPredicateTranslator {

  final case class Translated(expr: PredicateExpr, fieldIds: Set[Long])

  def translate(
      predicate: Predicate,
      schema: StructType
  ): Option[Translated] =
    if (predicate == null || schema == null) None
    else
      try translatePredicate(predicate, schema)
      catch {
        // V2 expression implementations are public extension points. A node
        // with a null child array, malformed metadata or another invalid
        // shape is not safe to accept, but it remains valid for Spark to
        // evaluate above the scan.
        case NonFatal(_) => None
      }

  private def translatePredicate(
      predicate: Predicate,
      schema: StructType
  ): Option[Translated] = {
    val children = predicate.children()
    predicate.name() match {
      case "=" =>
        comparison(children, schema, ComparisonOperator.EqualTo)
      case "<>" =>
        comparison(children, schema, ComparisonOperator.NotEqualTo)
      case "<=>" =>
        comparison(children, schema, ComparisonOperator.EqualNullSafe)
      case "<" =>
        comparison(children, schema, ComparisonOperator.LessThan)
      case "<=" =>
        comparison(children, schema, ComparisonOperator.LessThanOrEqual)
      case ">" =>
        comparison(children, schema, ComparisonOperator.GreaterThan)
      case ">=" =>
        comparison(children, schema, ComparisonOperator.GreaterThanOrEqual)

      case "IN" =>
        children.headOption.flatMap(field(_, schema)).flatMap { resolved =>
          val values = children.drop(1).map(literal(_, resolved))
          if (values.forall(_.isDefined)) {
            Some(
              Translated(
                In(resolved.ref, values.iterator.map(_.get).toVector),
                Set(resolved.ref.fieldId)
              )
            )
          } else None
        }

      case "IS_NULL" =>
        unaryField(children, schema)(ref => IsNull(ref))
      case "IS_NOT_NULL" =>
        unaryField(children, schema)(ref => IsNotNull(ref))

      case "STARTS_WITH" =>
        stringPredicate(children, schema)(StartsWith.apply)
      case "ENDS_WITH" =>
        stringPredicate(children, schema)(EndsWith.apply)

      case "AND" => booleanBinary(children, schema)(And.apply)
      case "OR"  => booleanBinary(children, schema)(Or.apply)
      case "NOT" =>
        if (children.length != 1) None
        else
          children(0) match {
            case child: Predicate =>
              translatePredicate(child, schema).map(t =>
                t.copy(expr = Not(t.expr))
              )
            case _ => None
          }

      // Spark can describe considerably more than this pushdown contract,
      // including CONTAINS, arithmetic, casts and arbitrary scalar functions.
      // They remain in Spark until core has an exact implementation of their
      // SQL semantics.
      case _ => None
    }
  }

  private def comparison(
      children: Array[SparkExpression],
      schema: StructType,
      op: ComparisonOperator
  ): Option[Translated] =
    if (children.length != 2) None
    else
      for {
        resolved <- field(children(0), schema)
        if comparisonSupported(resolved.ref.dataType, op)
        value <- literal(children(1), resolved)
      } yield Translated(
        Comparison(resolved.ref, op, value),
        Set(resolved.ref.fieldId)
      )

  private def unaryField(
      children: Array[SparkExpression],
      schema: StructType
  )(make: FieldRef => PredicateExpr): Option[Translated] =
    if (children.length != 1) None
    else
      field(children(0), schema).map(resolved =>
        Translated(make(resolved.ref), Set(resolved.ref.fieldId))
      )

  private def stringPredicate(
      children: Array[SparkExpression],
      schema: StructType
  )(make: (FieldRef, String) => PredicateExpr): Option[Translated] =
    if (children.length != 2) None
    else
      for {
        resolved <- field(children(0), schema)
        if isString(resolved.ref.dataType)
        value <- stringLiteral(children(1), resolved.sparkType)
      } yield Translated(make(resolved.ref, value), Set(resolved.ref.fieldId))

  private def booleanBinary(
      children: Array[SparkExpression],
      schema: StructType
  )(make: (PredicateExpr, PredicateExpr) => PredicateExpr): Option[Translated] =
    if (children.length != 2) None
    else
      (children(0), children(1)) match {
        case (left: Predicate, right: Predicate) =>
          for {
            l <- translatePredicate(left, schema)
            r <- translatePredicate(right, schema)
          } yield Translated(make(l.expr, r.expr), l.fieldIds ++ r.fieldIds)
        case _ => None
      }

  private final case class ResolvedField(
      ref: FieldRef,
      sparkType: SparkDataType
  )

  private def field(
      expression: SparkExpression,
      schema: StructType
  ): Option[ResolvedField] = expression match {
    case reference: NamedReference if reference.fieldNames().length == 1 =>
      val name = reference.fieldNames()(0)
      schema.fields.find(_.name == name).flatMap(resolveField)
    case _ => None
  }

  private def resolveField(field: StructField): Option[ResolvedField] = try {
    val metadata = field.metadata
    if (
      !metadata.contains(FieldMetadata.MilvusFieldIdMetadataKey) ||
      !metadata.contains(FieldMetadata.MilvusDataTypeMetadataKey)
    ) return None

    val fieldId = metadata.getLong(FieldMetadata.MilvusFieldIdMetadataKey)
    if (fieldId < 0L) return None

    val logicalTypeValue = metadata.getLong(
      FieldMetadata.MilvusDataTypeMetadataKey
    )
    if (
      logicalTypeValue < Int.MinValue.toLong ||
      logicalTypeValue > Int.MaxValue.toLong
    ) return None

    val logicalType = MilvusDataType.fromValue(logicalTypeValue.toInt)
    if (logicalType.isUnrecognized) return None

    sparkTypeFor(logicalType).flatMap { sparkType =>
      if (sparkType == field.dataType) {
        Some(ResolvedField(FieldRef(fieldId, logicalType), field.dataType))
      } else None
    }
  } catch {
    // Metadata belongs to a fixed snapshot, but old or externally assembled
    // schemas can still carry a value under the right key with the wrong
    // metadata type. A translator never guesses through malformed metadata.
    case NonFatal(_) => None
  }

  private def comparisonSupported(
      dataType: MilvusDataType,
      op: ComparisonOperator
  ): Boolean = dataType match {
    case MilvusDataType.Bool =>
      op == ComparisonOperator.EqualTo ||
      op == ComparisonOperator.NotEqualTo ||
      op == ComparisonOperator.EqualNullSafe
    case MilvusDataType.Int8 | MilvusDataType.Int16 | MilvusDataType.Int32 |
        MilvusDataType.Int64 | MilvusDataType.Float | MilvusDataType.Double |
        MilvusDataType.String | MilvusDataType.VarChar | MilvusDataType.Text =>
      true
    case _ => false
  }

  private def sparkTypeFor(dataType: MilvusDataType): Option[SparkDataType] =
    dataType match {
      case MilvusDataType.Bool   => Some(SparkBooleanType)
      case MilvusDataType.Int8   => Some(SparkByteType)
      case MilvusDataType.Int16  => Some(SparkShortType)
      case MilvusDataType.Int32  => Some(SparkIntegerType)
      case MilvusDataType.Int64  => Some(SparkLongType)
      case MilvusDataType.Float  => Some(SparkFloatType)
      case MilvusDataType.Double => Some(SparkDoubleType)
      case t if isString(t)      => Some(SparkStringType)
      case _                     => None
    }

  private def isString(dataType: MilvusDataType): Boolean =
    dataType == MilvusDataType.String ||
      dataType == MilvusDataType.VarChar ||
      dataType == MilvusDataType.Text

  private def literal(
      expression: SparkExpression,
      field: ResolvedField
  ): Option[Literal] = expression match {
    case value: SparkLiteral[_] =>
      if (value.value() == null) {
        if (
          value.dataType() == field.sparkType ||
          value.dataType() == SparkNullType
        ) Some(NullValue)
        else None
      } else if (value.dataType() != field.sparkType) {
        None
      } else {
        (field.ref.dataType, value.value()) match {
          case (MilvusDataType.Bool, v: java.lang.Boolean) =>
            Some(BooleanValue(v.booleanValue()))
          case (MilvusDataType.Int8, v: java.lang.Byte) =>
            Some(IntegerValue(v.longValue()))
          case (MilvusDataType.Int16, v: java.lang.Short) =>
            Some(IntegerValue(v.longValue()))
          case (MilvusDataType.Int32, v: java.lang.Integer) =>
            Some(IntegerValue(v.longValue()))
          case (MilvusDataType.Int64, v: java.lang.Long) =>
            Some(IntegerValue(v.longValue()))
          case (MilvusDataType.Float, v: java.lang.Float) =>
            Some(FloatValue(v.floatValue()))
          case (MilvusDataType.Double, v: java.lang.Double) =>
            Some(DoubleValue(v.doubleValue()))
          case (t, v: UTF8String) if isString(t) =>
            Some(StringValue(v.toString))
          case (t, v: String) if isString(t) => Some(StringValue(v))
          case _                             => None
        }
      }
    case _ => None
  }

  private def stringLiteral(
      expression: SparkExpression,
      expectedType: SparkDataType
  ): Option[String] = expression match {
    case value: SparkLiteral[_]
        if value.value() != null && value.dataType() == expectedType =>
      value.value() match {
        case v: UTF8String => Some(v.toString)
        case v: String     => Some(v)
        case _             => None
      }
    case _ => None
  }
}
