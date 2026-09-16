package com.zilliz.milvus.storage.expr

import io.milvus.grpc.schema.{DataType => MilvusDataType}

/** A serializable, Spark-independent predicate evaluated against an Arrow
  * batch.
  *
  * Every leaf compares one Milvus field with literals. Column-to-column and
  * computed expressions remain Spark residual predicates.
  */
sealed trait PredicateExpr extends Product with Serializable

/** Identifies a field without depending on the physical column name used by a
  * particular segment layout.
  */
final case class FieldRef(fieldId: Long, dataType: MilvusDataType)
    extends Serializable

sealed trait Literal extends Product with Serializable

object Literal {
  case object NullValue extends Literal
  final case class BooleanValue(value: Boolean) extends Literal
  final case class IntegerValue(value: Long) extends Literal
  final case class FloatValue(value: Float) extends Literal
  final case class DoubleValue(value: Double) extends Literal
  final case class StringValue(value: String) extends Literal
}

sealed trait ComparisonOperator extends Product with Serializable

object ComparisonOperator {
  case object EqualTo extends ComparisonOperator
  case object NotEqualTo extends ComparisonOperator
  case object EqualNullSafe extends ComparisonOperator
  case object LessThan extends ComparisonOperator
  case object LessThanOrEqual extends ComparisonOperator
  case object GreaterThan extends ComparisonOperator
  case object GreaterThanOrEqual extends ComparisonOperator
}

final case class Comparison(
    field: FieldRef,
    operator: ComparisonOperator,
    literal: Literal
) extends PredicateExpr

final case class In(field: FieldRef, literals: Vector[Literal])
    extends PredicateExpr

final case class IsNull(field: FieldRef) extends PredicateExpr

final case class IsNotNull(field: FieldRef) extends PredicateExpr

final case class StartsWith(field: FieldRef, prefix: String)
    extends PredicateExpr

final case class EndsWith(field: FieldRef, suffix: String) extends PredicateExpr

final case class And(left: PredicateExpr, right: PredicateExpr)
    extends PredicateExpr

final case class Or(left: PredicateExpr, right: PredicateExpr)
    extends PredicateExpr

final case class Not(child: PredicateExpr) extends PredicateExpr
