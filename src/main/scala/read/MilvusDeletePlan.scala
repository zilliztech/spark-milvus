package com.zilliz.spark.connector.read

sealed trait MilvusDeletePlan {
  def containsLongPk(value: Long): Boolean
  def containsStringPk(value: String): Boolean
  def isEmpty: Boolean
}

object MilvusDeletePlan {
  val empty: MilvusDeletePlan = EmptyDeletePlan

  def fromLongPks(values: Set[Long]): MilvusDeletePlan =
    if (values.isEmpty) empty else LongPkDeletePlan(values)

  def fromStringPks(values: Set[String]): MilvusDeletePlan =
    if (values.isEmpty) empty else StringPkDeletePlan(values)

  def union(left: MilvusDeletePlan, right: MilvusDeletePlan): MilvusDeletePlan =
    (left, right) match {
      case (EmptyDeletePlan, other)                   => other
      case (other, EmptyDeletePlan)                   => other
      case (LongPkDeletePlan(a), LongPkDeletePlan(b)) => fromLongPks(a ++ b)
      case (StringPkDeletePlan(a), StringPkDeletePlan(b)) =>
        fromStringPks(a ++ b)
      case _ =>
        throw new IllegalArgumentException(
          s"cannot union delete plans of different PK types: ${left.getClass.getSimpleName} vs ${right.getClass.getSimpleName}"
        )
    }

  def union(plans: Iterable[MilvusDeletePlan]): MilvusDeletePlan =
    plans.foldLeft(empty)(union)
}

case object EmptyDeletePlan extends MilvusDeletePlan {
  override def containsLongPk(value: Long): Boolean = false
  override def containsStringPk(value: String): Boolean = false
  override val isEmpty: Boolean = true
}

final case class LongPkDeletePlan(values: Set[Long]) extends MilvusDeletePlan {
  override def containsLongPk(value: Long): Boolean = values.contains(value)
  override def containsStringPk(value: String): Boolean = false
  override def isEmpty: Boolean = values.isEmpty
}

final case class StringPkDeletePlan(values: Set[String])
    extends MilvusDeletePlan {
  override def containsLongPk(value: Long): Boolean = false
  override def containsStringPk(value: String): Boolean = values.contains(value)
  override def isEmpty: Boolean = values.isEmpty
}
