package com.zilliz.spark.connector.read

sealed trait MilvusDeletePlan {
  def containsLongPk(value: Long, rowTs: Long): Boolean
  def containsStringPk(value: String, rowTs: Long): Boolean
  def isEmpty: Boolean
}

object MilvusDeletePlan {
  val empty: MilvusDeletePlan = EmptyDeletePlan

  def fromLongPks(values: Map[Long, Long]): MilvusDeletePlan =
    if (values.isEmpty) empty else LongPkDeletePlan(values)

  def fromStringPks(values: Map[String, Long]): MilvusDeletePlan =
    if (values.isEmpty) empty else StringPkDeletePlan(values)

  def union(left: MilvusDeletePlan, right: MilvusDeletePlan): MilvusDeletePlan =
    (left, right) match {
      case (EmptyDeletePlan, other) => other
      case (other, EmptyDeletePlan) => other
      case (LongPkDeletePlan(a), LongPkDeletePlan(b)) =>
        fromLongPks(mergeDeleteTimestamps(a, b))
      case (StringPkDeletePlan(a), StringPkDeletePlan(b)) =>
        fromStringPks(mergeDeleteTimestamps(a, b))
      case _ =>
        throw new IllegalArgumentException(
          s"cannot union delete plans of different PK types: ${left.getClass.getSimpleName} vs ${right.getClass.getSimpleName}"
        )
    }

  def union(plans: Iterable[MilvusDeletePlan]): MilvusDeletePlan =
    plans.foldLeft(empty)(union)

  private def mergeDeleteTimestamps[K](
      left: Map[K, Long],
      right: Map[K, Long]
  ): Map[K, Long] =
    (left.keySet ++ right.keySet).iterator.map { key =>
      key -> math.max(
        left.getOrElse(key, Long.MinValue),
        right.getOrElse(key, Long.MinValue)
      )
    }.toMap
}

case object EmptyDeletePlan extends MilvusDeletePlan {
  override def containsLongPk(value: Long, rowTs: Long): Boolean = false
  override def containsStringPk(value: String, rowTs: Long): Boolean = false
  override val isEmpty: Boolean = true
}

final case class LongPkDeletePlan(values: Map[Long, Long])
    extends MilvusDeletePlan {
  override def containsLongPk(value: Long, rowTs: Long): Boolean =
    values.get(value).exists(_ >= rowTs)
  override def containsStringPk(value: String, rowTs: Long): Boolean = false
  override def isEmpty: Boolean = values.isEmpty
}

final case class StringPkDeletePlan(values: Map[String, Long])
    extends MilvusDeletePlan {
  override def containsLongPk(value: Long, rowTs: Long): Boolean = false
  override def containsStringPk(value: String, rowTs: Long): Boolean =
    values.get(value).exists(_ >= rowTs)
  override def isEmpty: Boolean = values.isEmpty
}
