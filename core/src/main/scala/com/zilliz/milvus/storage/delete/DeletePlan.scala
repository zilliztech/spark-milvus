package com.zilliz.milvus.storage.delete

sealed trait DeletePlan {
  def containsLongPk(value: Long, rowTs: Long): Boolean
  def containsStringPk(value: String, rowTs: Long): Boolean
  def isEmpty: Boolean
}

object DeletePlan {
  val empty: DeletePlan = EmptyDeletePlan

  def fromLongPks(values: Map[Long, Long]): DeletePlan =
    if (values.isEmpty) empty else LongPkDeletePlan(values)

  def fromStringPks(values: Map[String, Long]): DeletePlan =
    if (values.isEmpty) empty else StringPkDeletePlan(values)

  def union(left: DeletePlan, right: DeletePlan): DeletePlan =
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

  def union(plans: Iterable[DeletePlan]): DeletePlan =
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

case object EmptyDeletePlan extends DeletePlan {
  override def containsLongPk(value: Long, rowTs: Long): Boolean = false
  override def containsStringPk(value: String, rowTs: Long): Boolean = false
  override val isEmpty: Boolean = true
}

final case class LongPkDeletePlan(values: Map[Long, Long]) extends DeletePlan {
  override def containsLongPk(value: Long, rowTs: Long): Boolean =
    values.get(value).exists(_ >= rowTs)
  override def containsStringPk(value: String, rowTs: Long): Boolean = false
  override def isEmpty: Boolean = values.isEmpty
}

final case class StringPkDeletePlan(values: Map[String, Long])
    extends DeletePlan {
  override def containsLongPk(value: Long, rowTs: Long): Boolean = false
  override def containsStringPk(value: String, rowTs: Long): Boolean =
    values.get(value).exists(_ >= rowTs)
  override def isEmpty: Boolean = values.isEmpty
}
