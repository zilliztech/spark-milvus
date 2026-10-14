package com.zilliz.spark.connector.read

import org.scalatest.funsuite.AnyFunSuite

class MilvusDeletePlanTest extends AnyFunSuite {
  test("Int64 upsert deletes the old row but preserves its replacement") {
    val plan = MilvusDeletePlan.fromLongPks(Map(7L -> 200L))
    assert(plan.containsLongPk(7L, 199L))
    assert(!plan.containsLongPk(7L, 200L))
    assert(!plan.containsLongPk(7L, 201L))
    assert(!plan.containsLongPk(8L, 100L))
  }

  test("VarChar upsert keeps equality after merging segment and L0 deletes") {
    val plan = MilvusDeletePlan.union(
      MilvusDeletePlan.fromStringPks(Map("pk" -> 100L)),
      MilvusDeletePlan.fromStringPks(Map("pk" -> 200L))
    )
    assert(plan.containsStringPk("pk", 199L))
    assert(!plan.containsStringPk("pk", 200L))
    assert(!plan.containsStringPk("pk", 201L))
    assert(!plan.containsStringPk("other", 100L))
  }
}
