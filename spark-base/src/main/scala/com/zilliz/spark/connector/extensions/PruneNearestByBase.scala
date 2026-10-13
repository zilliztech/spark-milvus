package com.zilliz.spark.connector.extensions

import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule

/** Drops from a [[MilvusNearestByJoin]]'s output the base columns the plan
  * above it does not read.
  *
  * Spark's `ColumnPruning` prunes a node's children to what the node and its
  * parent reference. The join references its base vector for the ranking, so
  * the vector column stays in the base, and because the join's output was the
  * base's output, the vector stayed in the output too: the take stage then read
  * it for every hit, 3 KB a row, though the ranking it serves was already
  * answered by the index or by the search task. This rule runs in the same
  * optimizer batch as `ColumnPruning` and keeps in the join's output only the
  * base columns the project above it references; the base keeps the vector for
  * the ranking (docs/design/enhance/2026-10-13-search-take-stage.html).
  */
object PruneNearestByBase extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.transform {
    case project @ Project(_, join: MilvusNearestByJoin)
        if join.baseOutput.exists(a => !project.references.contains(a)) =>
      project.copy(child =
        join.copy(prunedBase =
          Some(join.baseOutput.filter(project.references.contains))
        )
      )
  }
}
