package com.zilliz.spark.connector.read

import org.apache.arrow.vector.VectorSchemaRoot

import com.zilliz.milvus.storage.expr.{
  Bitmap,
  Evaluator,
  Expr,
  PredicateEvaluator,
  PredicateExpr
}

/** Evaluates the two connector-owned filter contracts against one Arrow batch.
  *
  * Milvus expressions and Spark predicates keep separate representations and
  * evaluators because their literal and floating-point semantics differ. At the
  * reader boundary both produce an exclusion bitmap, so OR implements the
  * conjunction visible to the user without translating either contract.
  */
private[read] object BatchFilterEvaluator {
  def exclusions(
      batch: VectorSchemaRoot,
      milvusFilter: Option[Expr],
      pushedExpression: Option[PredicateExpr],
      arrowColumnFor: String => String,
      columnNameFor: Long => Option[String]
  ): Option[Bitmap] = {
    val milvus = milvusFilter.map(
      Evaluator.evaluate(_, batch, arrowColumnFor)
    )
    val spark = pushedExpression.map(
      PredicateEvaluator.evaluate(_, batch, columnNameFor)
    )
    (milvus, spark) match {
      case (Some(left), Some(right)) => Some(left.or(right))
      case (some @ Some(_), None)    => some
      case (None, some @ Some(_))    => some
      case _                         => None
    }
  }
}
