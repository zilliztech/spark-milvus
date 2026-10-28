package com.zilliz.spark.connector.read

import org.apache.arrow.vector.VectorSchemaRoot

import com.zilliz.milvus.storage.expr.{
  ExclusionBitmap,
  FilterEvaluator,
  FilterExpr,
  PredicateEvaluator,
  PredicateExpr
}

/** The rows of one Arrow batch that the Milvus filter or the predicate Spark
  * pushed into the scan rejects.
  *
  * The two keep separate representations and evaluators because their literal
  * and floating-point semantics differ. At the reader boundary both produce an
  * exclusion bitmap, so OR implements the conjunction visible to the user
  * without translating either contract.
  */
private[read] object BatchExclusions {
  def of(
      batch: VectorSchemaRoot,
      milvusFilter: Option[FilterExpr],
      pushedExpression: Option[PredicateExpr],
      arrowColumnFor: String => String,
      columnNameFor: Long => Option[String]
  ): Option[ExclusionBitmap] = {
    val milvus = milvusFilter.map(
      FilterEvaluator.evaluate(_, batch, arrowColumnFor)
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
