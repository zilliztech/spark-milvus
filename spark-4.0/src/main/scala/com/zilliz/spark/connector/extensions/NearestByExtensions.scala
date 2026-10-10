package com.zilliz.spark.connector.extensions

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.SparkSessionExtensions

/** Spark 4.0 has no NearestByJoin, so the connector brings the pieces
  * (docs/design/architecture/dataframe-api.html section 6): the vector
  * functions `vector_l2_distance`, `vector_cosine_similarity` and
  * `vector_inner_product`, the table function `nearest_by_join`, and the rule
  * that replaces the [[NearestByJoinRequest]] those and `nearestByJoin` build.
  */
object NearestByExtensions {
  def apply(extensions: SparkSessionExtensions): Unit = {
    VectorFunction.registrations.foreach(extensions.injectFunction)
    extensions.injectTableFunction(
      (
        FunctionIdentifier(NearestByJoinFunction.Name),
        NearestByJoinFunction.info,
        NearestByJoinFunction.builder _
      )
    )
    extensions.injectPostHocResolutionRule(new ReplaceNearestByJoinRequest(_))
  }
}
