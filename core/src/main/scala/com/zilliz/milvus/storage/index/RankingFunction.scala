package com.zilliz.milvus.storage.index

/** The function a search ranks a pair by when the engine does not score that
  * pair the way the function computes it (docs/design/architecture/
  * dataframe-api.html section 2).
  *
  * A NEAREST BY that the connector executes is ranked by one of Spark's vector
  * functions. Knowhere scores the pairs inside [[EngineRange]], where its
  * float32 arithmetic and the function's agree up to rounding; a base row
  * outside that range is scored by this function against every query of the
  * group, and the score joins Knowhere's in the same [[TopKMerger]]. The
  * implementation is the Spark line's own function, so the value is the one
  * Spark would rank by.
  */
trait RankingFunction extends Serializable {

  /** The pair's score on the scale the engine scores the metric -- the squared
    * distance for L2, the value itself for IP and COSINE -- or null when the
    * function gives the pair no value. Both arrays have the field's dimension.
    */
  def score(query: Array[Float], row: Array[Float]): java.lang.Double
}
