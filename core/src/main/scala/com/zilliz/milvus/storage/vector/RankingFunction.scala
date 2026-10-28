package com.zilliz.milvus.storage.vector

import java.lang.{Float => JavaFloat}

/** The function a search ranks a pair by when the engine does not score that
  * pair the way the function computes it.
  *
  * A NEAREST BY that the connector executes is ranked by one of Spark's vector
  * functions. Knowhere scores the pairs inside [[EngineRange]], where its
  * float32 inner product and cosine and its L2 computed pair by pair agree with
  * the function up to rounding (its batched L2 does not, and [[ExactScan]]
  * takes its candidates computed again pair by pair); a base row outside that
  * range is scored by this function against every query of the group, and the
  * score joins Knowhere's in the same [[TopKMerger]]. The implementation uses
  * the Spark line's own function and converts its result to the engine's score
  * scale in Float before local top-k selection. L2 squares the function's
  * distance in Double and rounds that square to Float; newly equal scores use
  * the same physical-address tie-break as engine hits.
  */
trait RankingFunction extends Serializable {

  /** The pair's score on the scale the engine scores the metric -- the squared
    * distance for L2, the value itself for IP and COSINE -- or null when the
    * function gives the pair no value. Intermediate arithmetic may use Double,
    * but the returned Float is the value every candidate merge ranks. Both
    * arrays have the field's dimension.
    */
  def score(query: Array[Float], row: Array[Float]): JavaFloat
}
