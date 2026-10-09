package com.zilliz.milvus.storage.schema

import java.util.Locale

/** The distance a vector search ranks by and an index is built for, the value
  * Milvus and Knowhere call `metric_type`.
  *
  * `name` is how Milvus records it in a snapshot's index parameters and how
  * Knowhere reads it in the JSON of a search or a build; it is also what
  * `toString` gives, so a message names a metric the way a call spells it.
  * `smallerIsBetter` is the direction scores rank in: a distance, where the
  * nearest row is the best, or a similarity, where the largest score is.
  */
sealed abstract class MetricType(
    val name: String,
    val smallerIsBetter: Boolean
) extends Product
    with Serializable {
  override def toString: String = name
}

object MetricType {
  case object L2 extends MetricType("L2", smallerIsBetter = true)
  case object IP extends MetricType("IP", smallerIsBetter = false)
  case object Cosine extends MetricType("COSINE", smallerIsBetter = false)
  case object Hamming extends MetricType("HAMMING", smallerIsBetter = true)
  case object Jaccard extends MetricType("JACCARD", smallerIsBetter = true)

  val values: Seq[MetricType] = Seq(L2, IP, Cosine, Hamming, Jaccard)

  /** The metric a name spells, in any letter case: a call's argument or the
    * `metric_type` a persisted index records.
    */
  def fromName(name: String): Option[MetricType] =
    Option(name)
      .map(_.toUpperCase(Locale.ROOT))
      .flatMap(upper => values.find(_.name == upper))

  /** The metrics a column of this element type is compared by: Hamming and
    * Jaccard over bits, L2, inner product and cosine over every other element
    * type. A search, a build and a persisted index are all checked against
    * this.
    */
  def forElementType(elementType: VectorElementType): Seq[MetricType] =
    elementType match {
      case VectorElementType.Bit => Seq(Hamming, Jaccard)
      case _                     => Seq(L2, IP, Cosine)
    }
}
