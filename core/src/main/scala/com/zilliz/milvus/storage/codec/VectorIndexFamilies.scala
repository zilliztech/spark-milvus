package com.zilliz.milvus.storage.codec

import java.util.Locale

/** The vector index types this connector reads and writes, and what their
  * persisted streams begin with.
  *
  * One list, because these two facts are the same fact: an index type the
  * loader accepts has to be one the writer may produce, and the check that a
  * stream belongs to the type the snapshot declared is a check on the bytes
  * that type writes. Keeping the type list in one place and the byte markers in
  * another is how a family ends up declared but unreadable.
  *
  * Faiss and Knowhere name the index class in the first four bytes: `IHNs` an
  * HNSW with scalar quantization, `IwFl` an IVF over uncompressed vectors,
  * `IxF2` a flat index over squared L2, `IBxF` a flat index over binary
  * vectors. One index type writes several of them — HNSW_SQ over COSINE writes
  * `IHNa` and over L2 writes `IHNs` — so the check is the family's two-byte
  * prefix, which still catches an IVF stream under a declared HNSW index while
  * leaving Knowhere to refuse a stream it cannot read.
  */
object VectorIndexFamilies {

  val Hnsw: Set[String] = Set("HNSW", "HNSW_SQ", "HNSW_PQ", "HNSW_PRQ")

  val Ivf: Set[String] = Set("IVF_FLAT", "IVF_SQ8", "IVF_PQ", "BIN_IVF_FLAT")

  val Flat: Set[String] = Set("FLAT", "BIN_FLAT")

  val Supported: Set[String] = Hnsw ++ Ivf ++ Flat

  def familyOf(indexType: String): String = {
    val name = normalized(indexType)
    if (Hnsw.contains(name)) "HNSW"
    else if (Ivf.contains(name)) "IVF"
    else "FLAT"
  }

  /** The two bytes a stream of this index type starts with. A binary index
    * writes `IB*` whatever its structure, so it belongs to both the IVF and the
    * flat set.
    */
  def magicsOf(indexType: String): Set[String] = familyOf(indexType) match {
    case "HNSW" => Set("IH")
    case "IVF"  => Set("Iw", "IB")
    case _      => Set("Ix", "IB")
  }

  private def normalized(indexType: String): String =
    Option(indexType).map(_.trim.toUpperCase(Locale.ROOT)).getOrElse("")
}
