package com.zilliz.milvus.storage.codec

/** Logical payload names, slices and bounded copy sizes shared by index readers
  * and encoders. These definitions do not load or write an index.
  */
object IndexFileFormat {
  val SliceMeta: String = "SLICE_META"
  val MaxObjectBytes: Long = 256L * 1024 * 1024
  val ChunkBytes: Int = 1024 * 1024

  final case class Slice(name: String, count: Int, length: Long)

  def validName(name: String): Boolean =
    name.matches("[A-Za-z0-9_+.-]+") && name != "." && name != ".."
}
