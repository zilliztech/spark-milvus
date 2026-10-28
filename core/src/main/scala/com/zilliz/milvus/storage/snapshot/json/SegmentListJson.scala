package com.zilliz.milvus.storage.snapshot.json

/** JSON for explicit Storage V3 manifest entries supplied by a reader. */
object SegmentListJson {
  def encodeManifestItems(items: Seq[ManifestItemJson]): String =
    Mapper.mapper.writeValueAsString(items)

  def decodeManifestItems(
      json: String
  ): Either[Throwable, Seq[ManifestItemJson]] =
    Mapper.read[Seq[ManifestItemJson]](json)
}
