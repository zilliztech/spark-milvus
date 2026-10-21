package com.zilliz.milvus.storage.write.codec

import com.fasterxml.jackson.databind.ObjectMapper

import com.zilliz.milvus.storage.codec.IndexFileFormat.Slice

/** Encodes the slice metadata emitted beside built index payloads. */
object IndexFileEncoder {
  private val mapper = new ObjectMapper()

  /** Encodes the slice metadata the loader reads through
    * [[com.zilliz.milvus.storage.codec.IndexFileCodec.parseSlices]].
    */
  def sliceMeta(slices: Seq[Slice]): Array[Byte] = {
    val root = mapper.createObjectNode()
    val meta = root.putArray("meta")
    slices.foreach { slice =>
      meta
        .addObject()
        .put("name", slice.name)
        .put("slice_num", slice.count)
        .put("total_len", slice.length)
    }
    mapper.writeValueAsBytes(root)
  }

}
