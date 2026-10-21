package com.zilliz.milvus.storage.write.stats

import java.util.Base64

import com.zilliz.milvus.storage.stats.BlockedBloomFilter

/** Encodes the blocked Bloom filter carried by primary-key statistics. */
object BlockedBloomFilterEncoder {
  def toJson(filter: BlockedBloomFilter): String = {
    import filter._
    val sb = new StringBuilder
    sb.append("{\"k\":").append(k).append(",\"b\":[")
    var b = 0
    while (b < numBlocks) {
      if (b > 0) sb.append(',')
      sb.append('"')
        .append(Base64.getUrlEncoder.encodeToString(blockBytes(b)))
        .append('"')
      b += 1
    }
    sb.append("]}")
    sb.toString
  }

}
