package com.zilliz.milvus.storage.write.stats

import java.nio.charset.StandardCharsets

import com.fasterxml.jackson.databind.ObjectMapper

import com.zilliz.milvus.storage.stats.PrimaryKeyStats

/** Encodes primary-key statistics for a stored segment. */
object PrimaryKeyStatsEncoder {
  private val mapper = new ObjectMapper()

  /** The JSON Milvus writes, field for field and in its order. `max` and `min`
    * are the legacy Int64 fields Milvus writes as 0.
    */
  def toJson(stats: PrimaryKeyStats): String = {
    import stats._
    val pk: Any => String = {
      case s: String => mapper.writeValueAsString(s)
      case other     => other.toString
    }
    s"""{"fieldID":$fieldId,"max":0,"min":0,"bfType":${PrimaryKeyStats.BlockedBfType},"bf":${BlockedBloomFilterEncoder
        .toJson(filter)},"pkType":${pkType.value},"maxPk":${pk(
        maxPk
      )},"minPk":${pk(minPk)}}"""
  }

  def toBytes(stats: PrimaryKeyStats): Array[Byte] =
    toJson(stats).getBytes(StandardCharsets.UTF_8)
}
