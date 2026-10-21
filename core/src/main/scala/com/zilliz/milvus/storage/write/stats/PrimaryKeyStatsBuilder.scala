package com.zilliz.milvus.storage.write.stats

import java.nio.charset.StandardCharsets

import com.zilliz.milvus.storage.stats.PrimaryKeyStats
import io.milvus.grpc.schema.DataType

/** Collects the keys of one segment and builds the stats once they are all in,
  * because the filter's size depends on their number.
  */
final class PrimaryKeyStatsBuilder(fieldId: Long, pkType: DataType) {
  import PrimaryKeyStats._
  require(
    pkType == DataType.Int64 || pkType == DataType.VarChar,
    s"a primary key is Int64 or VarChar, not $pkType"
  )
  private var hashes = new Array[Long](1024)
  private var count = 0
  private var minLong = Long.MaxValue
  private var maxLong = Long.MinValue
  private var minString: String = null
  private var maxString: String = null
  private var minUtf8: Array[Byte] = null
  private var maxUtf8: Array[Byte] = null

  private def push(hash: Long): Unit = {
    if (count == hashes.length)
      hashes = java.util.Arrays.copyOf(hashes, hashes.length * 2)
    hashes(count) = hash
    count += 1
  }

  def addLong(pk: Long): Unit = {
    require(pkType == DataType.Int64, s"an Int64 key on a $pkType field")
    if (pk < minLong) minLong = pk
    if (pk > maxLong) maxLong = pk
    push(hashLong(pk))
  }

  def addString(pk: String): Unit = {
    require(pkType == DataType.VarChar, s"a VarChar key on a $pkType field")
    val utf8 = pk.getBytes(StandardCharsets.UTF_8)
    if (minUtf8 == null || compareVarChar(utf8, minUtf8) < 0) {
      minUtf8 = utf8; minString = pk
    }
    if (maxUtf8 == null || compareVarChar(utf8, maxUtf8) > 0) {
      maxUtf8 = utf8; maxString = pk
    }
    push(hashString(pk))
  }

  def size: Int = count

  def build(): PrimaryKeyStats = {
    require(
      count > 0,
      "no primary key was added; a segment with no rows has no stats"
    )
    val filter = BlockedBloomFilterBuilder.sized(count.toLong)
    var i = 0
    while (i < count) { filter.add(hashes(i)); i += 1 }
    pkType match {
      case DataType.Int64 =>
        new PrimaryKeyStats(
          fieldId,
          pkType,
          filter.build(),
          minLong,
          maxLong,
          count.toLong
        )
      case _ =>
        new PrimaryKeyStats(
          fieldId,
          pkType,
          filter.build(),
          minString,
          maxString,
          count.toLong
        )
    }
  }
}
