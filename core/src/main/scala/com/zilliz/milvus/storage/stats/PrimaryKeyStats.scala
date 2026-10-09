package com.zilliz.milvus.storage.stats

import java.nio.charset.StandardCharsets

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

import io.milvus.grpc.schema.DataType

import net.openhft.hashing.LongHashFunction

/** The primary-key statistics Milvus keeps per segment,
  * `_stats/bloom_filter.<field id>/<log id>`: the JSON `PrimaryKeyStats` of
  * `internal/storage/stats.go`, a blocked bloom filter over the keys plus their
  * minimum and maximum. Milvus routes deletes and prunes primary-key queries
  * with it.
  *
  * An Int64 key is hashed as its 8 little-endian bytes, a VarChar key as its
  * UTF-8 bytes, both with xxh3 (seed 0), as `stats.Update` does. The filter is
  * sized for the number of keys at Milvus's default rate, so keys are collected
  * first and the filter built at the end.
  */
final class PrimaryKeyStats private (
    val fieldId: Long,
    val pkType: DataType,
    val filter: BlockedBloomFilter,
    val minPk: Any,
    val maxPk: Any,
    val keys: Long
) {

  def mightContainLong(pk: Long): Boolean =
    filter.mightContain(PrimaryKeyStats.hashLong(pk))

  def mightContainString(pk: String): Boolean =
    filter.mightContain(PrimaryKeyStats.hashString(pk))

  /** The JSON Milvus writes, field for field and in its order. `max` and `min`
    * are the legacy Int64 fields Milvus writes as 0.
    */
  def toJson: String = {
    val pk: Any => String = {
      case s: String => PrimaryKeyStats.mapper.writeValueAsString(s)
      case other     => other.toString
    }
    s"""{"fieldID":$fieldId,"max":0,"min":0,"bfType":${PrimaryKeyStats.BlockedBfType},"bf":${filter.toJson},"pkType":${pkType.value},"maxPk":${pk(
        maxPk
      )},"minPk":${pk(minPk)}}"""
  }

  def toBytes: Array[Byte] = toJson.getBytes(StandardCharsets.UTF_8)
}

object PrimaryKeyStats {

  /** `bloomfilter.BlockedBF` in Milvus's enumeration. */
  val BlockedBfType: Int = 4

  private val mapper = new ObjectMapper()
  private val xxh3 = LongHashFunction.xx3()

  def hashLong(pk: Long): Long = {
    val bytes = new Array[Byte](8)
    var i = 0
    var v = pk
    while (i < 8) { bytes(i) = (v & 0xff).toByte; v >>>= 8; i += 1 }
    xxh3.hashBytes(bytes)
  }

  def hashString(pk: String): Long =
    xxh3.hashBytes(pk.getBytes(StandardCharsets.UTF_8))

  /** The order Milvus keeps VarChar keys in: Go string comparison, which is
    * unsigned byte order over UTF-8 (primary_key.go, stats.go). Java's
    * String.compareTo orders UTF-16 code units and disagrees above the BMP, so
    * bounds computed with it exclude keys Milvus has. Any code that compares
    * against minPk/maxPk uses this order.
    */
  def compareVarChar(a: Array[Byte], b: Array[Byte]): Int =
    java.util.Arrays.compareUnsigned(a, b)

  def compareVarChar(a: String, b: String): Int =
    compareVarChar(
      a.getBytes(StandardCharsets.UTF_8),
      b.getBytes(StandardCharsets.UTF_8)
    )

  /** Collects the keys of one segment and builds the stats once they are all
    * in, because the filter's size depends on their number.
    */
  final class Builder(fieldId: Long, pkType: DataType) {
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
      push(xxh3.hashBytes(utf8))
    }

    def size: Int = count

    def build(): PrimaryKeyStats = {
      require(
        count > 0,
        "no primary key was added; a segment with no rows has no stats"
      )
      val filter = BlockedBloomFilter.sized(count.toLong)
      var i = 0
      while (i < count) { filter.add(hashes(i)); i += 1 }
      pkType match {
        case DataType.Int64 =>
          new PrimaryKeyStats(
            fieldId,
            pkType,
            filter,
            minLong,
            maxLong,
            count.toLong
          )
        case _ =>
          new PrimaryKeyStats(
            fieldId,
            pkType,
            filter,
            minString,
            maxString,
            count.toLong
          )
      }
    }
  }

  /** Reads what Milvus (or this class) wrote. */
  def fromJson(json: String): PrimaryKeyStats = {
    val node = mapper.readTree(json)
    require(
      node != null && node.isObject,
      "primary-key stats must be a JSON object"
    )
    fromNode(node)
  }

  /** Reads either `StatsWriter.Generate`'s single object or `GenerateList`'s
    * compound array. An empty compound file is invalid: a caller cannot use it
    * to prove that a segment has no matching key.
    */
  def fromBytes(bytes: Array[Byte]): Seq[PrimaryKeyStats] = {
    val root = mapper.readTree(bytes)
    require(root != null, "primary-key stats JSON must not be empty")
    if (root.isObject) Seq(fromNode(root))
    else if (root.isArray) {
      require(root.size() > 0, "compound primary-key stats must not be empty")
      Iterator.range(0, root.size()).map(i => fromNode(root.get(i))).toSeq
    } else {
      throw new IllegalArgumentException(
        "primary-key stats root must be an object or array"
      )
    }
  }

  private def fromNode(node: JsonNode): PrimaryKeyStats = {
    require(
      node != null && node.isObject,
      "primary-key stats entry must be an object"
    )
    val fieldIdNode = required(node, "fieldID")
    requireIntegralLong(fieldIdNode, "fieldID")
    val fieldId = fieldIdNode.longValue()

    val bfTypeNode = required(node, "bfType")
    requireIntegralInt(bfTypeNode, "bfType")
    val bfType = bfTypeNode.intValue()
    require(
      bfType == BlockedBfType,
      s"bfType $bfType is not the blocked bloom filter ($BlockedBfType)"
    )
    val bf = required(node, "bf")
    require(bf.isObject, "primary-key stats bf must be an object")
    val kNode = required(bf, "k")
    requireIntegralInt(kNode, "bf.k")
    val blocksNode = required(bf, "b")
    require(blocksNode.isArray, "primary-key stats bf.b must be an array")
    val blocks = Iterator
      .range(0, blocksNode.size())
      .map { index =>
        val block = blocksNode.get(index)
        require(
          block != null && block.isTextual,
          s"primary-key stats bf.b[$index] must be a string"
        )
        block.textValue()
      }
      .toSeq
    val filter = BlockedBloomFilter.fromJson(kNode.intValue(), blocks)

    val pkTypeNode = required(node, "pkType")
    requireIntegralInt(pkTypeNode, "pkType")
    val pkType = DataType.fromValue(pkTypeNode.intValue())
    require(
      pkType == DataType.Int64 || pkType == DataType.VarChar,
      s"unsupported primary-key stats type $pkType"
    )
    Seq("min", "max").foreach { name =>
      requireIntegralLong(required(node, name), name)
    }
    val (min, max) = pkType match {
      case DataType.Int64 =>
        val minPk = required(node, "minPk")
        val maxPk = required(node, "maxPk")
        requireIntegralLong(minPk, "minPk")
        requireIntegralLong(maxPk, "maxPk")
        (minPk.longValue(), maxPk.longValue())
      case _ =>
        val minPk = required(node, "minPk")
        val maxPk = required(node, "maxPk")
        require(minPk.isTextual, "primary-key stats minPk must be a string")
        require(maxPk.isTextual, "primary-key stats maxPk must be a string")
        (minPk.textValue(), maxPk.textValue())
    }
    new PrimaryKeyStats(
      fieldId,
      pkType,
      filter,
      min,
      max,
      -1L
    )
  }

  private def required(parent: JsonNode, name: String): JsonNode = {
    val value = parent.get(name)
    require(
      value != null && !value.isNull,
      s"primary-key stats $name is missing"
    )
    value
  }

  private def requireIntegralLong(value: JsonNode, name: String): Unit =
    require(
      value.isIntegralNumber && value.canConvertToLong,
      s"primary-key stats $name must be a 64-bit integer"
    )

  private def requireIntegralInt(value: JsonNode, name: String): Unit =
    require(
      value.isIntegralNumber && value.canConvertToInt,
      s"primary-key stats $name must be a 32-bit integer"
    )
}
