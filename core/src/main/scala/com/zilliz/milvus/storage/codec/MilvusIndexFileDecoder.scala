package com.zilliz.milvus.storage.codec

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName

/** Decodes the Milvus envelope; the result is still a named Knowhere payload,
  * not an index. File names and slice assembly belong to the index loader.
  */
object MilvusIndexFileDecoder extends IndexFileDecoder {
  private val MaxDecodedBytes = 256L * 1024 * 1024
  override def decode(bytes: Array[Byte]): DecodedIndexFile = {
    val file = BinlogCodec.parse(bytes, "index object")
    require(
      file.events.size == 1 && file.events.head.kind == 7,
      "A Milvus index object must contain exactly one IndexFileEvent"
    )
    // A nullable column's index names the rows it holds in its valid_data
    // payload, which `IndexRowMapping` turns into segment row numbers.
    Option(file.extras.get("nullable")).foreach { value =>
      require(value.isBoolean, "Index descriptor nullable must be a boolean")
    }
    val build = Option(file.extras.get("indexBuildID"))
      .getOrElse {
        throw new IllegalArgumentException(
          "Index descriptor has no indexBuildID"
        )
      }
      .asText()
      .toLong
    val payload = file.events.head.payload
    val decoded = file.dataType match {
      case 0 => payload
      case 2 | 20 =>
        val output = new ByteArrayOutputStream()
        var rows = 0L
        BinlogCodec.forEachRow(payload) { row =>
          require(
            row.columnCount == 1 && !row.isNull(0),
            "Index Parquet payload must have one non-null column"
          )
          if (file.dataType == 2) {
            require(
              rows < MaxDecodedBytes,
              "Decoded index object exceeds 256 MiB"
            )
            require(
              row.primitiveType(0) == PrimitiveTypeName.INT32,
              "INT8 index payload must use Parquet INT32"
            )
            val value = row.getInt(0)
            require(
              value >= -128 && value <= 127,
              "INT8 index value is out of range"
            )
            output.write(value & 0xff)
          } else {
            require(
              rows == 0 && row.primitiveType(0) == PrimitiveTypeName.BINARY,
              "Legacy STRING index payload must contain exactly one binary value"
            )
            val value = row.getBytes(0)
            require(
              value.length.toLong <= MaxDecodedBytes,
              "Decoded index object exceeds 256 MiB"
            )
            output.write(value)
          }
          rows += 1
        }
        require(rows > 0, "Empty index Parquet payload")
        output.toByteArray
      case other =>
        throw new UnsupportedOperationException(
          s"Unsupported Milvus index payload data type $other"
        )
    }
    require(decoded.nonEmpty, "Empty index payload")
    require(
      decoded.length.toLong <= MaxDecodedBytes,
      "Decoded index object exceeds 256 MiB"
    )
    new DecodedIndexFile {
      private var data: Array[Byte] = decoded
      override val collectionId: Long = file.collectionId
      override val partitionId: Long = file.partitionId
      override val segmentId: Long = file.segmentId
      override val fieldId: Long = file.fieldId
      override val buildId: Long = build
      override val payloadLength: Long = decoded.length.toLong
      override def readPayload(offset: Long, destination: ByteBuffer): Unit = {
        require(data != null, "Decoded index payload is closed")
        require(
          offset >= 0 && offset <= payloadLength - destination.remaining(),
          "Decoded index payload read is out of bounds"
        )
        destination.put(data, offset.toInt, destination.remaining())
      }
      override def close(): Unit = { data = null }
    }
  }
}
