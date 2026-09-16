package com.zilliz.milvus.storage.index

import java.nio.ByteBuffer

/** Decodes one Milvus index object after ObjectStore has read its bytes. */
trait IndexFileDecoder {
  def decode(bytes: Array[Byte]): DecodedIndexFile
}

/** Decoded object identity and payload. Closing releases its backing storage.
  */
trait DecodedIndexFile extends AutoCloseable {
  def collectionId: Long
  def partitionId: Long
  def segmentId: Long
  def fieldId: Long
  def buildId: Long
  def payloadLength: Long

  /** Copies exactly destination.remaining bytes and advances its position. */
  def readPayload(offset: Long, destination: ByteBuffer): Unit
}
