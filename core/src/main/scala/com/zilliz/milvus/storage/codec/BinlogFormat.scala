package com.zilliz.milvus.storage.codec

/** The fixed wire layout shared by binlog readers and encoders. */
object BinlogFormat {
  val Magic: Int = 0xfffabc
  val EventHeaderBytes = 17
  val BaseEventBytes = 16

  /** What Milvus records as each event type's fixed part: the descriptor's four
    * ids, two timestamps and data type, and two timestamps for every other
    * event (internal/core/src/storage/Event.cpp `GetEventFixPartSize`).
    */
  val PostHeaderLengths: Vector[Byte] =
    Vector[Byte](52, 16, 16, 16, 16, 16, 16, 16)
}
