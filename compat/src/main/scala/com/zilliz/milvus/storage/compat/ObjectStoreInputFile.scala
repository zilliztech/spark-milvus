package com.zilliz.milvus.storage.compat

import java.io.EOFException
import java.nio.ByteBuffer

import org.apache.parquet.io.{InputFile, SeekableInputStream}

import com.zilliz.milvus.storage.io.ObjectStore

/** Presents a key in an [[ObjectStore]] as a parquet [[InputFile]].
  *
  * Parquet reads a footer by seeking to the end and then to the offsets it
  * finds there, so only the ranges it asks for are fetched. The file size is
  * looked up once and reused for every range.
  */
private[compat] object ObjectStoreInputFile {

  def apply(store: ObjectStore, key: String): InputFile = {
    val length = store.size(key)
    new InputFile {
      override def getLength: Long = length
      override def newStream(): SeekableInputStream =
        new RangeStream(store, key, length)
    }
  }

  private final class RangeStream(
      store: ObjectStore,
      key: String,
      length: Long
  ) extends SeekableInputStream {

    private var position: Long = 0L

    override def getPos: Long = position

    override def seek(newPos: Long): Unit = position = newPos

    override def read(): Int = {
      if (position >= length) return -1
      val b = fetch(1)
      position += 1
      b(0) & 0xff
    }

    override def read(buffer: Array[Byte], offset: Int, len: Int): Int = {
      if (position >= length) return -1
      val n = math.min(len.toLong, length - position).toInt
      if (n <= 0) return 0
      System.arraycopy(fetch(n), 0, buffer, offset, n)
      position += n
      n
    }

    override def readFully(buffer: Array[Byte]): Unit =
      readFully(buffer, 0, buffer.length)

    override def readFully(buffer: Array[Byte], offset: Int, len: Int): Unit = {
      if (len == 0) return
      require(len)
      System.arraycopy(fetch(len), 0, buffer, offset, len)
      position += len
    }

    override def read(buffer: ByteBuffer): Int = {
      if (position >= length) return -1
      val n = math.min(buffer.remaining().toLong, length - position).toInt
      if (n <= 0) return 0
      buffer.put(fetch(n))
      position += n
      n
    }

    override def readFully(buffer: ByteBuffer): Unit = {
      val n = buffer.remaining()
      if (n == 0) return
      require(n)
      buffer.put(fetch(n))
      position += n
    }

    override def close(): Unit = ()

    private def require(n: Int): Unit =
      if (position + n > length) {
        throw new EOFException(
          s"$key: asked for $n bytes at $position but the file is $length"
        )
      }

    private def fetch(n: Int): Array[Byte] =
      store.readAt(key, position, n.toLong, length)
  }
}
