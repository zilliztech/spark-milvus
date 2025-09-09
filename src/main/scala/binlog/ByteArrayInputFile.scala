package com.zilliz.spark.connector.binlog

import java.io.IOException
import java.nio.ByteBuffer

import org.apache.parquet.io.InputFile
import org.apache.parquet.io.SeekableInputStream

/** An implementation of Parquet's InputFile interface that wraps a byte array.
  * Each call to newStream() returns an independent SeekableInputStream
  * instance.
  *
  * @param data
  *   The byte array containing the Parquet file content.
  */
class ByteArrayInputFile(data: Array[Byte]) extends InputFile {

  // Wrap the entire byte array into a read-only ByteBuffer.
  // asReadOnlyBuffer() ensures that the underlying data cannot be modified externally.
  private val byteBuffer: ByteBuffer = ByteBuffer.wrap(data).asReadOnlyBuffer()

  /** Returns the length of the underlying data source in bytes.
    */
  override def getLength: Long = data.length.toLong

  /** Creates and returns a new SeekableInputStream instance for reading data
    * from this InputFile. Each call to this method should return an independent
    * stream with its own read position.
    */
  override def newStream(): ByteBufferBackedSeekableInputStream = {
    // duplicate() creates a new ByteBuffer object that shares the same underlying data
    // as the original ByteBuffer but has its own independent position, limit, and mark.
    // This is crucial for ensuring that each SeekableInputStream instance has its own
    // independent reading state and doesn't interfere with others.
    val duplicatedBuffer = byteBuffer.duplicate()
    duplicatedBuffer.position(
      0
    ) // Ensure the new stream starts reading from the beginning
    new ByteBufferBackedSeekableInputStream(duplicatedBuffer)
  }
}

/** A SeekableInputStream implementation that reads data from a ByteBuffer. This
  * class is not thread-safe. If multiple threads need to read from the same
  * underlying ByteBuffer, a new instance should be created for each thread
  * (e.g., by calling InputFile.newStream()).
  *
  * @param buffer
  *   The ByteBuffer containing the data. Each instance should receive an
  *   independent view of the buffer (e.g., via duplicate()).
  */
class ByteBufferBackedSeekableInputStream(private val buffer: ByteBuffer)
    extends SeekableInputStream {

  @throws(classOf[IOException])
  override def read(): Int = {
    if (!buffer.hasRemaining) {
      -1 // End of stream
    } else {
      buffer
        .get() & 0xff // Read a single byte and convert to an unsigned integer
    }
  }

  @throws(classOf[IOException])
  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    if (!buffer.hasRemaining) {
      -1 // End of stream
    } else {
      val bytesToRead = Math.min(len, buffer.remaining())
      if (bytesToRead == 0) return 0 // No bytes left to read
      buffer.get(b, off, bytesToRead)
      bytesToRead
    }
  }

  @throws(classOf[IOException])
  override def read(buf: ByteBuffer): Int = {
    if (!buffer.hasRemaining) {
      -1 // End of stream
    } else {
      val bytesToTransfer = Math.min(buf.remaining(), buffer.remaining())
      if (bytesToTransfer == 0) return 0 // No bytes to transfer

      val originalLimit = buffer.limit() // Save original limit
      // Set a new limit to restrict reading, ensuring we don't exceed
      // the capacity of the target ByteBuffer or the current stream's remaining capacity.
      buffer.limit(buffer.position() + bytesToTransfer)
      buf.put(
        buffer
      ) // Copy data from the current stream's buffer to the provided buf
      buffer.limit(originalLimit) // Restore original limit
      bytesToTransfer
    }
  }

  @throws(classOf[IOException])
  override def readFully(bytes: Array[Byte]): Unit = {
    readFully(bytes, 0, bytes.length)
  }

  @throws(classOf[IOException])
  override def readFully(bytes: Array[Byte], offset: Int, length: Int): Unit = {
    val bytesRead = read(bytes, offset, length)
    if (bytesRead < length) {
      throw new IOException(
        s"Not enough bytes available. Expected $length, but only read $bytesRead."
      )
    }
  }

  /** Reads bytes from the stream directly into a ByteBuffer until the buffer's
    * limit is reached, or the end of the stream is encountered.
    *
    * @param buf
    *   The ByteBuffer to read into. Its position will be advanced by the number
    *   of bytes read.
    * @throws IOException
    *   If an I/O error occurs or not enough bytes are available to fill the
    *   buffer.
    */
  @throws(classOf[IOException])
  override def readFully(buf: ByteBuffer): Unit = {
    val bytesToRead = buf.remaining()
    val bytesTransferred = read(buf) // Use the existing read(ByteBuffer) method

    if (bytesTransferred < bytesToRead) {
      throw new IOException(
        s"Not enough bytes available to fill buffer. Expected ${bytesToRead}, but only read ${bytesTransferred}."
      )
    }
  }

  /** Returns the current position of the stream (byte offset).
    */
  @throws(classOf[IOException])
  override def getPos: Long = buffer.position().toLong

  /** Moves the current position of the stream to the specified offset.
    */
  @throws(classOf[IOException])
  override def seek(newPos: Long): Unit = {
    if (newPos < 0 || newPos > buffer.capacity()) {
      throw new IOException(
        s"Invalid seek position: $newPos. Buffer capacity: ${buffer.capacity()}"
      )
    }
    buffer.position(newPos.toInt) // ByteBuffer's position is an int
  }

  /** Skips over the specified number of bytes.
    */
  @throws(classOf[IOException])
  override def skip(n: Long): Long = {
    val oldPos = buffer.position()
    val newPos = Math.min(buffer.limit().toLong, oldPos + n)
    buffer.position(newPos.toInt)
    newPos - oldPos
  }

  /** Closes the stream. For memory-backed ByteBuffers, this is typically a
    * no-op.
    */
  @throws(classOf[IOException])
  override def close(): Unit = {
    // Usually no specific action needed for in-memory ByteBuffers
  }
}
