package com.zilliz.milvus.storage.codec

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, EOFException}
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets
import java.util.{Arrays, Collections}
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.parquet.column.impl.ColumnReaderImpl
import org.apache.parquet.format.{FileMetaData => ThriftFileMetaData, Util}
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.io.{InputFile, SeekableInputStream}
import org.apache.parquet.io.api.PrimitiveConverter
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.VersionParser

/** The common Milvus binlog envelope and flat Parquet payload. This codec
  * receives bytes only; opening objects belongs to core.io.
  */
object BinlogCodec {
  private val mapper = new ObjectMapper()
  private val ParquetMagic = "PAR1".getBytes(StandardCharsets.US_ASCII)
  final case class Event(kind: Int, payload: Array[Byte])
  final case class Container(
      collectionId: Long,
      partitionId: Long,
      segmentId: Long,
      fieldId: Long,
      dataType: Int,
      extras: JsonNode,
      events: Vector[Event]
  )

  def parse(bytes: Array[Byte], path: String): Container = {
    val b = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
    def requireBytes(n: Int): Unit =
      if (n < 0 || b.remaining() < n)
        throw new EOFException(
          s"Truncated binlog $path at ${b.position()}: need $n bytes"
        )
    def fail(message: String): Nothing =
      throw new IllegalArgumentException(s"Invalid binlog $path: $message")
    def header(): (Int, Int) = {
      val start = b.position()
      requireBytes(17)
      b.getLong()
      val kind = b.get() & 0xff
      val length = b.getInt()
      val next = b.getInt()
      if (length < 17 || length.toLong > bytes.length.toLong - start)
        fail(s"event length $length at $start")
      val end = start + length
      // Milvus's serde writers leave NextPosition at newEventHeader's -1
      // (internal/storage/serde_delta.go, serde_events.go), and its readers
      // never look at it; a value that is set has to name the event end.
      if (next != 0 && next != -1 && next != end)
        fail(s"next position $next differs from event end $end")
      (kind, end)
    }
    requireBytes(4)
    if (b.getInt() != 0xfffabc) fail("magic is not 0xfffabc")
    val (kind, descriptorEnd) = header()
    if (kind != 0) fail("first event is not a descriptor")
    requireBytes(64)
    val collection = b.getLong()
    val partition = b.getLong()
    val segment = b.getLong()
    val field = b.getLong()
    b.getLong()
    b.getLong()
    val dataType = b.getInt()
    val post = Array.fill(8)(b.get() & 0xff)
    val extraLength = b.getInt()
    if (extraLength < 0 || extraLength != descriptorEnd - b.position())
      fail(s"descriptor extras length $extraLength")
    requireBytes(extraLength)
    val extra = new Array[Byte](extraLength)
    b.get(extra)
    val extras =
      if (extra.isEmpty) mapper.createObjectNode() else mapper.readTree(extra)
    if (extras == null || !extras.isObject)
      fail("descriptor extras must be an object")
    if (extras.has("edek") || extras.has("encryption_zone"))
      throw new UnsupportedOperationException(
        s"Encrypted Milvus binlog is unsupported: $path"
      )
    val events = Vector.newBuilder[Event]
    while (b.hasRemaining) {
      val (eventType, end) = header()
      if (eventType <= 0 || eventType >= post.length)
        fail(s"event type $eventType")
      val fixed = post(eventType)
      if (fixed < 16 || fixed > end - b.position())
        fail(s"event post-header length $fixed")
      b.position(b.position() + fixed)
      val payload = new Array[Byte](end - b.position())
      b.get(payload)
      events += Event(eventType, payload)
    }
    Container(
      collection,
      partition,
      segment,
      field,
      dataType,
      extras,
      events.result()
    )
  }

  /** Walks every row of a parquet payload, one row group at a time.
    *
    * The read goes through parquet's column readers, not its record assembly:
    * the column readers address a column by its descriptor and need nothing
    * from Hadoop's MapReduce classes, which record assembly's `ParquetReader`
    * pulls in.
    */
  def forEachRow(payload: Array[Byte])(f: ParquetRow => Unit): Unit = {
    val file =
      ParquetFileReader.open(new InMemoryInputFile(withColumnNames(payload)))
    try {
      val meta = file.getFooter.getFileMetaData
      val columns = meta.getSchema.getColumns.asScala.toIndexedSeq
      require(
        columns.forall(column =>
          column.getPath.length == 1 && column.getMaxRepetitionLevel == 0
        ),
        "Milvus binlog Parquet payload must have a flat, non-repeated schema"
      )
      val writerVersion =
        try VersionParser.parse(meta.getCreatedBy)
        catch { case NonFatal(_) => null }
      var pages = file.readNextRowGroup()
      while (pages != null) {
        val row = new ParquetRow(columns.map { column =>
          new ColumnReaderImpl(
            column,
            pages.getPageReader(column),
            NoConverter,
            writerVersion
          )
        })
        var remaining = pages.getRowCount
        while (remaining > 0) {
          f(row)
          row.advance()
          remaining -= 1
        }
        pages = file.readNextRowGroup()
      }
    } finally {
      file.close()
    }
  }

  /** Gives every unnamed column of a flat parquet file a name.
    *
    * parquet-mr identifies a column by its path of names, both in the schema
    * and in the row group's column chunks. The `_delta` files milvus-storage
    * writes for a V3 segment carry two columns with empty names, so parquet-mr
    * sees one path twice, reads one chunk and fails on the other. Naming the
    * columns in the footer, `column_0`, `column_1`, and so on, leaves the data
    * pages untouched and makes the file readable. A file whose columns are all
    * named is returned as is.
    */
  private def withColumnNames(payload: Array[Byte]): Array[Byte] = {
    require(payload.length >= 12, "Truncated Milvus binlog Parquet payload")
    require(
      Arrays.equals(payload.take(4), ParquetMagic) &&
        Arrays.equals(payload.takeRight(4), ParquetMagic),
      "Milvus binlog payload must have PAR1 Parquet headers"
    )
    val footerLength = ByteBuffer
      .wrap(payload, payload.length - 8, 4)
      .order(ByteOrder.LITTLE_ENDIAN)
      .getInt
    require(
      footerLength > 0 && footerLength <= payload.length - 12,
      s"Invalid Milvus binlog Parquet footer length $footerLength"
    )
    val footerStart = payload.length - 8 - footerLength
    val footer: ThriftFileMetaData = Util.readFileMetaData(
      new ByteArrayInputStream(payload, footerStart, footerLength)
    )
    val elements = footer.getSchema.asScala
    val leaves = elements.drop(1)
    if (leaves.forall(e => !e.getName.isEmpty)) return payload
    if (leaves.exists(e => e.isSetNum_children && e.getNum_children > 0)) {
      throw new IllegalStateException(
        "delete log parquet payload has unnamed columns in a nested schema"
      )
    }
    leaves.zipWithIndex.foreach { case (leaf, i) =>
      if (leaf.getName.isEmpty) leaf.setName(s"column_$i")
    }
    footer.getRow_groups.asScala.foreach { rowGroup =>
      rowGroup.getColumns.asScala.zipWithIndex.foreach { case (chunk, i) =>
        chunk.getMeta_data.setPath_in_schema(
          Collections.singletonList[String](leaves(i).getName)
        )
      }
    }
    val out = new ByteArrayOutputStream(payload.length)
    out.write(payload, 0, footerStart)
    val footerOut = new ByteArrayOutputStream(footerLength)
    Util.writeFileMetaData(footer, footerOut)
    footerOut.writeTo(out)
    out.write(
      ByteBuffer
        .allocate(4)
        .order(ByteOrder.LITTLE_ENDIAN)
        .putInt(footerOut.size())
        .array()
    )
    out.write(ParquetMagic)
    out.toByteArray
  }

  /** The current row of a row group, read column by column. */
  final class ParquetRow(readers: IndexedSeq[ColumnReaderImpl]) {
    def columnCount: Int = readers.size

    def isNull(column: Int): Boolean = {
      val reader = readers(column)
      reader.getCurrentDefinitionLevel < reader.getDescriptor.getMaxDefinitionLevel
    }

    def getLong(column: Int): Long = readers(column).getLong
    def getInt(column: Int): Int = readers(column).getInteger
    def getBytes(column: Int): Array[Byte] = readers(column).getBinary.getBytes
    def primitiveType(column: Int): PrimitiveTypeName =
      readers(column).getDescriptor.getPrimitiveType.getPrimitiveTypeName

    def getString(column: Int): String = {
      val reader = readers(column)
      reader.getDescriptor.getPrimitiveType.getPrimitiveTypeName match {
        case PrimitiveTypeName.BINARY |
            PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY =>
          reader.getBinary.toStringUsingUTF8
        case other =>
          throw new IllegalStateException(
            s"delete log column $column is $other, expected a string"
          )
      }
    }

    def advance(): Unit = readers.foreach(_.consume())
  }

  /** Column readers require a converter; the values are pulled directly. */
  private object NoConverter extends PrimitiveConverter

  private final class InMemoryInputFile(bytes: Array[Byte]) extends InputFile {
    override def getLength: Long = bytes.length.toLong

    override def newStream(): SeekableInputStream =
      new SeekableInputStream {
        private var pos = 0

        override def getPos: Long = pos.toLong

        override def seek(newPos: Long): Unit = {
          if (newPos < 0 || newPos > bytes.length) {
            throw new EOFException(
              s"invalid seek position $newPos for in-memory parquet payload of ${bytes.length} bytes"
            )
          }
          pos = newPos.toInt
        }

        override def read(): Int = {
          if (pos >= bytes.length) -1
          else {
            val value = bytes(pos) & 0xff
            pos += 1
            value
          }
        }

        override def read(b: Array[Byte], off: Int, len: Int): Int = {
          if (pos >= bytes.length) {
            -1
          } else {
            val toRead = math.min(len, bytes.length - pos)
            System.arraycopy(bytes, pos, b, off, toRead)
            pos += toRead
            toRead
          }
        }

        override def readFully(target: Array[Byte]): Unit =
          readFully(target, 0, target.length)

        override def read(target: ByteBuffer): Int = {
          if (pos >= bytes.length) {
            -1
          } else {
            val toRead = math.min(target.remaining(), bytes.length - pos)
            target.put(bytes, pos, toRead)
            pos += toRead
            toRead
          }
        }

        override def readFully(target: ByteBuffer): Unit = {
          val len = target.remaining()
          ensureAvailable(len)
          target.put(bytes, pos, len)
          pos += len
        }

        override def readFully(
            target: Array[Byte],
            start: Int,
            len: Int
        ): Unit = {
          ensureAvailable(len)
          System.arraycopy(bytes, pos, target, start, len)
          pos += len
        }

        private def ensureAvailable(len: Int): Unit = {
          if (pos + len > bytes.length) {
            throw new EOFException(
              s"unexpected EOF while reading in-memory parquet payload: need $len bytes at offset $pos, payload size=${bytes.length}"
            )
          }
        }
      }
  }
}
