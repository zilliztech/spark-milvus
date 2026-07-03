package com.zilliz.spark.connector.read

import java.io.EOFException
import java.nio.ByteBuffer
import java.nio.ByteOrder
import scala.collection.mutable
import scala.util.control.NonFatal

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.example.data.Group
import org.apache.parquet.hadoop.api.ReadSupport
import org.apache.parquet.hadoop.example.GroupReadSupport
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.io.{InputFile, SeekableInputStream}
import org.apache.spark.internal.Logging

import io.milvus.grpc.schema.{CollectionSchema, DataType, FieldSchema}

object MilvusDeltaLogReader extends Logging {
  private val MagicNumber = 0xfffabc
  private val DescriptorEventType: Byte = 0
  private val DeleteEventType: Byte = 2
  private val EventTypeCount = 8
  private val BaseEventHeaderSize = 17
  private val DescriptorEventDataFixPartSize = 52
  private val DeleteEventDataFixPartSize = 16
  private val MultiFieldVersion = "MULTI_FIELD"

  private val mapper = new ObjectMapper()

  def loadDeletePlansBySegment(
      segments: Seq[V2SegmentInfo],
      milvusSchema: CollectionSchema,
      bucket: String,
      hadoopConf: Configuration
  ): Either[Throwable, Map[Long, MilvusDeletePlan]] = {
    val pkField = primaryKeyField(milvusSchema)
    val deleteOnlySegments = segments.filter(_.columnGroups.isEmpty)
    val dataSegments = segments.filter(_.columnGroups.nonEmpty)

    for {
      globalPlan <- loadDeletePlan(
        deleteOnlySegments.flatMap(_.deltaLogs),
        pkField,
        bucket,
        hadoopConf
      )
      perSegment <- sequence(
        dataSegments.map { seg =>
          loadDeletePlan(seg.deltaLogs, pkField, bucket, hadoopConf).map {
            ownPlan =>
              seg.segmentId -> MilvusDeletePlan.union(globalPlan, ownPlan)
          }
        }
      )
    } yield perSegment.toMap
  }

  def loadDeletePlan(
      deltaLogs: Seq[V2DeltaLogFile],
      pkField: FieldSchema,
      bucket: String,
      hadoopConf: Configuration
  ): Either[Throwable, MilvusDeletePlan] = {
    try {
      validatePkType(pkField)
      val plans = deltaLogs.map { log =>
        val fullyQualifiedPath =
          V2SegmentLoader.resolvePath(log.logPath, bucket)
        val bytes = V2SegmentLoader.readAllBytes(hadoopConf, fullyQualifiedPath)
        decodeDeletePlan(bytes, pkField, fullyQualifiedPath)
      }
      sequence(plans).map(MilvusDeletePlan.union)
    } catch {
      case NonFatal(e) => Left(e)
    }
  }

  private def decodeDeletePlan(
      bytes: Array[Byte],
      pkField: FieldSchema,
      path: String
  ): Either[Throwable, MilvusDeletePlan] = {
    try {
      val container = parseContainer(bytes, path)
      val plans = container.payloads.map { payload =>
        decodePayloadPlan(payload, container.multiField, pkField, path)
      }
      Right(MilvusDeletePlan.union(plans))
    } catch {
      case NonFatal(e) => Left(e)
    }
  }

  private def decodePayloadPlan(
      payload: Array[Byte],
      multiField: Boolean,
      pkField: FieldSchema,
      path: String
  ): MilvusDeletePlan = {
    val longs = mutable.HashSet.empty[Long]
    val strings = mutable.HashSet.empty[String]
    val reader = newGroupReader(payload)
    try {
      var record = reader.read()
      while (record != null) {
        if (multiField) {
          appendMultiFieldDelete(record, pkField, path, longs, strings)
        } else {
          appendLegacyDelete(
            extractLegacyDelete(record, path),
            pkField,
            path,
            longs,
            strings
          )
        }
        record = reader.read()
      }
    } finally {
      reader.close()
    }

    pkField.dataType match {
      case DataType.Int64   => MilvusDeletePlan.fromLongPks(longs.toSet)
      case DataType.VarChar => MilvusDeletePlan.fromStringPks(strings.toSet)
      case other =>
        throw new IllegalArgumentException(
          s"unsupported primary key type $other for delete logs"
        )
    }
  }

  private def appendMultiFieldDelete(
      record: Group,
      pkField: FieldSchema,
      path: String,
      longs: mutable.Set[Long],
      strings: mutable.Set[String]
  ): Unit = {
    val fieldCount = record.getType.getFieldCount
    if (fieldCount < 2) {
      throw new IllegalStateException(
        s"multi-field delete log payload in $path must contain pk and ts columns, found $fieldCount columns"
      )
    }
    if (record.getFieldRepetitionCount(0) == 0) {
      throw new IllegalStateException(
        s"multi-field delete log payload in $path is missing the pk value"
      )
    }

    pkField.dataType match {
      case DataType.Int64   => longs += record.getLong(0, 0)
      case DataType.VarChar => strings += record.getString(0, 0)
      case other =>
        throw new IllegalArgumentException(
          s"unsupported primary key type $other for delete logs"
        )
    }
  }

  private def extractLegacyDelete(record: Group, path: String): String = {
    val fieldCount = record.getType.getFieldCount
    if (fieldCount < 1) {
      throw new IllegalStateException(
        s"legacy delete log payload in $path is missing the delta column"
      )
    }
    if (record.getFieldRepetitionCount(0) == 0) {
      throw new IllegalStateException(
        s"legacy delete log payload in $path has an empty delta value"
      )
    }
    record.getString(0, 0)
  }

  private def newGroupReader(payload: Array[Byte]): ParquetReader[Group] =
    new ParquetReader.Builder[Group](new InMemoryInputFile(payload)) {
      override protected def getReadSupport(): ReadSupport[Group] =
        new GroupReadSupport()
    }.build()

  private def appendLegacyDelete(
      raw: String,
      pkField: FieldSchema,
      path: String,
      longs: mutable.Set[Long],
      strings: mutable.Set[String]
  ): Unit = {
    val trimmed = raw.trim
    if (trimmed.startsWith("{")) {
      val json = mapper.readTree(trimmed)
      val pkType = json.path("pkType").asInt(Int.MinValue)
      val pkNode = json.path("pk")
      if (pkType == Int.MinValue || pkNode.isMissingNode) {
        throw new IllegalStateException(
          s"invalid legacy delete log JSON in $path: $trimmed"
        )
      }
      val expectedPkType = pkField.dataType match {
        case DataType.Int64   => DataType.Int64.value
        case DataType.VarChar => DataType.VarChar.value
        case other =>
          throw new IllegalArgumentException(
            s"unsupported primary key type $other for delete logs"
          )
      }
      if (pkType != expectedPkType) {
        throw new IllegalStateException(
          s"delete log pkType $pkType in $path does not match collection PK type ${pkField.dataType.value}"
        )
      }
      pkField.dataType match {
        case DataType.Int64 =>
          if (!pkNode.isNumber) {
            throw new IllegalStateException(
              s"delete log pk in $path must be numeric for Int64 PKs: $trimmed"
            )
          }
          longs += pkNode.longValue()
        case DataType.VarChar =>
          if (!pkNode.isTextual) {
            throw new IllegalStateException(
              s"delete log pk in $path must be textual for VarChar PKs: $trimmed"
            )
          }
          strings += pkNode.textValue()
        case _ =>
      }
    } else {
      val parts = trimmed.split(",", 2)
      if (parts.length != 2) {
        throw new IllegalStateException(
          s"invalid legacy delete log payload in $path: $trimmed"
        )
      }
      if (pkField.dataType != DataType.Int64) {
        throw new IllegalStateException(
          s"legacy 'pk,ts' delete log payload in $path only supports Int64 PKs"
        )
      }
      longs += parts(0).trim.toLong
    }
  }

  private def parseContainer(
      bytes: Array[Byte],
      path: String
  ): ParsedContainer = {
    val buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
    val magic = buf.getInt()
    if (magic != MagicNumber) {
      throw new IllegalStateException(
        f"invalid deltalog magic number in $path: expected 0x$MagicNumber%x got 0x$magic%x"
      )
    }

    val descriptorHeader = readHeader(buf, s"descriptor header in $path")
    if (descriptorHeader.typeCode != DescriptorEventType) {
      throw new IllegalStateException(
        s"expected descriptor event at start of $path, got type ${descriptorHeader.typeCode}"
      )
    }

    skipFully(
      buf,
      DescriptorEventDataFixPartSize,
      s"descriptor fix part in $path"
    )
    val postHeaderLengths = new Array[Int](EventTypeCount)
    var idx = 0
    while (idx < EventTypeCount) {
      ensureRemaining(buf, 1, s"descriptor post-header lengths in $path")
      postHeaderLengths(idx) = buf.get() & 0xff
      idx += 1
    }
    ensureRemaining(buf, 4, s"descriptor extras length in $path")
    val extraLength = buf.getInt()
    if (extraLength < 0) {
      throw new IllegalStateException(
        s"negative descriptor extras length $extraLength in $path"
      )
    }
    ensureRemaining(buf, extraLength, s"descriptor extras in $path")
    val extraBytes = new Array[Byte](extraLength)
    buf.get(extraBytes)
    val extras =
      if (extraBytes.isEmpty) mapper.createObjectNode()
      else mapper.readTree(extraBytes)
    val multiField =
      Option(extras.get("version")).exists(_.asText() == MultiFieldVersion)

    val payloads = mutable.ArrayBuffer.empty[Array[Byte]]
    while (buf.hasRemaining) {
      val header = readHeader(buf, s"event header in $path")
      val fixPartSize =
        if (header.typeCode >= 0 && header.typeCode < postHeaderLengths.length)
          postHeaderLengths(header.typeCode)
        else 0
      if (fixPartSize < 0) {
        throw new IllegalStateException(
          s"negative event fix-part size $fixPartSize in $path"
        )
      }
      ensureRemaining(buf, fixPartSize, s"event fix part in $path")
      skipFully(buf, fixPartSize, s"event fix part in $path")
      val payloadLength = header.eventLength - BaseEventHeaderSize - fixPartSize
      if (payloadLength < 0) {
        throw new IllegalStateException(
          s"negative event payload length $payloadLength in $path"
        )
      }
      ensureRemaining(buf, payloadLength, s"event payload in $path")
      val payload = new Array[Byte](payloadLength)
      buf.get(payload)
      if (header.typeCode == DeleteEventType && payload.nonEmpty) {
        payloads += payload
      }
    }

    ParsedContainer(multiField = multiField, payloads = payloads.toSeq)
  }

  private def readHeader(buf: ByteBuffer, context: String): ParsedHeader = {
    ensureRemaining(buf, BaseEventHeaderSize, context)
    buf.getLong()
    val typeCode = buf.get()
    val eventLength = buf.getInt()
    buf.getInt()
    ParsedHeader(typeCode, eventLength)
  }

  private def primaryKeyField(milvusSchema: CollectionSchema): FieldSchema = {
    val pkField = milvusSchema.fields.find(_.isPrimaryKey).getOrElse {
      throw new IllegalArgumentException("No primary key field found in schema")
    }
    validatePkType(pkField)
    pkField
  }

  private def validatePkType(pkField: FieldSchema): Unit = {
    pkField.dataType match {
      case DataType.Int64 | DataType.VarChar =>
      case other =>
        throw new IllegalArgumentException(
          s"StorageV2 delete logs only support Int64/VarChar PKs, got $other"
        )
    }
  }

  private def asLong(value: Any, context: String): Long = value match {
    case n: java.lang.Long    => n.longValue()
    case n: java.lang.Integer => n.longValue()
    case n: java.lang.Short   => n.longValue()
    case n: java.lang.Byte    => n.longValue()
    case n: java.lang.Number  => n.longValue()
    case other =>
      throw new IllegalStateException(s"expected numeric $context, got $other")
  }

  private def ensureRemaining(
      buf: ByteBuffer,
      needed: Int,
      context: String
  ): Unit = {
    if (buf.remaining() < needed) {
      throw new EOFException(
        s"unexpected EOF while reading $context: need $needed bytes, only ${buf.remaining()} remain"
      )
    }
  }

  private def skipFully(buf: ByteBuffer, length: Int, context: String): Unit = {
    ensureRemaining(buf, length, context)
    buf.position(buf.position() + length)
  }

  private def sequence[A](
      items: Seq[Either[Throwable, A]]
  ): Either[Throwable, Seq[A]] = {
    val out = mutable.ArrayBuffer.empty[A]
    items.foreach {
      case Right(value) => out += value
      case Left(err)    => return Left(err)
    }
    Right(out.toSeq)
  }

  private final case class ParsedHeader(typeCode: Byte, eventLength: Int)
  private final case class ParsedContainer(
      multiField: Boolean,
      payloads: Seq[Array[Byte]]
  )

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
