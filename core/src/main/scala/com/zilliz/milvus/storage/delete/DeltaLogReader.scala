package com.zilliz.milvus.storage.delete

import java.nio.charset.StandardCharsets
import scala.collection.mutable
import scala.util.control.NonFatal

import com.zilliz.milvus.storage.codec.BinlogCodec.{forEachRow, ParquetRow}
import com.zilliz.milvus.storage.io.ObjectStore
import com.zilliz.milvus.storage.path.StoragePath
import com.zilliz.milvus.storage.snapshot.DeltaLogFile
import io.milvus.grpc.schema.{DataType, FieldSchema}

object DeltaLogReader extends com.zilliz.milvus.storage.Logging {

  /** Decodes a bare Parquet file containing primary-key and timestamp columns.
    * Input formats that wrap these bytes in another container must unwrap it
    * themselves. This entry point never interprets a legacy binlog envelope.
    */
  def decodeParquet(
      bytes: Array[Byte],
      pkField: FieldSchema,
      path: String
  ): Either[Throwable, DeletePlan] =
    try {
      validatePkType(pkField)
      require(
        isParquet(bytes),
        s"delete payload in $path must be bare Parquet (PAR1)"
      )
      Right(decodePayloadPlan(bytes, pkField, path))
    } catch {
      case NonFatal(e) => Left(e)
    }

  def loadDeletePlan(
      deltaLogs: Seq[DeltaLogFile],
      pkField: FieldSchema,
      bucket: String,
      store: ObjectStore
  ): Either[Throwable, DeletePlan] = {
    try {
      validatePkType(pkField)
      val plans = deltaLogs.map { log =>
        val at = StoragePath.parse(log.logPath, bucket)
        decodeParquet(store.readAll(at), pkField, at.key)
      }
      sequence(plans).map(DeletePlan.union)
    } catch {
      case NonFatal(e) => Left(e)
    }
  }

  private val ParquetMagic = "PAR1".getBytes(StandardCharsets.US_ASCII)

  private def isParquet(bytes: Array[Byte]): Boolean =
    bytes != null && bytes.length >= 4 && java.util.Arrays.equals(
      java.util.Arrays.copyOfRange(bytes, 0, 4),
      ParquetMagic
    )

  private def decodePayloadPlan(
      payload: Array[Byte],
      pkField: FieldSchema,
      path: String
  ): DeletePlan = {
    val longs = mutable.HashMap.empty[Long, Long]
    val strings = mutable.HashMap.empty[String, Long]
    forEachRow(payload) { row =>
      appendMultiFieldDelete(row, pkField, path, longs, strings)
    }

    pkField.dataType match {
      case DataType.Int64   => DeletePlan.fromLongPks(longs.toMap)
      case DataType.VarChar => DeletePlan.fromStringPks(strings.toMap)
      case other =>
        throw new IllegalArgumentException(
          s"unsupported primary key type $other for delete logs"
        )
    }
  }

  private def appendMultiFieldDelete(
      row: ParquetRow,
      pkField: FieldSchema,
      path: String,
      longs: mutable.Map[Long, Long],
      strings: mutable.Map[String, Long]
  ): Unit = {
    val fieldCount = row.columnCount
    if (fieldCount < 2) {
      throw new IllegalStateException(
        s"multi-field delete log payload in $path must contain pk and ts columns, found $fieldCount columns"
      )
    }
    if (row.isNull(0)) {
      throw new IllegalStateException(
        s"multi-field delete log payload in $path is missing the pk value"
      )
    }
    if (row.isNull(1)) {
      throw new IllegalStateException(
        s"multi-field delete log payload in $path is missing the ts value"
      )
    }
    val deleteTs = row.getLong(1)

    pkField.dataType match {
      case DataType.Int64 =>
        val pk = row.getLong(0)
        longs.update(pk, math.max(longs.getOrElse(pk, Long.MinValue), deleteTs))
      case DataType.VarChar =>
        val pk = row.getString(0)
        strings.update(
          pk,
          math.max(strings.getOrElse(pk, Long.MinValue), deleteTs)
        )
      case other =>
        throw new IllegalArgumentException(
          s"unsupported primary key type $other for delete logs"
        )
    }
  }

  private def validatePkType(pkField: FieldSchema): Unit = {
    pkField.dataType match {
      case DataType.Int64 | DataType.VarChar =>
      case other =>
        throw new IllegalArgumentException(
          s"delete logs only support Int64/VarChar PKs, got $other"
        )
    }
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

}
