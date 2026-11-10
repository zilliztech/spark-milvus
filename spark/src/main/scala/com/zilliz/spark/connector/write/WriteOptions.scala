package com.zilliz.spark.connector.write

import java.util.{Base64, Locale}

import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.milvus.storage.snapshot.json.SnapshotJson
import com.zilliz.spark.connector.options.{OptionParsing, StorageOptions}
import io.milvus.grpc.schema.CollectionSchema

/** Only immutable write execution settings cross the Spark task boundary. */
final case class WriteOptions(
    batchRows: Int = 5000,
    fileRollingBytes: Long = 2L * 1024L * 1024L * 1024L,
    variableWidthBytesPerValue: Double = 32.0,
    customPath: Option[String] = None,
    replaceColumns: Boolean = false,
    database: String = "default",
    collection: Option[String] = None
) extends Serializable {
  require(batchRows > 0, "batchRows must be positive")
  require(fileRollingBytes > 0, "fileRollingBytes must be positive")
  require(
    variableWidthBytesPerValue > 0 && java.lang.Double
      .isFinite(variableWidthBytesPerValue),
    "variableWidthBytesPerValue must be finite and positive"
  )

  def writerProperties(storage: Map[String, String]): Map[String, String] =
    storage + ("writer.file_rolling.size" -> fileRollingBytes.toString)
}
