package com.zilliz.spark.connector.write

import java.{util => ju}
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{
  SupportsRead,
  Table,
  TableCapability
}
import org.apache.spark.sql.connector.read.{
  Batch,
  InputPartition,
  PartitionReader,
  PartitionReaderFactory,
  Scan,
  ScanBuilder
}
import org.apache.spark.sql.types.{
  ArrayType,
  DataType,
  IntegerType,
  LongType,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String

import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.spark.connector.options.{MilvusOption, StorageOptions}

/** `milvus.read.mode=write_result`: the published receipt of one column
  * replacement as one row per committed segment. The rows are the receipt's own
  * small metadata, read once on the driver; they are not collection data.
  */
final class WriteResultTable(result: WriteResult)
    extends Table
    with SupportsRead {

  override def name(): String = s"write result ${result.operationId}"

  override def schema(): StructType = WriteResultTable.Schema

  override def capabilities(): ju.Set[TableCapability] =
    Set(TableCapability.BATCH_READ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    () =>
      new Scan with Batch {
        override def readSchema(): StructType = WriteResultTable.Schema
        override def toBatch: Batch = this
        override def planInputPartitions(): Array[InputPartition] =
          Array(WriteResultPartition(WriteResultTable.rows(result)))
        override def createReaderFactory(): PartitionReaderFactory =
          WriteResultReaderFactory
      }
}

object WriteResultTable {

  val Schema: StructType = StructType(
    Seq(
      StructField("operation_id", StringType, nullable = false),
      StructField("collection_id", LongType, nullable = false),
      StructField("schema_version", IntegerType, nullable = false),
      StructField("snapshot_id", LongType, nullable = false),
      StructField("snapshot_name", StringType, nullable = false),
      StructField("segment_id", LongType, nullable = false),
      StructField("partition_id", LongType, nullable = false),
      StructField("manifest_base_path", StringType, nullable = false),
      StructField("source_version", LongType, nullable = false),
      StructField("version", LongType, nullable = false),
      StructField("row_count", LongType, nullable = false),
      StructField(
        "changed_fields",
        ArrayType(StringType, containsNull = false),
        nullable = false
      ),
      StructField(
        "changed_field_ids",
        ArrayType(LongType, containsNull = false),
        nullable = false
      )
    )
  )

  /** The receipt published at the load path, read with the storage identity the
    * options and the session's Hadoop settings describe for it.
    */
  def load(
      options: CaseInsensitiveStringMap,
      userSchema: Option[StructType]
  ): WriteResultTable = {
    userSchema.filter(_.nonEmpty).foreach { schema =>
      if (!DataType.equalsIgnoreNullability(schema, Schema)) {
        throw new IllegalArgumentException(
          s"A write result has the fixed schema ${Schema.simpleString}; do not give it another one"
        )
      }
    }
    val path = Option(options.get("path"))
      .map(_.trim)
      .filter(_.nonEmpty)
      .getOrElse(
        throw new IllegalArgumentException(
          s"'${MilvusOption.ReadMode}=${MilvusOption.ReadModeWriteResult}' reads the receipt at a write " +
            "result address: .load(path)"
        )
      )
    val raw = options.asScala.toMap
    val bucket = StorageOptions
      .snapshotS3BucketForRelativePaths(path, raw)
      .getOrElse("")
    val storage = StorageOptions.storagePropertiesFor(
      StorageOptions.buildHadoopConfForOptions(raw, path),
      bucket,
      raw ++ (if (bucket.nonEmpty) Map(StorageProperties.BucketName -> bucket)
              else Map.empty[String, String])
    )
    new WriteResultTable(
      WriteResultRecords.readResult(WriteResultLocation.resolve(path, storage))
    )
  }

  private[write] def rows(result: WriteResult): Seq[WriteResultRow] =
    result.segments.map { committed =>
      val segment = committed.planned
      WriteResultRow(
        operationId = result.operationId,
        collectionId = result.snapshot.collectionId,
        schemaVersion = result.snapshot.schemaVersion,
        snapshotId = result.snapshot.id,
        snapshotName = result.snapshot.name,
        segmentId = segment.segmentId,
        partitionId = segment.partitionId,
        manifestBasePath = segment.manifestBasePath,
        sourceVersion = segment.sourceVersion,
        version = committed.version,
        rowCount = segment.rowCount,
        changedFields = result.changedFields.map(_.name),
        changedFieldIds = result.changedFields.map(_.fieldId)
      )
    }
}

/** One row of [[WriteResultTable.Schema]] as plain values, so that the
  * partition carrying it serializes without Spark's internal row classes.
  */
final case class WriteResultRow(
    operationId: String,
    collectionId: Long,
    schemaVersion: Int,
    snapshotId: Long,
    snapshotName: String,
    segmentId: Long,
    partitionId: Long,
    manifestBasePath: String,
    sourceVersion: Long,
    version: Long,
    rowCount: Long,
    changedFields: Seq[String],
    changedFieldIds: Seq[Long]
) {
  def toInternalRow: InternalRow = InternalRow(
    UTF8String.fromString(operationId),
    collectionId,
    schemaVersion,
    snapshotId,
    UTF8String.fromString(snapshotName),
    segmentId,
    partitionId,
    UTF8String.fromString(manifestBasePath),
    sourceVersion,
    version,
    rowCount,
    new GenericArrayData(changedFields.map(UTF8String.fromString).toArray[Any]),
    new GenericArrayData(changedFieldIds.toArray[Any])
  )
}

final case class WriteResultPartition(rows: Seq[WriteResultRow])
    extends InputPartition

private object WriteResultReaderFactory extends PartitionReaderFactory {
  override def createReader(
      partition: InputPartition
  ): PartitionReader[InternalRow] = {
    val rows = partition.asInstanceOf[WriteResultPartition].rows.iterator
    new PartitionReader[InternalRow] {
      private var row: InternalRow = _
      override def next(): Boolean =
        if (rows.hasNext) { row = rows.next().toInternalRow; true }
        else false
      override def get(): InternalRow = row
      override def close(): Unit = ()
    }
  }
}
