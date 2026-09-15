package com.zilliz.spark.connector.read

import scala.jdk.CollectionConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.{
  Batch,
  InputPartition,
  PartitionReaderFactory,
  Scan,
  Statistics,
  SupportsReportStatistics
}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import com.zilliz.milvus.client.api.MilvusClient
import com.zilliz.milvus.storage.compat.backup.BackupMetaReader
import com.zilliz.milvus.storage.schema.FieldMetadata
import com.zilliz.spark.connector.options.{ReadMode, StorageOptions}
import com.zilliz.spark.connector.options.MilvusOption
import com.zilliz.spark.connector.read.{
  MilvusInputPartition,
  V2InheritedDeletes,
  MilvusPartitionReaderFactory
}
import com.zilliz.spark.connector.read.plan.{
  BackupPlanner,
  ClientSnapshotPlanner,
  DeletePlanning,
  OptionSnapshotPlanner,
  ScanContext
}

class MilvusScan(
    schema: StructType,
    options: CaseInsensitiveStringMap,
    pushedFilters: Array[Filter] = Array.empty[Filter],
    // Backup meta already parsed at table init (threaded directly, never via
    // options, so it does not ride along on InputPartitions to executors).
    preParsedBackupMeta: Option[BackupMetaReader.BackupInfo] = None,
    private[read] val pushedLimit: Option[Int] = None
) extends Scan
    with Batch
    with SupportsReportStatistics
    with Logging {
  private val milvusOption = MilvusOption(options)
  private[read] val ctx = new ScanContext(options, milvusOption)
  private val readMode: ReadMode = MilvusOption.readMode(options)

  /** Row count is the sum over planned partitions; the byte size is that count
    * times an estimated row width. Both come from the plan already built, so
    * this opens nothing. Spark uses them to pick a join strategy, which is what
    * makes a broadcast join possible for a small collection.
    */
  override def estimateStatistics(): Statistics =
    MilvusScan.statisticsFor(planInputPartitions(), schema)

  ctx.vectorSearch.foreach { config =>
    logInfo(
      s"Vector search enabled: topK=${config.topK}, metric=${config.metricType}, column=${config.vectorColumn}"
    )
  }

  override def readSchema(): StructType = {
    schema
  }

  override def toBatch: Batch = this

  /** Whether Spark asks this scan for batches or for rows.
    *
    * PARTITION_DEFINED so the factory decides per partition, which is where the
    * layout is known. It answers false unless `milvus.read.columnar` is on: the
    * row path is what every existing job runs, and the two have to be shown to
    * agree on real data before the default moves.
    */
  override def columnarSupportMode(): Scan.ColumnarSupportMode =
    if (MilvusOption.readColumnar(options)) {
      Scan.ColumnarSupportMode.PARTITION_DEFINED
    } else {
      Scan.ColumnarSupportMode.UNSUPPORTED
    }

  private lazy val plannedSnapshotPartitions: Array[InputPartition] =
    computeInputPartitions()

  override def planInputPartitions(): Array[InputPartition] = {
    if (shouldCacheInputPartitions) plannedSnapshotPartitions
    else computeInputPartitions()
  }

  /** Every mode reads one fixed snapshot, so the plan is computed once. */
  private[read] def shouldCacheInputPartitions: Boolean = true

  private def computeInputPartitions(): Array[InputPartition] =
    readMode match {
      case ReadMode.Snapshot => new OptionSnapshotPlanner(ctx).plan()
      case ReadMode.Backup =>
        new BackupPlanner(ctx, preParsedBackupMeta).plan()
      case ReadMode.Client => planFromClient()
    }

  /** Client mode: the service names the collection id, the snapshot comes
    * from the snapshot directory.
    */
  private def planFromClient(): Array[InputPartition] = {
    if (milvusOption.collectionName.isEmpty) {
      throw new IllegalArgumentException("collectionName cannot be empty")
    }
    val client = MilvusClient(milvusOption.connectionParams)
    try new ClientSnapshotPlanner(ctx).plan(client)
    finally client.close()
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    val optionsMap = options.asScala.toMap
    val inheritedPlansByPartition =
      if (
        readMode == ReadMode.Snapshot && MilvusOption.readApplyDeletes(options)
      ) {
        // Computed here (not read from a planning side effect) so delete
        // handling does not depend on Spark evaluating partitions first.
        val snapshot = OptionSnapshotPlanner.snapshotFor(ctx)
        DeletePlanning.loadInheritedDeletePlans(
          ctx,
          snapshot,
          StorageOptions.connectorS3BucketOption(optionsMap),
          ctx.hadoopConf(""),
          errorContext = "reader factory"
        )
      } else if (
        readMode == ReadMode.Backup && MilvusOption.readApplyDeletes(options)
      ) {
        // Computed here (not read from a planning side effect) so delete
        // handling does not depend on Spark evaluating partitions first.
        new BackupPlanner(ctx, preParsedBackupMeta).inheritedDeletePlans()
      } else {
        Map.empty[Long, com.zilliz.milvus.storage.delete.DeletePlan]
      }

    new MilvusPartitionReaderFactory(
      schema,
      optionsMap,
      pushedFilters,
      V2InheritedDeletes(inheritedPlansByPartition),
      pushedLimit
    )
  }
}

object MilvusScan extends Logging {

  /** Table statistics from a plan. Pure, so it is testable without storage.
    *
    * `numRows` is known only when every partition knows its own count; one
    * unknown makes the total unknown rather than an undercount Spark would
    * trust. `sizeInBytes` is rows times [[estimatedRowWidth]].
    */
  private[read] def statisticsFor(
      partitions: Array[InputPartition],
      schema: StructType
  ): Statistics = {
    val specs = partitions.collect { case p: MilvusInputPartition => p.task }
    val rows =
      com.zilliz.milvus.storage.read.plan.ReadPlan(specs.toSeq).totalRows
    val width = estimatedRowWidth(schema)
    new Statistics {
      override def numRows(): java.util.OptionalLong =
        rows.fold(java.util.OptionalLong.empty())(java.util.OptionalLong.of)
      override def sizeInBytes(): java.util.OptionalLong =
        rows.fold(java.util.OptionalLong.empty())(r =>
          java.util.OptionalLong.of(r * width)
        )
    }
  }

  /** Bytes per row of `schema`, as stored.
    *
    * A vector field is presented as an array but stored as a fixed-width blob,
    * so its width is dimension times element width from the field metadata;
    * Spark's own `defaultSize` for an array assumes one element and would put a
    * 768-dimensional column at 4 bytes. Everything else takes `defaultSize`.
    */
  private[read] def estimatedRowWidth(schema: StructType): Long =
    schema.fields.map { field =>
      val md = field.metadata
      if (md.contains(FieldMetadata.MilvusVectorDimensionMetadataKey)) {
        val dim = md.getLong(FieldMetadata.MilvusVectorDimensionMetadataKey)
        val kind =
          if (md.contains(FieldMetadata.MilvusDataTypeMetadataKey))
            md.getString(FieldMetadata.MilvusDataTypeMetadataKey)
          else ""
        kind match {
          case "Float16Vector" | "BFloat16Vector" => dim * 2
          case "Int8Vector"                       => dim
          case "BinaryVector"                     => (dim + 7) / 8
          case _                                  => dim * 4
        }
      } else field.dataType.defaultSize.toLong
    }.sum
}
