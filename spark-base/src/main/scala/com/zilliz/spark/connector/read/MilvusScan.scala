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
import com.zilliz.milvus.storage.delete.MilvusDeltaLogReader
import com.zilliz.milvus.storage.schema.FieldMetadata
import com.zilliz.milvus.storage.snapshot.MilvusSnapshotReader
import com.zilliz.spark.connector.options.{ReadMode, StorageOptions}
import com.zilliz.spark.connector.options.MilvusOption
import com.zilliz.spark.connector.read.{
  MilvusInputPartition,
  MilvusPackedV2DeleteContext,
  MilvusPartitionReaderFactory
}
import io.milvus.grpc.schema.CollectionSchema
import com.zilliz.spark.connector.read.plan.{ScanContext, ClientSnapshotPlanner, LegacyClientPlanner, OptionSnapshotPlanner, BackupPlanner}

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

  ctx.vectorSearchConfig.foreach { config =>
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

  private[read] def shouldCacheInputPartitions: Boolean =
    readMode != ReadMode.Client ||
      ClientSnapshotPlanner.canUseClientSnapshotFastPath(milvusOption)

  private def computeInputPartitions(): Array[InputPartition] =
    readMode match {
      case ReadMode.Snapshot => new OptionSnapshotPlanner(ctx).plan()
      case ReadMode.Backup =>
        new BackupPlanner(ctx, preParsedBackupMeta).plan()
      case ReadMode.Client => planFromClient()
    }

  /** Client mode: the snapshot fast path when nothing rules it out, otherwise
    * the legacy segment listing.
    */
  private def planFromClient(): Array[InputPartition] = {
    if (milvusOption.collectionName.isEmpty) {
      throw new IllegalArgumentException("collectionName cannot be empty")
    }

    val client = MilvusClient(milvusOption.connectionParams)
    try {
      val clientSnapshotPartitions =
        if (ClientSnapshotPlanner.canUseClientSnapshotFastPath(milvusOption)) {
          new ClientSnapshotPlanner(ctx).plan(client)
        } else {
          logInfo(
            "client snapshot fast path disabled because partition/segment selector is set"
          )
          None
        }

      clientSnapshotPartitions.getOrElse {
        val collectionInfo = client
          .getCollectionInfo(
            milvusOption.databaseName,
            milvusOption.collectionName
          )
          .getOrElse(
            throw new Exception(
              s"Collection ${milvusOption.collectionName} not found"
            )
          )
        new LegacyClientPlanner(ctx).plan(client, collectionInfo)
      }
    } finally {
      client.close()
    }
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    val optionsMap = options.asScala.toMap
    val inheritedPlansByPartition =
      if (
        readMode == ReadMode.Snapshot && MilvusOption.readApplyDeletes(options)
      ) {
        val schemaBytes = Option(options.get(MilvusOption.SnapshotSchemaBytes))
          .map(base64 => java.util.Base64.getDecoder.decode(base64))
          .getOrElse(Array.emptyByteArray)
        val v2Segments = Option(options.get(MilvusOption.SnapshotV2Segments))
          .filter(_.nonEmpty)
          .map(json =>
            MilvusSnapshotReader.deserializeV2Segments(json) match {
              case Right(segs) => segs
              case Left(err) =>
                throw new IllegalStateException(
                  s"Failed to parse SnapshotV2Segments in reader factory: ${err.getMessage}",
                  err
                )
            }
          )
          .getOrElse(Seq.empty)
        val inheritedDeleteSegments =
          v2Segments.filter(seg =>
            seg.columnGroups.isEmpty && seg.deltaLogs.nonEmpty
          )
        if (schemaBytes.isEmpty || inheritedDeleteSegments.isEmpty) {
          Map.empty[Long, com.zilliz.milvus.storage.delete.MilvusDeletePlan]
        } else {
          val pkField = CollectionSchema
            .parseFrom(schemaBytes)
            .fields
            .find(_.isPrimaryKey)
            .getOrElse(
              throw new IllegalArgumentException(
                "No primary key field found in schema"
              )
            )
          MilvusDeltaLogReader.loadPartitionScopedDeletePlans(
            inheritedDeleteSegments,
            pkField,
            StorageOptions.connectorS3BucketOption(optionsMap).getOrElse(""),
            StorageOptions.storeFor(
              ctx.hadoopConf(""),
              StorageOptions.connectorS3BucketOption(optionsMap).getOrElse(""),
              optionsMap
            )
          ) match {
            case Right(plans) => plans
            case Left(err) =>
              throw new IllegalStateException(
                s"Failed to load inherited StorageV2 delete logs for reader factory: ${err.getMessage}",
                err
              )
          }
        }
      } else if (
        readMode == ReadMode.Backup && MilvusOption.readApplyDeletes(options)
      ) {
        // Computed here (not read from a planning side effect) so delete
        // handling does not depend on Spark evaluating partitions first.
        new BackupPlanner(ctx, preParsedBackupMeta).inheritedDeletePlans()
      } else {
        Map.empty[Long, com.zilliz.milvus.storage.delete.MilvusDeletePlan]
      }

    new MilvusPartitionReaderFactory(
      schema,
      optionsMap,
      pushedFilters,
      MilvusPackedV2DeleteContext(inheritedPlansByPartition),
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
    val specs = partitions.collect { case p: MilvusInputPartition => p.spec }
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
