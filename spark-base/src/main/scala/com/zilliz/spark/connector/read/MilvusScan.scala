package com.zilliz.spark.connector.read

import java.util.OptionalLong
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.expressions.{Expressions, NamedReference}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.metric.CustomMetric
import org.apache.spark.sql.connector.read.{
  Batch,
  InputPartition,
  PartitionReaderFactory,
  Scan,
  Statistics,
  SupportsReportStatistics,
  SupportsRuntimeV2Filtering
}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.milvus.storage.expr.PredicateExpr
import com.zilliz.milvus.storage.io.ObjectStore
import com.zilliz.milvus.storage.read.plan.{DeleteFileListing, ReadPlan}
import com.zilliz.milvus.storage.schema.FieldMetadata
import com.zilliz.milvus.storage.snapshot.{
  SegmentLayout,
  Snapshot,
  SnapshotOrigin
}
import com.zilliz.milvus.storage.stats.{PrimaryKeyBloomPruner, PrimaryKeyFilter}
import com.zilliz.spark.connector.expr.SparkPredicateTranslator
import com.zilliz.spark.connector.metrics.ScanMetrics
import com.zilliz.spark.connector.options.{
  MilvusOption,
  SnapshotSources,
  StorageOptions
}
import io.milvus.grpc.schema.{DataType => MilvusDataType}

/** One read of one [[Snapshot]]: the table resolved it once, this plans one
  * partition per data segment and builds the reader factory from it. Driver
  * storage is opened only for manifest metadata, delete-file listings and
  * primary-key Bloom statistics.
  */
class MilvusScan(
    schema: StructType,
    options: CaseInsensitiveStringMap,
    private[read] val snapshot: Snapshot,
    pushedExpression: Option[PredicateExpr] = None,
    private[read] val pushedLimit: Option[Int] = None,
    private[read] val planningSchema: StructType = null
) extends Scan
    with Batch
    with SupportsReportStatistics
    with SupportsRuntimeV2Filtering
    with Logging {
  private val milvusOption = MilvusOption(options)
  private val predicateSchema = Option(planningSchema).getOrElse(schema)
  milvusOption.vectorSearch.filter(_.mode == "index").foreach { search =>
    SegmentIndexSearch.validate(search, snapshot.schema)
  }
  private val runtimePrimaryKey =
    if (milvusOption.vectorSearch.isEmpty)
      snapshot.primaryKeyField.filter(field =>
        field.dataType == MilvusDataType.Int64 ||
          field.dataType == MilvusDataType.VarChar
      )
    else None
  private val pushedPrimaryKeyFilter =
    for {
      primaryKey <- runtimePrimaryKey
      expression <- pushedExpression
      filter <- PrimaryKeyFilter.fromPredicate(expression, primaryKey)
    } yield filter

  @transient private var runtimePrimaryKeyFilter: Option[PrimaryKeyFilter] =
    None
  @transient private var cachedDeleteListing: DeleteFileListing = null
  @transient private var cachedBloomPruner: PrimaryKeyBloomPruner = null
  @transient private var plannedPartitions: Array[InputPartition] = null

  /** Row count is the sum over planned partitions; the byte size is that count
    * times an estimated row width. Both come from the plan already built, so
    * this opens nothing. Spark uses them to pick a join strategy, which is what
    * makes a broadcast join possible for a small collection.
    */
  override def estimateStatistics(): Statistics =
    MilvusScan.statisticsFor(planInputPartitions(), schema)

  override def readSchema(): StructType = schema

  override def toBatch: Batch = this

  override def supportedCustomMetrics(): Array[CustomMetric] =
    ScanMetrics.supported

  override def columnarSupportMode(): Scan.ColumnarSupportMode =
    // SUPPORTED makes Spark skip the reader factory's per-partition check.
    // The core evaluator consumes Arrow batches before either reader exposes
    // them, so an accepted predicate does not force row materialization.
    if (MilvusOption.readColumnar(options) && milvusOption.vectorSearch.isEmpty)
      Scan.ColumnarSupportMode.SUPPORTED
    else Scan.ColumnarSupportMode.UNSUPPORTED

  /** The primary key, when the scan reads it. Spark resolves these references
    * against the scan output, and Spark 4.2 does so whenever it plans the scan
    * (SPARK-56467), so a key pruned out of `readSchema()` cannot be offered. A
    * join on the key keeps it in the output.
    */
  override def filterAttributes(): Array[NamedReference] =
    runtimePrimaryKey
      .filter(field => schema.fieldNames.contains(field.name))
      .map(field => Array(Expressions.column(field.name)))
      .getOrElse(Array.empty[NamedReference])

  /** Spark can call this more than once as more join-side values become
    * available. Each accepted finite primary-key set narrows the prior set;
    * unsupported shapes leave the current plan unchanged.
    */
  override def filter(predicates: Array[Predicate]): Unit = {
    if (runtimePrimaryKey.isEmpty || predicates == null) return
    val primaryKey = runtimePrimaryKey.get
    val incoming = predicates.iterator
      .flatMap(predicate =>
        SparkPredicateTranslator
          .translate(predicate, predicateSchema)
          .flatMap(translated =>
            PrimaryKeyFilter.fromPredicate(translated.expr, primaryKey)
          )
      )
      .reduceLeftOption(_.intersect(_))
    incoming.foreach { accepted =>
      synchronized {
        val next = runtimePrimaryKeyFilter
          .map(_.intersect(accepted))
          .orElse(Some(accepted))
        if (next != runtimePrimaryKeyFilter) {
          runtimePrimaryKeyFilter = next
          plannedPartitions = null
        }
      }
    }
  }

  private[read] def currentPrimaryKeyFilter: Option[PrimaryKeyFilter] =
    effectivePrimaryKeyFilter

  private def effectivePrimaryKeyFilter: Option[PrimaryKeyFilter] =
    (pushedPrimaryKeyFilter, runtimePrimaryKeyFilter) match {
      case (Some(pushed), Some(runtime)) => Some(pushed.intersect(runtime))
      case (some @ Some(_), None)        => some
      case (None, some @ Some(_))        => some
      case _                             => None
    }

  /** Every plan uses one fixed snapshot. Runtime filters may replace the
    * partition array, while delete listing and Bloom inputs remain cached.
    */
  override def planInputPartitions(): Array[InputPartition] = synchronized {
    if (plannedPartitions == null) plannedPartitions = plan()
    plannedPartitions
  }

  private def bucket: Option[String] =
    Option(snapshot.bucket).map(_.trim).filter(_.nonEmpty)

  private[read] def openPlanningStore(): ObjectStore =
    StorageOptions.storeFor(
      hadoopConf,
      bucket.getOrElse(""),
      milvusOption.options
    )

  /** Hadoop configuration for objects under `path`, from the connector's `fs.*`
    * options plus per-bucket S3A settings.
    */
  private[read] def hadoopConfFor(path: String): Configuration =
    StorageOptions.buildHadoopConfForOptions(milvusOption.options, path)

  /** Per-bucket Hadoop settings are keyed by the location being read. */
  private def hadoopConf = snapshot.origin match {
    case SnapshotOrigin.Backup(dir) => hadoopConfFor(dir)
    case _                          => hadoopConfFor("")
  }

  private def errorContext: String = s"snapshot ${snapshot.name}"

  private def plan(): Array[InputPartition] = {
    logInfo(
      s"Planning ${snapshot.segments.size} segment(s) of ${snapshot.name} from ${snapshot.origin}"
    )
    // The delete files are listed here, which opens every V3 segment's
    // manifest; none of them is read here. The store is the driver's, rooted
    // at the snapshot's bucket.
    val applyDeletes = MilvusOption.readApplyDeletes(options)
    def listDeletes(
        store: ObjectStore
    ): DeleteFileListing =
      DeleteFileListing
        .of(
          snapshot,
          applyDeletes,
          bucket.getOrElse(""),
          store,
          StorageOptions.storeEndpoint(
            hadoopConf,
            bucket.getOrElse(""),
            milvusOption.options
          )
        )
        .fold(
          e =>
            throw new IllegalStateException(
              s"cannot list the delete files of $errorContext: ${e.getMessage}",
              e
            ),
          identity
        )
    val primaryKeyFilter = effectivePrimaryKeyFilter
    val needsBloomStore =
      primaryKeyFilter.exists(_.values.nonEmpty) && cachedBloomPruner == null
    val needsDeleteStore =
      cachedDeleteListing == null &&
        DeleteFileListing.requiresStore(snapshot, applyDeletes)

    // V2 delete metadata is already in the snapshot. Resolve and validate it
    // before opening a store only for Bloom statistics, so a delete-contract
    // failure cannot be mistaken for an optional pruning failure.
    if (cachedDeleteListing == null && !needsDeleteStore) {
      cachedDeleteListing = listDeletes(null)
    }

    def loadBloom(store: ObjectStore): Unit =
      if (needsBloomStore) {
        cachedBloomPruner = PrimaryKeyBloomPruner.load(
          snapshot,
          runtimePrimaryKey.get,
          Option(cachedDeleteListing)
            .map(_.v3ReadVersions)
            .getOrElse(Map.empty),
          bucket.getOrElse(""),
          StorageOptions.effectiveEndpoint(milvusOption.options).getOrElse(""),
          store
        )
      }

    if (needsDeleteStore || needsBloomStore) {
      try {
        SnapshotSources.withStore(
          openPlanningStore()
        ) { store =>
          if (needsDeleteStore)
            cachedDeleteListing = listDeletes(store)
          loadBloom(store)
        }
      } catch {
        case NonFatal(e) if !needsDeleteStore && needsBloomStore =>
          logWarning(
            s"Retaining all segments of $errorContext because its primary-key " +
              s"Bloom statistics cannot be loaded: ${e.getMessage}"
          )
          cachedBloomPruner = PrimaryKeyBloomPruner.unavailable(
            snapshot,
            runtimePrimaryKey.get
          )
      }
    }

    val plannedSnapshot = primaryKeyFilter match {
      case Some(filter) if filter.values.isEmpty =>
        snapshot.retainDataSegments(Set.empty)
      case Some(filter) => cachedBloomPruner.prune(filter)
      case None         => snapshot
    }
    inputPartitions(plannedSnapshot, cachedDeleteListing)
  }

  /** The plan, `core.read.plan.ReadPlan.of`, wrapped into Spark's input
    * partitions: each task becomes the partition of its storage line, with what
    * only this layer knows on it (the vector search, the partition name, the
    * option map the reader factory reads, the L0 marker a V2 reader resolves on
    * the executor).
    */
  private[read] def inputPartitions(
      snapshot: Snapshot,
      deletes: DeleteFileListing = DeleteFileListing.empty
  ): Array[InputPartition] = {
    // Every path in the snapshot is a key of `snapshot.bucket`, so that is the
    // bucket the native reader is rooted at, whatever the raw options say (a
    // backup read derives it from `milvus.backup.dir`).
    val canonicalMilvusOption = bucket
      .map(b =>
        milvusOption.copy(
          options = milvusOption.options ++
            Map(StorageProperties.BucketName -> b)
        )
      )
      .getOrElse(milvusOption)
    val planProperties = StorageOptions.storagePropertiesFor(
      hadoopConf,
      bucket.getOrElse(""),
      canonicalMilvusOption.options
    )
    // Parsed once for the whole plan rather than per partition: a bad storage
    // configuration should fail planning, not every task.
    val plan = ReadPlan.of(
      snapshot,
      properties = _ => planProperties,
      applyDeletes = MilvusOption.readApplyDeletes(options),
      deletes = deletes,
      neededFieldIds = MilvusOption.readerFieldIds(options),
      limits = milvusOption.readLimits
    )
    milvusOption.vectorSearch.filter(_.mode == "index").foreach { search =>
      SegmentIndexSearch.checkPlan(search, snapshot.schema, plan.specs)
    }
    plan.specs.map { task =>
      task.layout match {
        case SegmentLayout.Manifest(_, _) =>
          MilvusV3InputPartition(
            task,
            task.partitionId.toString,
            canonicalMilvusOption
          ): InputPartition
        case SegmentLayout.ColumnGroups(_) =>
          MilvusV2InputPartition(task, canonicalMilvusOption): InputPartition
      }
    }.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory =
    new MilvusPartitionReaderFactory(
      schema,
      options.asScala.toMap,
      pushedExpression,
      pushedLimit
    )
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
      ReadPlan(specs.toSeq).totalRows
    val width = estimatedRowWidth(schema)
    new Statistics {
      override def numRows(): OptionalLong =
        rows.fold(OptionalLong.empty())(OptionalLong.of)
      override def sizeInBytes(): OptionalLong =
        rows.fold(OptionalLong.empty())(r => OptionalLong.of(r * width))
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
          if (md.contains(FieldMetadata.MilvusDataTypeMetadataKey)) {
            val value = md.getLong(FieldMetadata.MilvusDataTypeMetadataKey)
            if (value >= Int.MinValue && value <= Int.MaxValue)
              Some(MilvusDataType.fromValue(value.toInt))
            else None
          } else None
        kind match {
          case Some(MilvusDataType.FloatVector) => dim * 4
          case Some(MilvusDataType.Float16Vector) |
              Some(MilvusDataType.BFloat16Vector) =>
            dim * 2
          case Some(MilvusDataType.Int8Vector)   => dim
          case Some(MilvusDataType.BinaryVector) => (dim + 7) / 8
          case _                                 => dim * 4
        }
      } else field.dataType.defaultSize.toLong
    }.sum
}
