package com.zilliz.spark.connector.scan

import java.net.URI
import scala.jdk.CollectionConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.connector.read.InputPartition

import com.zilliz.milvus.client.api.MilvusClient
import com.zilliz.milvus.storage.compat.v2packed.V2SegmentLoader
import com.zilliz.milvus.storage.delete.MilvusDeltaLogReader
import com.zilliz.milvus.storage.snapshot.{
  MilvusSnapshotReader,
  SnapshotMetadata,
  StorageV2ManifestItem,
  V2SegmentInfo
}
import com.zilliz.spark.connector.loon.Properties
import com.zilliz.spark.connector.options.MilvusOption
import com.zilliz.spark.connector.options.StorageOptions
import io.milvus.grpc.schema.CollectionSchema

/** Client mode, fast path: `CreateSnapshot` on the service, then the snapshot
  * JSON it points at is read from object storage and planned like an option
  * snapshot. The snapshot is dropped when the Spark SQL execution ends.
  */
private[scan] final class ClientSnapshotPlanner(ctx: ScanContext)
    extends PartitionPlanner(ctx) {

  /** The partitions of a snapshot created on the service for this read, or
    * `None` when the fast path has to give way to the legacy segment listing.
    */
  def plan(client: MilvusClient): Option[Array[InputPartition]] = {
    val snapshotName = Option(options.get(MilvusOption.ClientSnapshotName))
      .filter(_.trim.nonEmpty)
      .getOrElse(
        ClientReadSnapshot.generatedClientSnapshotName(
          milvusOption.collectionName
        )
      )
    val description = Option(
      options.get(MilvusOption.ClientSnapshotDescription)
    ).filter(_.trim.nonEmpty).getOrElse("spark connector client read snapshot")
    val protectionSeconds =
      ClientReadSnapshot.parseClientSnapshotCompactionProtectionSeconds(options)

    val cleanupRegistration = ClientReadSnapshot.activeCleanupRegistration()
    if (cleanupRegistration.isEmpty) {
      logWarning(
        "Skipping client snapshot fast path because Spark SQL execution cleanup cannot be registered; " +
          "falling back to legacy GetPersistentSegmentInfo read path"
      )
      return None
    }

    client.createSnapshotForRead(
      milvusOption.databaseName,
      milvusOption.collectionName,
      snapshotName,
      description,
      protectionSeconds
    ) match {
      case scala.util.Success(snapshot) =>
        val connectorBucket = StorageOptions.connectorS3BucketOption(
          milvusOption.options
        )
        if (
          connectorBucket.isEmpty && StorageOptions
            .isBucketRelativeSnapshotLocation(snapshot.s3Location)
        ) {
          logWarning(
            s"Skipping client snapshot fast path because ${Properties.FsConfig.FsBucketName} is missing " +
              "and the snapshot location is bucket-relative; falling back to legacy GetPersistentSegmentInfo read path"
          )
          ClientReadSnapshot.submitClientSnapshotCleanup(
            options.asScala.toMap,
            milvusOption.databaseName,
            milvusOption.collectionName,
            snapshot.name,
            s"missing ${Properties.FsConfig.FsBucketName} for bucket-relative snapshot location"
          )
          None
        } else if (
          ClientReadSnapshot.registerClientSnapshotCleanup(
            cleanupRegistration.get,
            options.asScala.toMap,
            milvusOption.databaseName,
            milvusOption.collectionName,
            snapshot.name,
            autoCleanup = MilvusOption.clientSnapshotAutoCleanup(options)
          )
        ) {
          if (MilvusOption.clientSnapshotAutoCleanup(options)) {
            logWarning(
              s"Client read snapshot ${snapshot.name} will be dropped when the Spark SQL execution ends; " +
                "an unclean driver exit can leave it behind and require manual cleanup."
            )
          }
          val snapshotPath = StorageOptions.resolveClientSnapshotLocation(
            snapshot.s3Location,
            connectorBucket.getOrElse("")
          )
          Some(
            planFromSnapshotPath(
              snapshotPath,
              forceCanonicalBucket = connectorBucket
            )
          )
        } else {
          ClientReadSnapshot.submitClientSnapshotCleanup(
            options.asScala.toMap,
            milvusOption.databaseName,
            milvusOption.collectionName,
            snapshot.name,
            "cleanup registration failure"
          )
          None
        }

      case scala.util.Failure(e) if MilvusClient.isServiceNotImplemented(e) =>
        logWarning(
          "CreateSnapshot/DescribeSnapshot is not implemented by this Milvus service; " +
            "falling back to legacy GetPersistentSegmentInfo read path"
        )
        None

      case scala.util.Failure(e) =>
        throw new RuntimeException(
          s"Failed to create client read snapshot for collection ${milvusOption.collectionName}: ${e.getMessage}",
          e
        )
    }
  }

  private def planFromSnapshotPath(
      snapshotPath: String,
      forceCanonicalBucket: Option[String]
  ): Array[InputPartition] = {
    val hadoopConf = ctx.hadoopConf(snapshotPath)
    val snapshotJson = readAllBytes(hadoopConf, snapshotPath)
    val metadata =
      ClientSnapshotPlanner.validateClientSnapshotMetadata(
        MilvusSnapshotReader.parseSnapshotMetadata(snapshotJson) match {
          case Right(value) => value
          case Left(err) =>
            throw new IllegalArgumentException(
              s"Failed to parse client-created snapshot metadata: ${err.getMessage}",
              err
            )
        },
        snapshotPath
      )

    val snapshotBucket = forceCanonicalBucket
      .map(_.trim)
      .filter(_.nonEmpty)
      .orElse(
        StorageOptions
          .snapshotS3BucketForRelativePaths(snapshotPath, milvusOption.options)
      )
    val applyDeletes = MilvusOption.readApplyDeletes(options)
    val v2Segments =
      if (metadata.manifestList.nonEmpty) {
        V2SegmentLoader.loadV2Segments(
          metadata.manifestList,
          snapshotBucket.getOrElse(""),
          StorageOptions.storeFor(
            hadoopConf,
            snapshotBucket.getOrElse(""),
            milvusOption.options
          ),
          manifestSchemaVersion = metadata.manifestSchemaVersion,
          applyDeletes = applyDeletes
        ) match {
          case Right(segs) => segs
          case Left(err) =>
            throw new IllegalStateException(
              s"Failed to load StorageV2 segments from client-created snapshot: ${err.getMessage}",
              err
            )
        }
      } else Seq.empty
    val storageV2ManifestList =
      metadata.storageV2ManifestList.getOrElse(Seq.empty)
    ClientSnapshotPlanner.ensureClientSnapshotHasPackedSegments(
      storageV2ManifestList,
      v2Segments,
      metadata.collection.schema.name
    )
    ClientSnapshotPlanner.validateSnapshotBucketForRelativeDataPaths(
      snapshotPath,
      StorageOptions.connectorS3BucketOption(milvusOption.options),
      storageV2ManifestList,
      v2Segments
    )

    val schemaBytes = MilvusSnapshotReader.toProtobufSchemaBytes(
      metadata.collection.schema
    )
    val v3DeletePlanning =
      DeletePlanning.loadV3DeletePlanning(
        ctx,
        storageV2ManifestList,
        schemaBytes,
        snapshotBucket,
        hadoopConf,
        errorContext = "client-created snapshot"
      )
    val v2DeletePlans =
      DeletePlanning.loadV2DeletePlans(
        ctx,
        v2Segments,
        schemaBytes,
        snapshotBucket,
        hadoopConf,
        errorContext = "client-created snapshot"
      )

    val inheritedDeleteSegments =
      v2Segments.filter(seg =>
        seg.columnGroups.isEmpty && seg.deltaLogs.nonEmpty
      )
    val inheritedDeletePlansByPartition =
      if (
        !MilvusOption.readApplyDeletes(
          options
        ) || inheritedDeleteSegments.isEmpty
      ) {
        Map.empty[Long, com.zilliz.milvus.storage.delete.MilvusDeletePlan]
      } else {
        val pkField = CollectionSchema
          .parseFrom(schemaBytes)
          .fields
          .find(_.isPrimaryKey)
          .getOrElse {
            throw new IllegalArgumentException(
              "No primary key field found in schema"
            )
          }
        MilvusDeltaLogReader.loadPartitionScopedDeletePlans(
          inheritedDeleteSegments,
          pkField,
          snapshotBucket.getOrElse(""),
          StorageOptions.storeFor(
            hadoopConf,
            snapshotBucket.getOrElse(""),
            milvusOption.options
          )
        ) match {
          case Right(plans) => plans
          case Left(err) =>
            throw new IllegalStateException(
              s"Failed to load inherited StorageV2 delete logs from client-created snapshot: ${err.getMessage}",
              err
            )
        }
      }

    SnapshotPartitions.build(
      ctx,
      manifestList = storageV2ManifestList,
      defaultPartitionId = metadata.snapshotInfo.partitionIds.headOption
        .map(_.toString)
        .getOrElse("0"),
      schemaBytes = schemaBytes,
      v3DeletePlans = v3DeletePlanning.deletePlans,
      v3ReadVersions = v3DeletePlanning.readVersions,
      v2Segments = v2Segments,
      v2DeletePlans = v2DeletePlans,
      inheritedDeletePlansByPartition = inheritedDeletePlansByPartition,
      inlineInheritedDeletePlans = true
    )
  }

  private[scan] def readAllBytes(
      conf: Configuration,
      path: String
  ): String = {
    val maxBytes = StorageOptions.parsePositiveLongOption(
      options,
      MilvusOption.SnapshotMaxJsonBytes,
      MilvusSnapshotReader.MaxSnapshotJsonBytes
    )
    val uri = new URI(path)
    val fs = FileSystem.get(uri, conf)
    try {
      val in = fs.open(new Path(uri))
      try MilvusSnapshotReader.readUtf8WithLimit(in, path, maxBytes)
      finally in.close()
    } finally {
      Option(uri.getScheme).foreach { scheme =>
        if (conf.getBoolean(s"fs.$scheme.impl.disable.cache", false)) {
          fs.close()
        }
      }
    }
  }
}

object ClientSnapshotPlanner {
  private val SnapshotOptionKeys = Seq(
    MilvusOption.SnapshotMode,
    MilvusOption.SnapshotManifests,
    MilvusOption.SnapshotV2Segments,
    MilvusOption.SnapshotCollectionId,
    MilvusOption.SnapshotPartitionIds,
    MilvusOption.SnapshotSchemaJson,
    MilvusOption.SnapshotSchemaBytes
  )

  private[scan] def storageV2ManifestBasePath(
      item: StorageV2ManifestItem
  ): String = {
    MilvusSnapshotReader.parseManifestContent(item.manifest) match {
      case Right(content) => content.basePath
      case Left(_)        => item.manifest
    }
  }

  private[scan] def validateSnapshotBucketForRelativeDataPaths(
      snapshotPath: String,
      connectorBucket: Option[String],
      storageV2ManifestList: Seq[StorageV2ManifestItem],
      v2Segments: Seq[V2SegmentInfo]
  ): Unit = {
    StorageOptions.snapshotBucket(snapshotPath).foreach { snapshot =>
      val relativeStorageV3Paths = storageV2ManifestList
        .map(storageV2ManifestBasePath)
        .filter(StorageOptions.isBucketRelativeSnapshotLocation)
      val relativeStorageV2Paths = v2Segments
        .flatMap(_.columnGroups.flatMap(_.filePaths))
        .filter(StorageOptions.isBucketRelativeSnapshotLocation)
      val relativePaths = relativeStorageV3Paths ++ relativeStorageV2Paths
      if (relativePaths.nonEmpty && !connectorBucket.contains(snapshot)) {
        val connectorDescription = connectorBucket.getOrElse("<unset>")
        throw new IllegalArgumentException(
          s"Client-created snapshot metadata is in bucket '$snapshot' but " +
            s"${Properties.FsConfig.FsBucketName} is '$connectorDescription' and snapshot data paths are bucket-relative. " +
            "Refusing to guess which bucket native executors should use; set the connector bucket to the data bucket or use fully-qualified data paths. " +
            s"Example relative path: ${relativePaths.head}"
        )
      }
    }
  }

  private[scan] def ensureClientSnapshotHasPackedSegments(
      storageV2ManifestList: Seq[StorageV2ManifestItem],
      v2Segments: Seq[V2SegmentInfo],
      collectionName: String
  ): Unit = {
    if (storageV2ManifestList.isEmpty && v2Segments.isEmpty) {
      throw new IllegalArgumentException(
        s"No packed-parquet segments (StorageV2/V3) found in client-created snapshot for collection " +
          s"$collectionName. This connector requires Milvus 2.6+ with Storage V2 or V3. " +
          "Please ensure the collection has been flushed and contains data."
      )
    }
  }

  private[scan] def canUseClientSnapshotFastPath(
      milvusOption: MilvusOption
  ): Boolean = {
    milvusOption.partitionName.isEmpty &&
    milvusOption.partitionID.isEmpty &&
    milvusOption.segmentID.isEmpty
  }

  private[scan] def buildClientSnapshotOptions(
      baseOptions: Map[String, String],
      collectionName: String,
      collectionId: Long,
      partitionIds: Seq[Long],
      schemaBytesBase64: String,
      manifestList: Seq[StorageV2ManifestItem],
      v2Segments: Seq[V2SegmentInfo],
      snapshotBucketForRelativePaths: Option[String] = None
  ): Map[String, String] = {
    var out = baseOptions.filterNot { case (key, _) =>
      SnapshotOptionKeys.exists(_.equalsIgnoreCase(key))
    }
    out = out ++ Map(
      MilvusOption.SnapshotMode -> "true",
      MilvusOption.MilvusCollectionName -> collectionName,
      MilvusOption.SnapshotCollectionId -> collectionId.toString,
      MilvusOption.SnapshotPartitionIds -> partitionIds.mkString(","),
      MilvusOption.SnapshotSchemaBytes -> schemaBytesBase64,
      MilvusOption.SnapshotManifests ->
        MilvusSnapshotReader.serializeManifestList(manifestList)
    )
    if (v2Segments.nonEmpty) {
      out += MilvusOption.SnapshotV2Segments ->
        MilvusSnapshotReader.serializeV2Segments(v2Segments)
    }
    snapshotBucketForRelativePaths.foreach { bucket =>
      out = out.filterNot { case (key, _) =>
        key.equalsIgnoreCase(Properties.FsConfig.FsBucketName)
      }
      out += Properties.FsConfig.FsBucketName -> bucket
    }
    out
  }

  private[scan] def validateClientSnapshotMetadata(
      metadata: SnapshotMetadata,
      snapshotPath: String
  ): SnapshotMetadata = {
    if (metadata == null) {
      throw new IllegalArgumentException(
        s"Client-created snapshot metadata at $snapshotPath is missing metadata"
      )
    }
    if (metadata.snapshotInfo == null) {
      throw new IllegalArgumentException(
        s"Client-created snapshot metadata at $snapshotPath is missing snapshot_info"
      )
    }
    if (metadata.collection == null) {
      throw new IllegalArgumentException(
        s"Client-created snapshot metadata at $snapshotPath is missing collection"
      )
    }
    if (metadata.collection.schema == null) {
      throw new IllegalArgumentException(
        s"Client-created snapshot metadata at $snapshotPath is missing collection.schema"
      )
    }
    if (
      metadata.manifestList.isEmpty &&
      metadata.storageV2ManifestList.forall(_.isEmpty)
    ) {
      throw new IllegalArgumentException(
        s"Invalid client-created snapshot metadata at $snapshotPath: client snapshot is empty: no manifests and no V2 segments"
      )
    }
    metadata
  }
}
