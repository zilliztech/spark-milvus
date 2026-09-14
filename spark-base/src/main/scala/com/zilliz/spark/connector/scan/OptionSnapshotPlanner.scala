package com.zilliz.spark.connector.scan

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.connector.read.InputPartition

import com.zilliz.milvus.storage.delete.MilvusDeltaLogReader
import com.zilliz.spark.connector.options.MilvusOption
import com.zilliz.spark.connector.options.StorageOptions
import io.milvus.grpc.schema.CollectionSchema

/** Snapshot mode: the manifest list and the V2 segment list arrive in the
  * options themselves, so no Milvus service is contacted.
  */
private[scan] final class OptionSnapshotPlanner(ctx: ScanContext)
    extends PartitionPlanner(ctx) {

  /** Plan input partitions from snapshot manifests (offline mode - no client
    * connection) This enables reading Milvus data purely from snapshot metadata
    * without any client calls.
    *
    * The caller passes either `manifestsJson` (the legacy `SnapshotManifests`
    * option) for manifest-based segments, whose segment info carries
    * `storage_version = 3`, or a non-empty `SnapshotV2Segments` option carrying
    * a materialized list of `V2SegmentInfo` for packed-parquet segments, whose
    * segment info carries `storage_version = 2`.
    *
    * When both are present the planner emits partitions from both sources
    * (mixed-version snapshot).
    */
  def plan(): Array[InputPartition] = {
    MilvusOption.validateSnapshotModeOptions(options)
    val manifestsJson =
      Option(options.get(MilvusOption.SnapshotManifests)).getOrElse("")
    import com.zilliz.milvus.storage.snapshot.{
      MilvusSnapshotReader,
      StorageV2ManifestItem
    }

    logInfo(
      "Using snapshot mode for partition planning (no Milvus client connection)"
    )

    val manifestList: Seq[StorageV2ManifestItem] =
      if (manifestsJson == null || manifestsJson.isEmpty) Seq.empty
      else
        MilvusSnapshotReader.deserializeManifestList(manifestsJson) match {
          case Right(list) => list
          case Left(e) =>
            throw new Exception(
              s"Failed to parse snapshot manifests: ${e.getMessage}",
              e
            )
        }

    val partitionIds = Option(options.get(MilvusOption.SnapshotPartitionIds))
      .map(_.split(",").map(_.trim).filter(_.nonEmpty))
      .getOrElse(Array.empty[String])
    val defaultPartitionId = partitionIds.headOption.getOrElse("0")

    val schemaBytes = Option(options.get(MilvusOption.SnapshotSchemaBytes))
      .map(base64 => java.util.Base64.getDecoder.decode(base64))
      .getOrElse {
        logWarning(
          "No schema bytes provided in snapshot mode, using empty schema"
        )
        Array.empty[Byte]
      }

    logInfo(
      s"Using schema bytes (${schemaBytes.length} bytes) for V2 partitions"
    )

    val v2Segments = Option(options.get(MilvusOption.SnapshotV2Segments))
      .filter(_.nonEmpty)
      .map { json =>
        MilvusSnapshotReader.deserializeV2Segments(json) match {
          case Right(segs) => segs
          case Left(e) =>
            throw new Exception(
              s"Failed to parse SnapshotV2Segments: ${e.getMessage}",
              e
            )
        }
      }
      .getOrElse(Seq.empty)

    val snapshotBucketForRelativePaths =
      StorageOptions.connectorS3BucketOption(options.asScala.toMap)

    val snapshotHadoopConf = ctx.hadoopConf("")
    val v3DeletePlanning =
      DeletePlanning.loadV3DeletePlanning(
        ctx,
        manifestList,
        schemaBytes,
        snapshotBucket = snapshotBucketForRelativePaths,
        hadoopConf = snapshotHadoopConf,
        errorContext = "snapshot metadata"
      )

    val v2DeletePlans =
      DeletePlanning.loadV2DeletePlans(
        ctx,
        v2Segments,
        schemaBytes,
        snapshotBucket = snapshotBucketForRelativePaths,
        hadoopConf = snapshotHadoopConf,
        errorContext = "snapshot metadata"
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
          snapshotBucketForRelativePaths.getOrElse(""),
          StorageOptions.storeFor(
            snapshotHadoopConf,
            snapshotBucketForRelativePaths.getOrElse(""),
            milvusOption.options
          )
        ) match {
          case Right(plans) => plans
          case Left(err) =>
            throw new IllegalStateException(
              s"Failed to load inherited StorageV2 delete logs from snapshot metadata: ${err.getMessage}",
              err
            )
        }
      }

    SnapshotPartitions.build(
      ctx,
      manifestList = manifestList,
      defaultPartitionId = defaultPartitionId,
      schemaBytes = schemaBytes,
      v3DeletePlans = v3DeletePlanning.deletePlans,
      v3ReadVersions = v3DeletePlanning.readVersions,
      v2Segments = v2Segments,
      v2DeletePlans = v2DeletePlans,
      inheritedDeletePlansByPartition = inheritedDeletePlansByPartition
    )
  }
}
