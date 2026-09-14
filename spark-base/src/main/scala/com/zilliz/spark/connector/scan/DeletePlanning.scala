package com.zilliz.spark.connector.scan

import org.apache.hadoop.conf.Configuration
import org.apache.spark.internal.Logging

import com.zilliz.milvus.storage.delete.MilvusDeltaLogReader
import com.zilliz.milvus.storage.manifest.MilvusStorageV3ManifestReader
import com.zilliz.milvus.storage.snapshot.{
  MilvusSnapshotReader,
  StorageV2ManifestItem,
  V2DeltaLogFile,
  V2SegmentInfo
}
import com.zilliz.spark.connector.options.MilvusOption
import com.zilliz.spark.connector.options.StorageOptions
import io.milvus.grpc.schema.CollectionSchema

/** Delete plans for a set of segments, resolved on the driver.
  *
  * V2 packed segments carry their delta logs in the segment info; V3 segments
  * carry them in the manifest at the read version, which is resolved here too.
  */
object DeletePlanning extends Logging {
  private[scan] case class V3DeletePlanning(
      deletePlans: Map[
        Long,
        com.zilliz.milvus.storage.delete.MilvusDeletePlan
      ],
      readVersions: Map[Long, Long]
  )

  private[scan] object V3DeletePlanning {
    val empty: V3DeletePlanning = V3DeletePlanning(Map.empty, Map.empty)
  }

  private[scan] def loadV2DeletePlans(
      ctx: ScanContext,
      v2Segments: Seq[V2SegmentInfo],
      schemaBytes: Array[Byte],
      snapshotBucket: Option[String],
      hadoopConf: Configuration,
      errorContext: String
  ): Map[Long, com.zilliz.milvus.storage.delete.MilvusDeletePlan] = {
    val applyDeletes = MilvusOption.readApplyDeletes(ctx.options)
    val dataSegments = v2Segments.filter(seg =>
      seg.columnGroups.nonEmpty && seg.deltaLogs.nonEmpty
    )
    if (!applyDeletes || dataSegments.isEmpty) {
      Map.empty
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
      dataSegments.map { seg =>
        MilvusDeltaLogReader.loadDeletePlan(
          seg.deltaLogs,
          pkField,
          snapshotBucket.getOrElse(""),
          StorageOptions.storeFor(
            hadoopConf,
            snapshotBucket.getOrElse(""),
            ctx.milvusOption.options
          )
        ) match {
          case Right(plan) => seg.segmentId -> plan
          case Left(err) =>
            throw new IllegalStateException(
              s"Failed to load StorageV2 delete logs from $errorContext for segment ${seg.segmentId}: ${err.getMessage}",
              err
            )
        }
      }.toMap
    }
  }

  private[scan] def loadV3DeletePlanning(
      ctx: ScanContext,
      manifestList: Seq[StorageV2ManifestItem],
      schemaBytes: Array[Byte],
      snapshotBucket: Option[String],
      hadoopConf: Configuration,
      errorContext: String
  ): V3DeletePlanning = {
    val applyDeletes = MilvusOption.readApplyDeletes(ctx.options)
    if (!applyDeletes || manifestList.isEmpty) {
      V3DeletePlanning.empty
    } else {
      val pkField = CollectionSchema
        .parseFrom(schemaBytes)
        .fields
        .find(_.isPrimaryKey)
      if (pkField.isEmpty) {
        return V3DeletePlanning.empty
      }

      val entries = manifestList.flatMap { item =>
        val (basePath, requestedReadVersion) =
          MilvusSnapshotReader.parseManifestContent(item.manifest) match {
            case Right(content) => (content.basePath, content.ver.toLong)
            case Left(_)        => (item.manifest, -1L)
          }
        val segmentId =
          SnapshotPartitions.segmentIdForManifestItem(item, basePath)
        if (segmentId == 0L) {
          None
        } else {
          val readVersion: Long =
            if (requestedReadVersion > 0L) {
              requestedReadVersion
            } else {
              MilvusStorageV3ManifestReader.latestManifestVersion(
                basePath,
                snapshotBucket.getOrElse(""),
                StorageOptions.storeFor(
                  hadoopConf,
                  snapshotBucket.getOrElse(""),
                  ctx.milvusOption.options
                )
              ) match {
                case Right(version) => version
                case Left(err) =>
                  throw new IllegalStateException(
                    s"Failed to resolve latest StorageV3 manifest version from $errorContext for segment $segmentId: ${err.getMessage}",
                    err
                  )
              }
            }
          if (readVersion <= 0L) {
            None
          } else {
            val deltaLogs: Seq[V2DeltaLogFile] =
              MilvusStorageV3ManifestReader.loadDeltaLogs(
                basePath,
                readVersion,
                snapshotBucket.getOrElse(""),
                StorageOptions.storeFor(
                  hadoopConf,
                  snapshotBucket.getOrElse(""),
                  ctx.milvusOption.options
                )
              ) match {
                case Right(logs) => logs
                case Left(err) =>
                  throw new IllegalStateException(
                    s"Failed to load StorageV3 manifest delete logs from $errorContext for segment $segmentId: ${err.getMessage}",
                    err
                  )
              }
            val deletePlan
                : Option[com.zilliz.milvus.storage.delete.MilvusDeletePlan] =
              if (deltaLogs.isEmpty) {
                None
              } else {
                MilvusDeltaLogReader.loadDeletePlan(
                  deltaLogs,
                  pkField.get,
                  snapshotBucket.getOrElse(""),
                  StorageOptions.storeFor(
                    hadoopConf,
                    snapshotBucket.getOrElse(""),
                    ctx.milvusOption.options
                  )
                ) match {
                  case Right(plan) => Some(plan)
                  case Left(err) =>
                    throw new IllegalStateException(
                      s"Failed to decode StorageV3 manifest delete logs from $errorContext for segment $segmentId: ${err.getMessage}",
                      err
                    )
                }
              }
            Some((segmentId, readVersion, deletePlan))
          }
        }
      }
      V3DeletePlanning(
        deletePlans = entries.flatMap { case (segmentId, _, deletePlan) =>
          deletePlan.map(segmentId -> _)
        }.toMap,
        readVersions = entries.map { case (segmentId, readVersion, _) =>
          segmentId -> readVersion
        }.toMap
      )
    }
  }
}
