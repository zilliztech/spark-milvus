package com.zilliz.spark.connector.read.plan

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import com.zilliz.milvus.storage.snapshot.{
  MilvusSnapshotReader,
  Snapshot,
  SnapshotCatalog,
  SnapshotOrigin
}
import com.zilliz.spark.connector.options.{MilvusOption, StorageOptions}

/** Snapshot mode: no Milvus service is contacted. The snapshot is either the
  * JSON at `milvus.snapshot.path`, read through [[SnapshotCatalog]], or the
  * 1.x form where the manifest list and the V2 segment list arrive as option
  * strings (capability K2, deleted; the code stays until backfill's source
  * read moves to the catalog).
  */
private[read] final class OptionSnapshotPlanner(ctx: ScanContext)
    extends PartitionPlanner(ctx) {

  def plan(): Array[InputPartition] = {
    MilvusOption.validateSnapshotModeOptions(options)
    logInfo(
      "Using snapshot mode for partition planning (no Milvus client connection)"
    )
    val snapshot = OptionSnapshotPlanner.snapshotFor(ctx)
    val bucket = StorageOptions.connectorS3BucketOption(options.asScala.toMap)
    val hadoopConf = ctx.hadoopConf("")
    val v3 = DeletePlanning.loadV3DeletePlanning(
      ctx,
      snapshot,
      bucket,
      hadoopConf,
      errorContext = "snapshot metadata"
    )
    val v2 = DeletePlanning.loadV2DeletePlans(
      ctx,
      snapshot,
      bucket,
      hadoopConf,
      errorContext = "snapshot metadata"
    )
    val inherited = DeletePlanning.loadInheritedDeletePlans(
      ctx,
      snapshot,
      bucket,
      hadoopConf,
      errorContext = "snapshot metadata"
    )
    SnapshotPartitions.build(
      ctx,
      snapshot,
      v3DeletePlans = v3.deletePlans,
      v3ReadVersions = v3.readVersions,
      v2DeletePlans = v2,
      inheritedDeletePlansByPartition = inherited
    )
  }
}

object OptionSnapshotPlanner {

  /** The snapshot the options describe. Read once per scan by the planner and
    * once by the reader factory, which must not depend on planning having run.
    */
  private[read] def snapshotFor(ctx: ScanContext): Snapshot =
    Option(ctx.options.get(MilvusOption.SnapshotPath))
      .map(_.trim)
      .filter(_.nonEmpty) match {
      case Some(path) => ClientSnapshotPlanner.catalog(ctx).read(path)
      case None       => fromOptionStrings(ctx.options)
    }

  /** The 1.x form: three option strings hold what the snapshot JSON holds. */
  private[read] def fromOptionStrings(
      options: CaseInsensitiveStringMap
  ): Snapshot = {
    val manifestsJson = Option(options.get(MilvusOption.SnapshotManifests))
      .getOrElse("")
    val v3Items =
      if (manifestsJson.isEmpty) Seq.empty
      else
        MilvusSnapshotReader.deserializeManifestList(manifestsJson) match {
          case Right(list) => list
          case Left(e) =>
            throw new IllegalArgumentException(
              s"Failed to parse snapshot manifests: ${e.getMessage}",
              e
            )
        }
    val v2Segments = Option(options.get(MilvusOption.SnapshotV2Segments))
      .filter(_.nonEmpty)
      .map { json =>
        MilvusSnapshotReader.deserializeV2Segments(json) match {
          case Right(segs) => segs
          case Left(e) =>
            throw new IllegalArgumentException(
              s"Failed to parse SnapshotV2Segments: ${e.getMessage}",
              e
            )
        }
      }
      .getOrElse(Seq.empty)
    val partitionIds = Option(options.get(MilvusOption.SnapshotPartitionIds))
      .map(_.split(",").map(_.trim).filter(_.nonEmpty).map(_.toLong).toSeq)
      .getOrElse(Seq.empty)
    val schemaBytes = Option(options.get(MilvusOption.SnapshotSchemaBytes))
      .map(base64 => java.util.Base64.getDecoder.decode(base64))
      .getOrElse(Array.emptyByteArray)
    val collectionId = Option(options.get(MilvusOption.SnapshotCollectionId))
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(_.toLong)
      .getOrElse(0L)
    SnapshotCatalog.fromLists(
      name = "options",
      collectionId = collectionId,
      createdAt = None,
      partitionIds = partitionIds,
      schemaBytes = schemaBytes,
      v3Items = v3Items,
      v2Segments = v2Segments,
      bucket = StorageOptions.connectorS3BucketOption(options.asScala.toMap).getOrElse(""),
      origin = SnapshotOrigin.Options
    ) match {
      case Right(s) => s
      case Left(e) =>
        throw new IllegalArgumentException(
          s"Invalid snapshot options: ${e.getMessage}",
          e
        )
    }
  }
}
