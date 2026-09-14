package com.zilliz.spark.connector.scan

import org.apache.spark.sql.connector.read.InputPartition

import com.zilliz.milvus.client.api.{MilvusClient, MilvusCollectionInfo}
import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.milvus.storage.read.plan.{InputSpec, SegmentLayout}
import com.zilliz.spark.connector.scan.MilvusStorageV3InputPartition

/** Client mode, legacy path: `GetPersistentSegmentInfo` lists the segments and
  * each V3 segment becomes a partition. Taken when a partition or segment
  * selector is set, or when the service has no `CreateSnapshot`.
  */
private[scan] final class LegacyClientPlanner(ctx: ScanContext)
    extends PartitionPlanner(ctx) {
  def plan(
      client: MilvusClient,
      collectionInfo: MilvusCollectionInfo
  ): Array[InputPartition] = {
    val collection = milvusOption.collectionID
    val partition = milvusOption.partitionID
    val segment = milvusOption.segmentID
    val s3RootPath =
      milvusOption.options.getOrElse(StorageProperties.RootPath, "files")

    def createPartition(
        segmentID: String,
        partitionID: String
    ): InputPartition = {
      val segmentPath =
        s"$s3RootPath/insert_log/$collection/$partitionID/$segmentID"
      logInfo(
        s"Creating V3 partition: segmentID=$segmentID, segmentPath=$segmentPath"
      )

      val segmentIDLong =
        try { segmentID.toLong }
        catch { case _: NumberFormatException => -1L }
      val partitionIdLong =
        try { partitionID.toLong }
        catch { case _: NumberFormatException => Long.MinValue }
      MilvusStorageV3InputPartition(
        InputSpec(
          segmentId = segmentIDLong,
          partitionId = partitionIdLong,
          layout = SegmentLayout.Manifest(segmentPath),
          schemaBytes = collectionInfo.schema.toByteArray,
          properties = StorageProperties.from(milvusOption.options)
        ),
        partitionID,
        milvusOption,
        vectorSearchConfig.map(_.topK),
        vectorSearchConfig.map(_.queryVector),
        vectorSearchConfig.map(_.metricType),
        vectorSearchConfig.map(_.vectorColumn)
      )
    }

    val allPackedSegments = client
      .getSegments(
        milvusOption.databaseName,
        milvusOption.collectionName
      )
      .getOrElse(
        throw new Exception("Failed to get segments")
      )
      .filter(_.storageVersion >= 2)

    if (allPackedSegments.isEmpty) {
      throw new IllegalArgumentException(
        s"No packed-parquet segments (StorageV2/V3) found in collection " +
          s"${milvusOption.collectionName}. This connector requires Milvus " +
          "2.6+ with Storage V2 or V3. Please ensure the collection has " +
          "been flushed and contains data."
      )
    }

    val storageV2Segments = allPackedSegments.filter(_.storageVersion == 2)
    if (storageV2Segments.nonEmpty) {
      throw new IllegalArgumentException(
        s"Legacy GetPersistentSegmentInfo read path cannot safely read StorageV2 segments for collection " +
          s"${milvusOption.collectionName}. Use snapshot mode or the client snapshot fast path instead. " +
          s"Offending segment IDs: ${storageV2Segments.map(_.segmentID).sorted.mkString(",")}"
      )
    }

    val storageV3Segments = allPackedSegments.filter(_.storageVersion >= 3)

    val partitions =
      if (partition.nonEmpty && segment.nonEmpty) {
        val segmentInfo =
          storageV3Segments.find(_.segmentID.toString == segment)
        segmentInfo match {
          case Some(seg) =>
            if (seg.partitionID.toString != partition) {
              throw new IllegalArgumentException(
                s"Segment $segment belongs to partition ${seg.partitionID}, not $partition"
              )
            }
            Array(createPartition(segment, partition))
          case None =>
            throw new IllegalArgumentException(
              s"Segment $segment not found or has storage_version < 2 " +
                "(StorageV2/V3 packed parquet required)"
            )
        }
      } else if (partition.nonEmpty) {
        storageV3Segments
          .filter(_.partitionID.toString == partition)
          .map(seg => createPartition(seg.segmentID.toString, partition))
          .toArray
      } else {
        storageV3Segments.map { seg =>
          createPartition(seg.segmentID.toString, seg.partitionID.toString)
        }.toArray
      }

    logInfo(s"Created ${partitions.length} partitions via legacy client path")
    partitions
  }
}
