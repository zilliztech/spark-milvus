package com.zilliz.milvus.storage.snapshot

import java.nio.charset.StandardCharsets
import scala.util.control.NonFatal

import com.zilliz.milvus.storage.io.ObjectStore
import com.zilliz.milvus.storage.manifest.{
  SnapshotSegmentEntry,
  SnapshotSegmentReader
}
import com.zilliz.milvus.storage.path.StoragePath
import com.zilliz.milvus.storage.snapshot.json.{
  ManifestContentJson,
  ManifestItemJson,
  SnapshotJson
}
import com.zilliz.milvus.storage.Logging
import io.milvus.grpc.schema.{
  CollectionSchema => ProtoSchema,
  DataType => ProtoDataType
}

/** The main read entry point: snapshots Milvus wrote to the snapshot directory,
  * read through `core.io.ObjectStore`.
  *
  * Layout, written by DataCoord:
  * {{{
  *   {rootPath}/snapshots/{collectionId}/metadata/{snapshotId}.json
  *   {rootPath}/snapshots/{collectionId}/manifests/{snapshotId}/{segmentId}.avro
  * }}}
  *
  * @param store
  *   bound to `bucket`; keys handed to it are bucket-relative
  * @param bucket
  *   the bucket every relative path in the snapshot resolves against; empty for
  *   a local store
  */
final class SnapshotCatalog(
    store: ObjectStore,
    bucket: String,
    maxJsonBytes: Long = SnapshotJson.MaxBytes,
    endpoint: String = ""
) extends Logging {

  /** The snapshot at `location`: a bucket-relative key, a `s3a://` / `s3://`
    * URI whose bucket must match this catalog's, or the
    * `scheme://endpoint/bucket/key` form Milvus prints for its own snapshots
    * (CreateSnapshot's `s3_location`), recognised only when the host is this
    * catalog's endpoint.
    */
  def read(location: String): Snapshot =
    materialize(location, metadata(location))

  /** The parsed JSON at `location`, with the bucket and size checks; opens no
    * segment file. A location that names a bucket must name this catalog's;
    * over a store with no bucket (the local backend) it is refused rather than
    * read as a local key.
    */
  private def metadata(location: String): SnapshotJson = {
    val located = StoragePath.parseMilvus(location, bucket, endpoint)
    if (located.hasBucket && located.bucket != bucket) {
      val boundTo =
        if (bucket.isEmpty) "a store with no bucket (the local backend)"
        else s"bucket '$bucket'"
      throw new IllegalArgumentException(
        s"snapshot $location is in bucket '${located.bucket}', catalog is bound to $boundTo"
      )
    }
    val key = located.key
    val size = store.size(key)
    if (size > maxJsonBytes) {
      throw new IllegalArgumentException(
        s"snapshot metadata $location is $size bytes, over the $maxJsonBytes byte limit"
      )
    }
    val json = new String(store.readAll(key), StandardCharsets.UTF_8)
    val parsed = SnapshotJson.parse(json) match {
      case Right(m) => m
      case Left(e) =>
        throw new IllegalArgumentException(
          s"failed to parse snapshot metadata at $location: ${e.getMessage}",
          e
        )
    }
    if (parsed.snapshotInfo == null) {
      throw new IllegalArgumentException(
        s"invalid snapshot at $location: snapshot is missing snapshot_info"
      )
    }
    parsed
  }

  /** Reads the segment records and validates the fixed V3 snapshot. */
  private def materialize(location: String, metadata: SnapshotJson): Snapshot =
    SnapshotCatalog.fromMetadata(
      metadata,
      SnapshotOrigin.Catalog(location),
      store,
      bucket,
      endpoint
    ) match {
      case Right(s) => s
      case Left(e) =>
        throw new IllegalArgumentException(
          s"invalid snapshot at $location: ${e.getMessage}",
          e
        )
    }

  /** A fixed snapshot file source. */
  def at(location: String): SnapshotSource = SnapshotSource(read(location))

}

object SnapshotCatalog extends Logging {

  /** Segment id of a V3 manifest entry: the id the entry carries, else the last
    * path element of its base path, else 0.
    */
  def segmentIdForManifestItem(
      item: ManifestItemJson,
      basePath: String
  ): Long =
    if (item.segmentID != 0L) item.segmentID
    else {
      val parts = basePath.split("/").filter(_.nonEmpty)
      if (parts.isEmpty) 0L
      else
        try parts.last.toLong
        catch { case _: NumberFormatException => 0L }
    }

  /** Partition id of a V3 segment from its base path, which Milvus lays out as
    * `.../insert_log/{collectionId}/{partitionId}/{segmentId}`.
    */
  def partitionIdFromBasePath(basePath: String): Option[Long] = {
    val parts = basePath.split("/").filter(_.nonEmpty)
    val i = parts.lastIndexOf("insert_log")
    if (i >= 0 && parts.length > i + 3)
      try Some(parts(i + 2).toLong)
      catch { case _: NumberFormatException => None }
    else None
  }

  /** The snapshot JSON's shape becomes the model. Validation that used to sit
    * in the client planner lives here: a snapshot missing its info, collection
    * or schema is rejected. A valid empty collection has no manifests and
    * produces an empty snapshot with its collection schema intact.
    */
  def fromMetadata(
      metadata: SnapshotJson,
      origin: SnapshotOrigin,
      store: ObjectStore,
      bucket: String,
      endpoint: String = ""
  ): Either[Throwable, Snapshot] = {
    def bad(msg: String) = Left(new IllegalArgumentException(msg))
    if (metadata == null) return bad("snapshot metadata is missing")
    if (metadata.snapshotInfo == null)
      return bad("snapshot is missing snapshot_info")
    if (metadata.collection == null)
      return bad("snapshot is missing collection")
    if (metadata.collection.schema == null)
      return bad("snapshot is missing collection.schema")
    val v3Items = metadata.storageV2ManifestList.getOrElse(Seq.empty)
    if (metadata.manifestList.isEmpty && v3Items.isEmpty) {
      if (
        metadata.segmentIds.exists(_.nonEmpty) || metadata.allSegments.nonEmpty
      )
        return bad(
          "snapshot declares segments but contains no segment manifests"
        )
      if (
        metadata.snapshotInfo.collectionId <= 0L ||
        Option(metadata.snapshotInfo.name).forall(_.trim.isEmpty) ||
        metadata.snapshotInfo.rawCreateTs.isEmpty ||
        metadata.snapshotInfo.createTs <= 0L ||
        metadata.collection.schema.fields.isEmpty
      )
        return bad(
          "empty snapshot requires collection identity, creation timestamp and a nonempty schema"
        )
    }
    val entries =
      try {
        metadata.manifestList.map { path =>
          val key = keyIn(bucket, path, "segment snapshot Avro", endpoint)
          SnapshotSegmentReader.parse(
            store.readAll(key),
            metadata.manifestSchemaVersion
          ) match {
            case Right(entry) => entry
            case Left(error) =>
              throw new IllegalArgumentException(
                s"failed to decode segment snapshot Avro $key: ${error.getMessage}",
                error
              )
          }
        }
      } catch { case NonFatal(error) => return Left(error) }
    val unsupported = entries.filter(_.storageVersion != 3L)
    if (unsupported.nonEmpty)
      return bad(
        "public snapshot reads require Storage V3; unsupported segment(s): " +
          unsupported
            .map(entry => s"${entry.segmentId}=${entry.storageVersion}")
            .mkString(", ")
      )
    val schemaBytes =
      try metadata.collection.schema.toProtobufBytes
      catch { case e: Exception => return Left(e) }
    val info = metadata.snapshotInfo
    val snapshot = fromLists(
      name = info.name,
      collectionId = info.collectionId,
      createdAt = info.rawCreateTs.map(_ => info.createTs),
      partitionIds = info.partitionIds,
      schemaBytes = schemaBytes,
      v3Items = v3Items,
      bucket = bucket,
      origin = origin,
      endpoint = endpoint
    )
    snapshot.flatMap { value =>
      try Right(attachIndexes(value, metadata, entries, endpoint))
      catch { case NonFatal(error) => Left(error) }
    }
  }

  private def attachIndexes(
      snapshot: Snapshot,
      metadata: SnapshotJson,
      entries: Seq[SnapshotSegmentEntry],
      endpoint: String
  ): Snapshot = {
    val definitions = metadata.indexes.map(_.map(_.toIndex).toVector)
    definitions.foreach(_.foreach { index =>
      require(
        index.collectionId == snapshot.collectionId,
        s"collection index ${index.indexId} belongs to collection ${index.collectionId}, expected ${snapshot.collectionId}"
      )
      require(
        snapshot.schema.fields.exists(_.fieldID == index.fieldId),
        s"collection index ${index.indexId} names unknown field ${index.fieldId}"
      )
    })
    val bySegment = entries.groupBy(_.segmentId)
    require(
      bySegment.forall(_._2.size == 1),
      "snapshot lists duplicate segment Avro records"
    )
    val describedIds = snapshot.segments.iterator.map(_.id).toSet
    val unlocated = bySegment.keySet.diff(describedIds)
    require(
      unlocated.isEmpty,
      s"snapshot segment Avro records have no V3 manifest location: ${unlocated.toSeq.sorted.mkString(", ")}"
    )
    val segments = snapshot.segments.map { segment =>
      bySegment.get(segment.id).map(_.head) match {
        case None => segment
        case Some(entry) =>
          require(
            entry.partitionId == segment.partitionId,
            s"segment ${segment.id} has conflicting snapshot partition ids"
          )
          require(
            entry.storageVersion == segment.storageVersion,
            s"segment ${segment.id} has conflicting snapshot storage versions"
          )
          require(
            entry.numOfRows >= 0L,
            s"segment ${segment.id} has negative row count"
          )
          val indexes = entry.indexFiles match {
            case None => SegmentIndexes.Unknown
            case Some(files) =>
              val available = files.filter(_.filePaths.nonEmpty).map { index =>
                require(
                  index.segmentId == segment.id,
                  s"index ${index.indexId} names segment ${index.segmentId}, expected ${segment.id}"
                )
                require(
                  index.rowCount == entry.numOfRows,
                  s"index ${index.indexId} has ${index.rowCount} rows but segment ${segment.id} has ${entry.numOfRows}"
                )
                require(
                  index.buildId > 0L,
                  s"index ${index.indexId} has no build id"
                )
                require(
                  index.filePaths.forall(_.nonEmpty),
                  s"index ${index.indexId} contains an empty file path"
                )
                metadata.buildIds.foreach { ids =>
                  require(
                    ids.contains(index.buildId),
                    s"index ${index.indexId} build ${index.buildId} is absent from snapshot build_ids"
                  )
                }
                definitions.foreach { all =>
                  require(
                    all.exists(d =>
                      d.indexId == index.indexId && d.fieldId == index.fieldId
                    ),
                    s"segment index ${index.indexId} for field ${index.fieldId} is absent from snapshot indexes"
                  )
                }
                SegmentIndex(
                  collectionId = snapshot.collectionId,
                  partitionId = entry.partitionId,
                  segmentId = segment.id,
                  fieldId = index.fieldId,
                  indexId = index.indexId,
                  buildId = index.buildId,
                  name = index.name,
                  parameters = index.parameters,
                  filePaths = index.filePaths.map(path =>
                    keyIn(
                      snapshot.bucket,
                      path,
                      s"index ${index.indexId}",
                      endpoint
                    )
                  ),
                  rowCount = index.rowCount,
                  serializedSize = index.serializedSize,
                  indexVersion = index.indexVersion,
                  currentIndexVersion = index.currentIndexVersion,
                  indexStorePathVersion = index.indexStorePathVersion
                )
              }
              if (available.nonEmpty) SegmentIndexes.Available(available)
              // A version 5 record whose segment manifest registers the index
              // names no files here; that is metadata this connector has not
              // read, not a segment without an index.
              else if (entry.manifestHasIndex.contains(true))
                SegmentIndexes.Unknown
              else SegmentIndexes.Unindexed
          }
          segment.copy(rows = Some(entry.numOfRows), indexes = indexes)
      }
    }
    snapshot.copy(
      segments = segments,
      indexes = definitions,
      buildIds = metadata.buildIds
    )
  }

  /** A [[Snapshot]] from explicit V3 manifest entries and schema metadata.
    *
    * Every path in a [[Segment]] is a key relative to `bucket`, because that is
    * what the native reader takes (it is rooted at `fs.bucket_name` and appends
    * what it is given). Sources hand paths in whatever spelling they use; a
    * path in another bucket is an error here, not a wrong read later.
    */
  def fromLists(
      name: String,
      collectionId: Long,
      createdAt: Option[Long],
      partitionIds: Seq[Long],
      schemaBytes: Array[Byte],
      v3Items: Seq[ManifestItemJson],
      bucket: String,
      origin: SnapshotOrigin,
      endpoint: String = ""
  ): Either[Throwable, Snapshot] =
    try {
      val defaultPartition = partitionIds.headOption
      val v3 = v3Items.map { item =>
        val (rawBasePath, version) =
          ManifestContentJson.parse(item.manifest) match {
            case Right(content) => (content.basePath, content.ver.toLong)
            case Left(_)        => (item.manifest, -1L)
          }
        val basePath =
          keyIn(
            bucket,
            rawBasePath,
            s"segment ${item.segmentID} manifest",
            endpoint
          )
        val partitionId = partitionIdFromBasePath(basePath).getOrElse {
          logWarning(
            s"manifest path '$basePath' does not match insert_log/{collectionId}/{partitionId}/{segmentId}; " +
              s"using partition ${defaultPartition.getOrElse(0L)}"
          )
          defaultPartition.getOrElse(0L)
        }
        Segment(
          id = segmentIdForManifestItem(item, basePath),
          partitionId = partitionId,
          storageVersion = 3,
          rows = None,
          layout = SegmentLayout.Manifest(basePath, version),
          deletes = DeleteFiles.InManifest,
          statistics = SegmentStatistics.InManifest
        )
      }
      val schema =
        try ProtoSchema.parseFrom(schemaBytes)
        catch { case e: Exception => return Left(e) }
      validateDynamicFieldSchema(name, schema)
      val segments = v3
      val invalidIds =
        segments.iterator.map(_.id).filter(_ <= 0L).toSeq.distinct.sorted
      if (invalidIds.nonEmpty) {
        return Left(
          new IllegalArgumentException(
            s"snapshot '$name' contains non-positive segment id(s): ${invalidIds.mkString(", ")}"
          )
        )
      }
      val duplicateIds = segments
        .groupBy(_.id)
        .collect {
          case (id, occurrences) if occurrences.size > 1 => id
        }
        .toSeq
        .sorted
      if (duplicateIds.nonEmpty) {
        return Left(
          new IllegalArgumentException(
            s"snapshot '$name' contains duplicate segment id(s): ${duplicateIds.mkString(", ")}"
          )
        )
      }
      Right(
        Snapshot(
          name = name,
          collectionId = collectionId,
          createdAt = createdAt,
          schema = schema,
          partitionIds = partitionIds,
          segments = segments,
          origin = origin,
          bucket = bucket
        )
      )
    } catch { case NonFatal(e) => Left(e) }

  /** A dynamic field is a stored JSON column with a service-assigned field ID.
    * The ID cannot be reconstructed from the other fields after schema
    * evolution, so an incomplete snapshot must fail instead of silently
    * dropping dynamic values or guessing a physical column.
    */
  private def validateDynamicFieldSchema(
      snapshotName: String,
      schema: ProtoSchema
  ): Unit = {
    if (!schema.enableDynamicField) return

    val dynamicCandidates =
      schema.fields.filter(field => field.isDynamic || field.name == "$meta")
    val valid = dynamicCandidates match {
      case Seq(field) =>
        field.name == "$meta" &&
        field.isDynamic &&
        field.dataType == ProtoDataType.JSON &&
        field.fieldID > 1L
      case _ => false
    }
    if (!valid) {
      throw new IllegalArgumentException(
        s"snapshot '$snapshotName' enables dynamic fields but its schema must " +
          "contain exactly one '$meta' JSON field marked dynamic with an " +
          "explicit non-system field id; the connector cannot infer that id"
      )
    }
  }

  /** A path as the key the native reader takes, relative to `bucket`. */
  private def keyIn(
      bucket: String,
      path: String,
      what: String,
      endpoint: String
  ): String = {
    val located = StoragePath.parseMilvus(path, bucket, endpoint)
    if (bucket.nonEmpty && located.hasBucket && located.bucket != bucket) {
      throw new IllegalArgumentException(
        s"$what is in bucket '${located.bucket}' but the read is bound to '$bucket': $path"
      )
    }
    located.key
  }

}
