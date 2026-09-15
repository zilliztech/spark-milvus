package com.zilliz.milvus.storage.snapshot

import java.nio.charset.StandardCharsets

import com.zilliz.milvus.storage.io.{FileInfo, ObjectStore}
import com.zilliz.milvus.storage.path.StoragePath
import com.zilliz.milvus.storage.snapshot.json.{
  ManifestContentJson,
  ManifestItemJson,
  SnapshotJson
}
import com.zilliz.milvus.storage.Logging
import io.milvus.grpc.schema.{CollectionSchema => ProtoSchema}

/** Materializes the V2 packed segments a snapshot lists.
  *
  * A V2 segment has no manifest: its column groups come from the snapshot's
  * Avro plus each parquet footer. That code is `compat.v2`, which core must not
  * depend on (capability K1), so the catalog is handed the resolver.
  */
trait V2SegmentResolver {
  def resolve(
      manifestPaths: Seq[String],
      bucket: String,
      store: ObjectStore,
      manifestSchemaVersion: Int
  ): Either[Throwable, Seq[V2SegmentInfo]]
}

object V2SegmentResolver {

  /** For a caller that needs the snapshot's schema and partitions only: the V2
    * segments are left out rather than materialized. The resulting `Snapshot`
    * must not be planned against.
    */
  val Skipped: V2SegmentResolver = new V2SegmentResolver {
    def resolve(
        manifestPaths: Seq[String],
        bucket: String,
        store: ObjectStore,
        manifestSchemaVersion: Int
    ): Either[Throwable, Seq[V2SegmentInfo]] = Right(Seq.empty)
  }

  /** For a caller that knows the snapshot holds no V2 segments. */
  val Unavailable: V2SegmentResolver = new V2SegmentResolver {
    def resolve(
        manifestPaths: Seq[String],
        bucket: String,
        store: ObjectStore,
        manifestSchemaVersion: Int
    ): Either[Throwable, Seq[V2SegmentInfo]] =
      if (manifestPaths.isEmpty) Right(Seq.empty)
      else
        Left(
          new IllegalStateException(
            s"snapshot lists ${manifestPaths.size} V2 segment manifest(s) but no V2 resolver is configured"
          )
        )
  }
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
    v2: V2SegmentResolver,
    maxJsonBytes: Long = SnapshotJson.MaxBytes
) extends Logging {

  /** The snapshot at `location`: a bucket-relative key, or a `s3a://` / `s3://`
    * URI whose bucket must match this catalog's.
    */
  def read(location: String): Snapshot = {
    val located = StoragePath.parse(location, bucket)
    if (bucket.nonEmpty && located.hasBucket && located.bucket != bucket) {
      throw new IllegalArgumentException(
        s"snapshot $location is in bucket '${located.bucket}', catalog is bound to '$bucket'"
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
    val metadata = SnapshotJson.parse(json) match {
      case Right(m) => m
      case Left(e) =>
        throw new IllegalArgumentException(
          s"failed to parse snapshot metadata at $location: ${e.getMessage}",
          e
        )
    }
    SnapshotCatalog.fromMetadata(
      metadata,
      SnapshotOrigin.Catalog(location),
      store,
      bucket,
      v2
    ) match {
      case Right(s) => s
      case Left(e) =>
        throw new IllegalArgumentException(
          s"invalid snapshot at $location: ${e.getMessage}",
          e
        )
    }
  }

  /** Every snapshot metadata file of a collection, unordered. */
  def list(rootPath: String, collectionId: Long): Seq[FileInfo] = {
    val prefix = SnapshotCatalog.metadataPrefix(rootPath, collectionId)
    if (!store.exists(prefix)) Seq.empty
    else
      store.list(prefix).filter(f => !f.isDirectory && f.path.endsWith(".json"))
  }

  /** The snapshot with the latest `create_ts`. */
  def latest(rootPath: String, collectionId: Long): Snapshot =
    select(rootPath, collectionId, "latest")(_ => true)

  /** The snapshot named `name`. */
  def byName(rootPath: String, collectionId: Long, name: String): Snapshot =
    select(rootPath, collectionId, s"named '$name'")(_.name == name)

  /** The latest snapshot created at or before `timestamp`. */
  def asOf(rootPath: String, collectionId: Long, timestamp: Long): Snapshot =
    select(rootPath, collectionId, s"as of $timestamp")(
      _.createdAt.exists(_ <= timestamp)
    )

  private def select(rootPath: String, collectionId: Long, what: String)(
      keep: Snapshot => Boolean
  ): Snapshot = {
    val files = list(rootPath, collectionId)
    if (files.isEmpty) {
      throw new IllegalArgumentException(
        s"no snapshot under ${SnapshotCatalog.metadataPrefix(rootPath, collectionId)}"
      )
    }
    // The directory holds only file names; name and create_ts are inside each
    // JSON, so every candidate is opened. README section 5 asks Milvus for a
    // catalog file that would make this one read.
    val candidates = files.map(f => read(f.path)).filter(keep)
    if (candidates.isEmpty) {
      throw new IllegalArgumentException(
        s"no snapshot $what among ${files.size} under ${SnapshotCatalog
            .metadataPrefix(rootPath, collectionId)}"
      )
    }
    candidates.maxBy(_.createdAt.getOrElse(Long.MinValue))
  }
}

object SnapshotCatalog extends Logging {

  def metadataPrefix(rootPath: String, collectionId: Long): String = {
    val root = Option(rootPath).map(_.trim.stripSuffix("/")).getOrElse("")
    val head = if (root.isEmpty) "" else root + "/"
    s"${head}snapshots/$collectionId/metadata/"
  }

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
    * or schema, or listing no segments at all, is rejected.
    */
  def fromMetadata(
      metadata: SnapshotJson,
      origin: SnapshotOrigin,
      store: ObjectStore,
      bucket: String,
      v2: V2SegmentResolver
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
      return bad("snapshot is empty: no manifests and no V2 segments")
    }
    val v2Segments = v2.resolve(
      metadata.manifestList,
      bucket,
      store,
      metadata.manifestSchemaVersion
    ) match {
      case Right(segs) => segs
      case Left(e)     => return Left(e)
    }
    val schemaBytes =
      try metadata.collection.schema.toProtobufBytes
      catch { case e: Exception => return Left(e) }
    val info = metadata.snapshotInfo
    fromLists(
      name = info.name,
      collectionId = info.collectionId,
      createdAt = info.rawCreateTs.map(_ => info.createTs),
      partitionIds = info.partitionIds,
      schemaBytes = schemaBytes,
      v3Items = v3Items,
      v2Segments = v2Segments,
      bucket = bucket,
      origin = origin
    )
  }

  /** A [[Snapshot]] from its parts, for every source that already holds the V3
    * manifest entries and the materialized V2 segments: the snapshot JSON, the
    * 1.x option strings, a backup export.
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
      v2Segments: Seq[V2SegmentInfo],
      bucket: String,
      origin: SnapshotOrigin
  ): Either[Throwable, Snapshot] = {
    val defaultPartition = partitionIds.headOption
    val v3 = v3Items.map { item =>
      val (rawBasePath, version) =
        ManifestContentJson.parse(item.manifest) match {
          case Right(content) => (content.basePath, content.ver.toLong)
          case Left(_)        => (item.manifest, -1L)
        }
      val basePath =
        keyIn(bucket, rawBasePath, s"segment ${item.segmentID} manifest")
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
        deletes = DeleteFiles.InManifest
      )
    }
    val schema =
      try ProtoSchema.parseFrom(schemaBytes)
      catch { case e: Exception => return Left(e) }
    Right(
      Snapshot(
        name = name,
        collectionId = collectionId,
        createdAt = createdAt,
        schema = schema,
        partitionIds = partitionIds,
        segments = v3 ++ v2Segments.map(fromV2(_, bucket)),
        origin = origin
      )
    )
  }

  /** A path as the key the native reader takes, relative to `bucket`. */
  private def keyIn(bucket: String, path: String, what: String): String = {
    val located = StoragePath.parse(path, bucket)
    if (bucket.nonEmpty && located.hasBucket && located.bucket != bucket) {
      throw new IllegalArgumentException(
        s"$what is in bucket '${located.bucket}' but the read is bound to '$bucket': $path"
      )
    }
    located.key
  }

  /** A materialized V2 segment as a [[Segment]]. Column groups are deduplicated
    * by slot here, once, so every reader sees the same owner for a field, and
    * their file paths become keys relative to `bucket`.
    */
  def fromV2(seg: V2SegmentInfo, bucket: String): Segment = {
    val bySlot = seg.dedupColumnGroupsBySlot
    val deduped = bySlot.copy(columnGroups =
      bySlot.columnGroups.map(g =>
        g.copy(filePaths =
          g.filePaths.map(p =>
            keyIn(bucket, p, s"segment ${seg.segmentId} column group")
          )
        )
      )
    )
    if (bySlot.columnGroups != seg.columnGroups) {
      logWarning(
        s"V2 slot dedup re-attributed fields for segment ${seg.segmentId}: " +
          "overlapping fields are now read from their max-slot group. This " +
          "assumes slot ids grow with write time; if they do not, the read " +
          "may return an older group's values."
      )
    }
    Segment(
      id = seg.segmentId,
      partitionId = seg.partitionId,
      storageVersion = 2,
      rows = Some(seg.numOfRows),
      layout = SegmentLayout.ColumnGroups(deduped.columnGroups),
      deletes =
        if (seg.deltaLogs.isEmpty) DeleteFiles.Empty
        else DeleteFiles.Listed(seg.deltaLogs)
    )
  }
}
