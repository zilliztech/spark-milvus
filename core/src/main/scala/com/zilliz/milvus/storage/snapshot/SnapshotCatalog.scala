package com.zilliz.milvus.storage.snapshot

import java.nio.charset.StandardCharsets
import scala.util.control.NonFatal

import com.zilliz.milvus.storage.io.{FileInfo, ObjectStore}
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
  ): Either[Throwable, Seq[Segment]]
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
    ): Either[Throwable, Seq[Segment]] = Right(Seq.empty)
  }

  /** For a caller that knows the snapshot holds no V2 segments. */
  val Unavailable: V2SegmentResolver = new V2SegmentResolver {
    def resolve(
        manifestPaths: Seq[String],
        bucket: String,
        store: ObjectStore,
        manifestSchemaVersion: Int
    ): Either[Throwable, Seq[Segment]] =
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
    maxJsonBytes: Long = SnapshotJson.MaxBytes,
    endpoint: String = ""
) extends Logging {

  /** The snapshot at `location`: a bucket-relative key, a `s3a://` / `s3://`
    * URI whose bucket must match this catalog's, or the
    * `scheme://endpoint/bucket/key` form Milvus prints for its own snapshots
    * (CreateSnapshot's `s3_location`), recognised only when the host is this
    * catalog's endpoint.
    */
  def read(location: String): Snapshot = {
    val located = StoragePath.parseMilvus(location, bucket, endpoint)
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
      v2,
      endpoint
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
  /** [[SnapshotSource]]s over this catalog, one per way of choosing. */
  def at(location: String): SnapshotSource = SnapshotSource(read(location))

  def latestOf(rootPath: String, collectionId: Long): SnapshotSource =
    SnapshotSource(latest(rootPath, collectionId))

  def named(
      rootPath: String,
      collectionId: Long,
      name: String
  ): SnapshotSource =
    SnapshotSource(byName(rootPath, collectionId, name))

  def latest(rootPath: String, collectionId: Long): Snapshot =
    select(rootPath, collectionId, "latest", requireCreatedAt = true)(_ => true)

  /** The snapshot named `name`. */
  def byName(rootPath: String, collectionId: Long, name: String): Snapshot =
    select(rootPath, collectionId, s"named '$name'", requireCreatedAt = false)(
      _.name == name
    )

  /** The latest snapshot created at or before `timestamp`. */
  def asOf(rootPath: String, collectionId: Long, timestamp: Long): Snapshot =
    select(
      rootPath,
      collectionId,
      s"as of $timestamp",
      requireCreatedAt = true
    )(
      _.createdAt.exists(_ <= timestamp)
    )

  private def select(
      rootPath: String,
      collectionId: Long,
      what: String,
      requireCreatedAt: Boolean
  )(
      keep: Snapshot => Boolean
  ): Snapshot = {
    val files = list(rootPath, collectionId)
    if (files.isEmpty) {
      throw new IllegalArgumentException(
        s"no snapshot under ${SnapshotCatalog.metadataPrefix(rootPath, collectionId)}; " +
          "the prefix is fs.root_path plus snapshots/<collection id>/metadata/, " +
          "and fs.root_path must be the Milvus minio.rootPath (an instance id " +
          "on Zilliz Cloud, files on a default self-managed Milvus)"
      )
    }
    // The directory holds only file names; name and create_ts are inside each
    // JSON, so every candidate is opened. README section 5 asks Milvus for a
    // catalog file that would make this one read.
    val snapshots = files.map(file => file.path -> read(file.path))
    if (requireCreatedAt) {
      val missingCreateTs = snapshots.collect {
        case (path, snapshot) if snapshot.createdAt.isEmpty => path
      }
      if (missingCreateTs.nonEmpty) {
        throw new IllegalArgumentException(
          s"cannot select snapshot $what: create_ts is missing from ${missingCreateTs.sorted.mkString(", ")}"
        )
      }
    }
    val candidates = snapshots.filter { case (_, snapshot) => keep(snapshot) }
    if (candidates.isEmpty) {
      throw new IllegalArgumentException(
        s"no snapshot $what among ${files.size} under ${SnapshotCatalog
            .metadataPrefix(rootPath, collectionId)}"
      )
    }
    val missingCandidateCreateTs = candidates.collect {
      case (path, snapshot) if snapshot.createdAt.isEmpty => path
    }
    if (candidates.size > 1 && missingCandidateCreateTs.nonEmpty) {
      throw new IllegalArgumentException(
        s"cannot select snapshot $what: create_ts is missing from ${missingCandidateCreateTs.sorted
            .mkString(", ")}"
      )
    }
    val latestCreateTs = candidates.iterator.map { case (_, snapshot) =>
      snapshot.createdAt.getOrElse(Long.MinValue)
    }.max
    val latest = candidates.filter { case (_, snapshot) =>
      snapshot.createdAt.getOrElse(Long.MinValue) == latestCreateTs
    }
    if (latest.size > 1) {
      throw new IllegalArgumentException(
        s"cannot select snapshot $what: ${latest.map(_._1).sorted.mkString(", ")} share create_ts $latestCreateTs"
      )
    }
    latest.head._2
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
      v2: V2SegmentResolver,
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
      origin = origin,
      endpoint = endpoint
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
      v2Segments: Seq[Segment],
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
          deletes = DeleteFiles.InManifest
        )
      }
      val schema =
        try ProtoSchema.parseFrom(schemaBytes)
        catch { case e: Exception => return Left(e) }
      validateDynamicFieldSchema(name, schema)
      val segments = v3 ++ v2Segments.map(normalizeV2(_, bucket, endpoint))
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

  /** A materialized V2 segment as a [[Segment]]. Column groups are deduplicated
    * by slot here, once, so every reader sees the same owner for a field, and
    * their file paths become keys relative to `bucket`.
    */
  /** A V2 segment as the read needs it: column groups deduplicated by slot and
    * every file path a key relative to `bucket`.
    */
  def normalizeV2(
      seg: Segment,
      bucket: String,
      endpoint: String = ""
  ): Segment = {
    val bySlot = seg.dedupColumnGroupsBySlot
    if (bySlot.columnGroups != seg.columnGroups) {
      logWarning(
        s"V2 slot dedup re-attributed fields for segment ${seg.id}: " +
          "overlapping fields are now read from their max-slot group. This " +
          "assumes slot ids grow with write time; if they do not, the read " +
          "may return an older group's values."
      )
    }
    val normalizedDeletes = bySlot.deletes match {
      case DeleteFiles.Listed(files) =>
        DeleteFiles.Listed(
          files.map(file =>
            file.copy(logPath =
              keyIn(
                bucket,
                file.logPath,
                s"segment ${seg.id} delete log",
                endpoint
              )
            )
          )
        )
      case other => other
    }
    bySlot.copy(
      layout = SegmentLayout.ColumnGroups(
        bySlot.columnGroups.map(g =>
          g.copy(filePaths =
            g.filePaths.map(p =>
              keyIn(
                bucket,
                p,
                s"segment ${seg.id} column group",
                endpoint
              )
            )
          )
        )
      ),
      deletes = normalizedDeletes
    )
  }
}
