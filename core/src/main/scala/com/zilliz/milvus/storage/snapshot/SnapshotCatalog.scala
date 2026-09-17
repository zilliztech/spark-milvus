package com.zilliz.milvus.storage.snapshot

import java.nio.charset.StandardCharsets
import scala.util.control.NonFatal

import com.zilliz.milvus.storage.io.{FileInfo, ObjectStore}
import com.zilliz.milvus.storage.manifest.{
  AvroManifestEntry,
  SegmentManifestReader
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

/** Materializes the V2 packed segments a snapshot lists.
  *
  * A V2 segment has no manifest: its column groups come from the snapshot's
  * Avro plus each parquet footer. That code is `compat.v2`, which core must not
  * depend on (capability K1), so the catalog is handed the resolver.
  */
trait V2SegmentResolver {
  def resolve(
      entries: Seq[AvroManifestEntry],
      bucket: String,
      store: ObjectStore
  ): Either[Throwable, Seq[Segment]]
}

object V2SegmentResolver {

  /** For a caller that needs the snapshot's schema and partitions only: the V2
    * segments are left out rather than materialized. The resulting `Snapshot`
    * must not be planned against.
    */
  val Skipped: V2SegmentResolver = new V2SegmentResolver {
    def resolve(
        entries: Seq[AvroManifestEntry],
        bucket: String,
        store: ObjectStore
    ): Either[Throwable, Seq[Segment]] = Right(Seq.empty)
  }

  /** For a caller that knows the snapshot holds no V2 segments. */
  val Unavailable: V2SegmentResolver = new V2SegmentResolver {
    def resolve(
        entries: Seq[AvroManifestEntry],
        bucket: String,
        store: ObjectStore
    ): Either[Throwable, Seq[Segment]] =
      if (
        !entries.exists(entry =>
          entry.storageVersion == 2L || entry.segmentLevel == 1L
        )
      ) Right(Seq.empty)
      else
        Left(
          new IllegalStateException(
            s"snapshot lists ${entries.count(entry => entry.storageVersion == 2L || entry.segmentLevel == 1L)} V2 or L0 segment manifest(s) but no V2 resolver is configured"
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
  def read(location: String): Snapshot =
    materialize(location, metadata(location))

  /** The parsed JSON at `location`, with the bucket and size checks; opens no
    * segment file. Selection reads every candidate this far and no further. A
    * location that names a bucket must name this catalog's; over a store with
    * no bucket (the local backend) it is refused rather than read as a local
    * key.
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

  /** The snapshot the metadata describes: V2 segments are resolved here, so
    * this is where segment files are opened.
    */
  private def materialize(location: String, metadata: SnapshotJson): Snapshot =
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
    select(rootPath, collectionId, "latest", requireCreatedAt = true)((_, _) =>
      true
    )

  /** The snapshot named `name`. */
  def byName(rootPath: String, collectionId: Long, name: String): Snapshot =
    select(rootPath, collectionId, s"named '$name'", requireCreatedAt = false)(
      (snapshotName, _) => snapshotName == name
    )

  /** The latest snapshot whose raw HybridTS boundary is at or before
    * `timestamp`.
    */
  def asOf(rootPath: String, collectionId: Long, timestamp: Long): Snapshot =
    select(
      rootPath,
      collectionId,
      s"as of $timestamp",
      requireCreatedAt = true
    )((_, createdAt) => createdAt.exists(_ <= timestamp))

  private def select(
      rootPath: String,
      collectionId: Long,
      what: String,
      requireCreatedAt: Boolean
  )(
      keep: (String, Option[Long]) => Boolean
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
    // JSON, so every candidate's JSON is read (README section 5 asks Milvus for
    // a catalog file that would make this one read). Only the chosen one is
    // materialized: an older snapshot whose segment files are gone must not
    // stop a newer one from being read.
    final case class Candidate(
        path: String,
        json: SnapshotJson,
        name: String,
        createdAt: Option[Long]
    )
    val snapshots = files.map { file =>
      val json = metadata(file.path)
      val info = json.snapshotInfo
      Candidate(
        file.path,
        json,
        info.name,
        info.rawCreateTs.map(_ => info.createTs)
      )
    }
    if (requireCreatedAt) {
      val missingCreateTs = snapshots.collect {
        case c if c.createdAt.isEmpty => c.path
      }
      if (missingCreateTs.nonEmpty) {
        throw new IllegalArgumentException(
          s"cannot select snapshot $what: create_ts is missing from ${missingCreateTs.sorted.mkString(", ")}"
        )
      }
    }
    val candidates = snapshots.filter(c => keep(c.name, c.createdAt))
    if (candidates.isEmpty) {
      throw new SnapshotNotFoundException(
        s"no snapshot $what among ${files.size} under ${SnapshotCatalog
            .metadataPrefix(rootPath, collectionId)}"
      )
    }
    val missingCandidateCreateTs = candidates.collect {
      case c if c.createdAt.isEmpty => c.path
    }
    if (candidates.size > 1 && missingCandidateCreateTs.nonEmpty) {
      throw new IllegalArgumentException(
        s"cannot select snapshot $what: create_ts is missing from ${missingCandidateCreateTs.sorted
            .mkString(", ")}"
      )
    }
    val latestCreateTs = candidates.iterator
      .map(_.createdAt.getOrElse(Long.MinValue))
      .max
    val latest =
      candidates.filter(_.createdAt.getOrElse(Long.MinValue) == latestCreateTs)
    if (latest.size > 1) {
      throw new IllegalArgumentException(
        s"cannot select snapshot $what: ${latest.map(_.path).sorted.mkString(", ")} share create_ts $latestCreateTs"
      )
    }
    materialize(latest.head.path, latest.head.json)
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
    val entries =
      try {
        metadata.manifestList.map { path =>
          val key = keyIn(bucket, path, "segment snapshot Avro", endpoint)
          SegmentManifestReader.parse(
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
    val v2Segments = v2.resolve(entries, bucket, store) match {
      case Right(segs) => segs
      case Left(e)     => return Left(e)
    }
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
      v2Segments = v2Segments,
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
      entries: Seq[AvroManifestEntry],
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
    val segments = snapshot.segments.map { segment =>
      bySegment.get(segment.id).map(_.head) match {
        case None => segment
        case Some(entry) =>
          require(
            entry.partitionId == segment.partitionId,
            s"segment ${segment.id} has conflicting snapshot partition ids"
          )
          require(
            entry.storageVersion == segment.storageVersion ||
              (entry.segmentLevel == 1L && !segment.hasData),
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
              if (available.isEmpty) SegmentIndexes.Unindexed
              else SegmentIndexes.Available(available)
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
          deletes = DeleteFiles.InManifest,
          statistics = SegmentStatistics.InManifest
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
