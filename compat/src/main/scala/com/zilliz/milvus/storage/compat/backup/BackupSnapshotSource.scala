package com.zilliz.milvus.storage.compat.backup

import scala.util.control.NonFatal

import com.zilliz.milvus.storage.io.ObjectStore
import com.zilliz.milvus.storage.path.StoragePath
import com.zilliz.milvus.storage.snapshot.{
  Segment,
  Snapshot,
  SnapshotCatalog,
  SnapshotOrigin,
  SnapshotSource
}

/** A milvus-backup binlog-format export as a [[Snapshot]] (capability K3).
  *
  * `full_meta.json` names the collections, their schemas and their segments;
  * the segments become the same `Segment` a snapshot read plans against, so
  * delete planning and the V2 reader are shared with the snapshot path. Object
  * paths are rebuilt from `backupDir` plus the ids in the meta; the meta's own
  * `log_path` values are the source cluster's keys and are never opened.
  *
  * @param withSegments
  *   false for a caller that needs only the schema and collection id (schema
  *   inference): the parquet footers are then not read and the snapshot has no
  *   segments, so it must not be planned against.
  */
final class BackupSnapshotSource(
    store: ObjectStore,
    backupDir: String,
    databaseName: String,
    collectionName: String,
    applyDeletes: Boolean,
    maxJsonBytes: Long,
    withSegments: Boolean
) extends SnapshotSource {

  def snapshot(): Either[Throwable, Snapshot] =
    try Right(build())
    catch { case NonFatal(e) => Left(e) }

  private def build(): Snapshot = {
    val bucket = StoragePath.parse(backupDir).bucket
    if (withSegments && bucket.isEmpty) {
      // The native reader requires object storage: with a local dir it would
      // fail on every task, so refuse here rather than after planning.
      throw new IllegalArgumentException(
        s"milvus.backup.dir must be an object storage URI (s3a://...) for " +
          s"reads; got '$backupDir'"
      )
    }
    val meta = BackupMetaReader.readMeta(store, backupDir, maxJsonBytes) match {
      case Right(m) => m
      case Left(err) =>
        throw new IllegalArgumentException(
          s"Failed to parse backup meta at ${BackupMetaReader
              .metaPath(backupDir)}: ${err.getMessage}",
          err
        )
    }
    if (meta.isSnapshotFormat) {
      throw new IllegalArgumentException(
        s"Backup '${meta.name}' is in snapshot format; only binlog-format " +
          "backups can be read as a datasource"
      )
    }
    val coll = BackupSnapshotSource.selectCollection(
      meta,
      databaseName,
      collectionName
    ) match {
      case Right(c)  => c
      case Left(msg) => throw new IllegalArgumentException(msg)
    }
    val schemaBytes = coll.schema
      .map(BackupMetaReader.toProtobufSchemaBytes)
      .getOrElse {
        throw new IllegalArgumentException(
          s"Backup '${meta.name}' meta has no schema for collection " +
            s"'${coll.collectionName}'; cannot read it"
        )
      }
    val segments: Seq[Segment] =
      if (!withSegments) Seq.empty
      else
        BackupMetaReader.toV2Segments(
          meta,
          store,
          backupDir,
          applyDeletes,
          coll.collectionId
        ) match {
          case Right(segs) => segs
          case Left(err) =>
            throw new IllegalStateException(
              s"Failed to load StorageV2 segments from backup: ${err.getMessage}",
              err
            )
        }
    val snapshot = SnapshotCatalog.fromLists(
      name = meta.name,
      collectionId = coll.collectionId,
      createdAt = None,
      partitionIds = segments.map(_.partitionId).distinct,
      schemaBytes = schemaBytes,
      v3Items = Seq.empty,
      v2Segments = segments,
      bucket = bucket,
      origin = SnapshotOrigin.Backup(backupDir)
    ) match {
      case Right(s) => s
      case Left(e) =>
        throw new IllegalArgumentException(
          s"Backup '${meta.name}' is not readable: ${e.getMessage}",
          e
        )
    }
    if (withSegments) {
      if (snapshot.primaryKeyField.isEmpty) {
        throw new IllegalArgumentException(
          s"Backup '${meta.name}' collection '${coll.collectionName}' schema " +
            "has no primary key; cannot plan a read"
        )
      }
      // Delete-only (L0) segments carry no column groups: a backup holding
      // only those would otherwise plan zero partitions and read zero rows.
      if (snapshot.dataSegments.isEmpty) {
        throw new IllegalArgumentException(
          s"Backup '${meta.name}' collection '${coll.collectionName}' has no " +
            "StorageV2 data segments to read (only delete-only segments). " +
            "This connector requires Milvus 2.6+ with Storage V2; ensure the " +
            "collection has been flushed and contains data."
        )
      }
    }
    snapshot
  }
}

object BackupSnapshotSource {

  /** The collection of a backup a read refers to, from `milvus.database.name`
    * and `milvus.collection.name`. `Left(message)` when nothing matches, when
    * an unqualified name is ambiguous across databases, or when the name is
    * unset and the backup holds several collections: never a silent `.head`.
    */
  def selectCollection(
      meta: BackupMetaReader.BackupInfo,
      dbName: String,
      collectionName: String
  ): Either[String, BackupMetaReader.CollectionBackup] = {
    // Milvus's default database is "default", but older milvus-backup
    // versions and single-db clusters omit db_name from the meta, so "" and
    // "default" both mean the default database on the matching side. An
    // explicitly supplied dbName (even "default") still selects the
    // per-database branch, or `default.orders` and `db2.orders` could not be
    // told apart.
    def normalizedDb(d: String): String = {
      val t = Option(d).map(_.trim).getOrElse("")
      if (t == "default") "" else t
    }
    def qualified(c: BackupMetaReader.CollectionBackup): String = {
      val db = normalizedDb(c.dbName)
      s"${if (db.nonEmpty) db + "." else ""}${c.collectionName}"
    }
    val rawDb = Option(dbName).map(_.trim).getOrElse("")
    val reqDb = normalizedDb(dbName)
    if (collectionName.nonEmpty) {
      val candidates =
        meta.collectionBackups.filter(_.collectionName == collectionName)
      if (rawDb.nonEmpty) {
        candidates.find(c => normalizedDb(c.dbName) == reqDb) match {
          case Some(coll) => Right(coll)
          case None =>
            Left(
              s"Backup '${meta.name}' does not contain collection " +
                s"'$collectionName' in database '$dbName'; available: " +
                meta.collectionBackups.map(qualified).mkString(",")
            )
        }
      } else {
        candidates match {
          case Seq(single) => Right(single)
          case Seq() =>
            Left(
              s"Backup '${meta.name}' does not contain collection " +
                s"'$collectionName'; available: " +
                meta.collectionBackups.map(qualified).mkString(",")
            )
          case many =>
            Left(
              s"Backup '${meta.name}' has multiple collections named " +
                s"'$collectionName' (${many.map(qualified).mkString(",")}); " +
                "set milvus.database.name to disambiguate"
            )
        }
      }
    } else {
      meta.collectionBackups match {
        case Seq(single) => Right(single)
        case _ =>
          Left(
            s"Backup '${meta.name}' contains " +
              s"${meta.collectionBackups.size} collection(s); set " +
              "milvus.collection.name to select one"
          )
      }
    }
  }
}
