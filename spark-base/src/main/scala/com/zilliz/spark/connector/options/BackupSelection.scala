package com.zilliz.spark.connector.options

import com.zilliz.milvus.storage.compat.backup.BackupMetaReader

/** Which collection of a milvus-backup export a read refers to, from
  * `milvus.database.name` and `milvus.collection.name`. Used by the table at
  * init and by the backup planner; both must pick the same one.
  */
object BackupSelection {

  /** Resolve the collection to read from a backup meta, matching on
    * `milvus.database.name` + `milvus.collection.name`. Returns `Left(message)`
    * when nothing matches, when an unqualified name is ambiguous across
    * databases, or when the name is unset and the backup holds multiple
    * collections — so callers never silently read the wrong `.head`.
    */
  private[connector] def resolveBackupCollection(
      meta: BackupMetaReader.BackupInfo,
      dbName: String,
      collectionName: String
  ): Either[String, BackupMetaReader.CollectionBackup] = {
    // Milvus's default database is named "default", but older milvus-backup
    // versions / single-db clusters omit db_name from the meta (omitempty), so
    // a candidate with db_name "" or "default" both mean the default database.
    // The normalization applies to the MATCHING side only: an explicitly
    // supplied dbName (even "default") still selects the per-database branch,
    // otherwise `default.orders` + `db2.orders` would be unreachable.
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
                s"set ${MilvusOption.MilvusDatabaseName} to disambiguate"
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
              s"${MilvusOption.MilvusCollectionName} to select one"
          )
      }
    }
  }
}
