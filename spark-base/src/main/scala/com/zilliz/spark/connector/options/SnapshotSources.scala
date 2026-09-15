package com.zilliz.spark.connector.options

import scala.util.control.NonFatal

import com.zilliz.milvus.client.api.MilvusClient
import com.zilliz.milvus.storage.compat.backup.BackupSnapshotSource
import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.milvus.storage.snapshot.{
  Snapshot,
  SnapshotCatalog,
  SnapshotOrigin,
  SnapshotSource,
  V2SegmentResolver
}
import com.zilliz.milvus.storage.snapshot.json.{SegmentListJson, SnapshotJson}

/** The [[SnapshotSource]] of a read, chosen from its options. This is where
  * compat's backup source and the client are wired in; core knows neither.
  *
  * `getTable` calls `forRead` with `withSegments = true` once and hands the
  * `Snapshot` to the table and the scan. `inferSchema` calls it with
  * `withSegments = false`: the snapshot JSON or backup meta is read for the
  * schema, but no parquet footer is opened and the result has no segments.
  */
object SnapshotSources {

  def forRead(
      milvusOption: MilvusOption,
      withSegments: Boolean
  ): SnapshotSource =
    MilvusOption.readMode(milvusOption.options) match {
      case ReadMode.Snapshot =>
        milvusOption.options
          .get(MilvusOption.SnapshotPath)
          .map(_.trim)
          .filter(_.nonEmpty) match {
          case Some(path) => catalog(milvusOption, withSegments).at(path)
          case None       => new OptionStringsSnapshotSource(milvusOption)
        }
      case ReadMode.Backup =>
        val dir = MilvusOption.backupDir(milvusOption.options).get
        new BackupSnapshotSource(
          store = StorageOptions.storeFor(
            StorageOptions.buildHadoopConfForOptions(milvusOption.options, dir),
            StorageOptions.snapshotBucket(dir).getOrElse(""),
            milvusOption.options
          ),
          backupDir = dir,
          databaseName = milvusOption.databaseName,
          collectionName = milvusOption.collectionName,
          applyDeletes = MilvusOption.readApplyDeletes(milvusOption.options),
          maxJsonBytes = StorageOptions.backupMaxJsonBytes(
            caseInsensitive(milvusOption.options)
          ),
          withSegments = withSegments
        )
      case ReadMode.Client =>
        new ClientSnapshotSource(
          milvusOption,
          catalog(milvusOption, withSegments)
        )
    }

  /** A catalog bound to the connector's bucket, reading through the driver's
    * object store; V2 segments are materialized through compat's footer
    * resolver, or skipped when the caller wants no segments.
    */
  private def catalog(
      milvusOption: MilvusOption,
      withSegments: Boolean
  ): SnapshotCatalog = {
    val bucket = StorageOptions.resolveConnectorS3Bucket(milvusOption.options)
    val store = StorageOptions.storeFor(
      StorageOptions.buildHadoopConfForOptions(milvusOption.options, ""),
      bucket,
      milvusOption.options
    )
    new SnapshotCatalog(
      store,
      bucket,
      if (withSegments)
        V2SegmentResolvers.footer(
          MilvusOption.readApplyDeletes(milvusOption.options)
        )
      else V2SegmentResolver.Skipped,
      StorageOptions.backupMaxJsonBytes(caseInsensitive(milvusOption.options))
    )
  }

  private def caseInsensitive(options: scala.collection.Map[String, String]) = {
    import scala.jdk.CollectionConverters._
    new org.apache.spark.sql.util.CaseInsensitiveStringMap(options.asJava)
  }
}

/** Client mode: the service names the collection id, the snapshot comes from
  * the snapshot directory (`milvus.client.snapshot.name` or the latest), and
  * the partition and segment selectors narrow it (capability R16).
  */
final class ClientSnapshotSource(
    milvusOption: MilvusOption,
    catalog: SnapshotCatalog
) extends SnapshotSource {

  def snapshot(): Either[Throwable, Snapshot] =
    try Right(build())
    catch { case NonFatal(e) => Left(e) }

  private def build(): Snapshot = {
    if (milvusOption.collectionName.isEmpty) {
      throw new IllegalArgumentException("collectionName cannot be empty")
    }
    val client = MilvusClient(milvusOption.connectionParams)
    try {
      val collectionInfo = client
        .getCollectionInfo(
          milvusOption.databaseName,
          milvusOption.collectionName
        )
        .getOrElse(
          throw new IllegalArgumentException(
            s"Collection ${milvusOption.collectionName} not found"
          )
        )
      val rootPath =
        milvusOption.options.getOrElse(StorageProperties.RootPath, "files")
      val snapshot = milvusOption.options
        .get(MilvusOption.ClientSnapshotName)
        .map(_.trim)
        .filter(_.nonEmpty) match {
        case Some(name) =>
          catalog.byName(rootPath, collectionInfo.collectionID, name)
        case None => catalog.latest(rootPath, collectionInfo.collectionID)
      }
      val partitionId: Option[Long] =
        if (milvusOption.partitionID.trim.nonEmpty)
          Some(
            parseId(MilvusOption.MilvusPartitionID, milvusOption.partitionID)
          )
        else if (milvusOption.partitionName.trim.nonEmpty)
          Some(
            client
              .getPartitionID(
                milvusOption.databaseName,
                milvusOption.collectionName,
                milvusOption.partitionName.trim
              )
              .getOrElse(
                throw new IllegalArgumentException(
                  s"Partition '${milvusOption.partitionName.trim}' not found in collection ${milvusOption.collectionName}"
                )
              )
          )
        else None
      val segmentId: Option[Long] =
        if (milvusOption.segmentID.trim.nonEmpty)
          Some(parseId(MilvusOption.MilvusSegmentID, milvusOption.segmentID))
        else None
      snapshot.narrow(partitionId, segmentId)
    } finally client.close()
  }

  private def parseId(key: String, raw: String): Long =
    try raw.trim.toLong
    catch {
      case _: NumberFormatException =>
        throw new IllegalArgumentException(
          s"Option '$key' must be a numeric id, got '$raw'"
        )
    }
}

/** The 1.x form: the collection id, partitions, schema and segment lists travel
  * as option strings. Kept while backfill hands its own read the segments this
  * way; it goes when backfill reads through the catalog.
  */
final class OptionStringsSnapshotSource(milvusOption: MilvusOption)
    extends SnapshotSource {

  def snapshot(): Either[Throwable, Snapshot] =
    try Right(build())
    catch { case NonFatal(e) => Left(e) }

  private def option(key: String): Option[String] =
    milvusOption.options.get(key).map(_.trim).filter(_.nonEmpty)

  private def build(): Snapshot = {
    MilvusOption.validateSnapshotModeOptions(milvusOption.options)
    val v3Items = option(MilvusOption.SnapshotManifests)
      .map { json =>
        SegmentListJson.decodeManifestItems(json) match {
          case Right(list) => list
          case Left(e) =>
            throw new IllegalArgumentException(
              s"Failed to parse snapshot manifests: ${e.getMessage}",
              e
            )
        }
      }
      .getOrElse(Seq.empty)
    val v2Segments = option(MilvusOption.SnapshotV2Segments)
      .map { json =>
        SegmentListJson.decodeV2Segments(json) match {
          case Right(segs) => segs
          case Left(e) =>
            throw new IllegalArgumentException(
              s"Failed to parse SnapshotV2Segments: ${e.getMessage}",
              e
            )
        }
      }
      .getOrElse(Seq.empty)
    val partitionIds = option(MilvusOption.SnapshotPartitionIds)
      .map(_.split(",").map(_.trim).filter(_.nonEmpty).map(_.toLong).toSeq)
      .getOrElse(Seq.empty)
    val schemaBytes = option(MilvusOption.SnapshotSchemaBytes)
      .map { base64 =>
        try java.util.Base64.getDecoder.decode(base64)
        catch {
          case NonFatal(e) =>
            throw new IllegalArgumentException(
              s"Failed to parse ${MilvusOption.SnapshotSchemaBytes}: ${e.getMessage}",
              e
            )
        }
      }
      .orElse(option(MilvusOption.SnapshotSchemaJson).map { json =>
        SnapshotJson.parse(json) match {
          case Right(metadata) => metadata.collection.schema.toProtobufBytes
          case Left(err) =>
            throw new IllegalArgumentException(
              s"Failed to parse ${MilvusOption.SnapshotSchemaJson}: $err"
            )
        }
      })
      .getOrElse(
        io.milvus.grpc.schema
          .CollectionSchema(name = milvusOption.collectionName)
          .toByteArray
      )
    val collectionId =
      option(MilvusOption.SnapshotCollectionId).map(_.toLong).getOrElse(0L)
    SnapshotCatalog.fromLists(
      name = "options",
      collectionId = collectionId,
      createdAt = None,
      partitionIds = partitionIds,
      schemaBytes = schemaBytes,
      v3Items = v3Items,
      v2Segments = v2Segments,
      bucket = StorageOptions
        .connectorS3BucketOption(milvusOption.options)
        .getOrElse(""),
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
