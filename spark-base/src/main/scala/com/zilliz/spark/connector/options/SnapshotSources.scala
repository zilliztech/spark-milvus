package com.zilliz.spark.connector.options

import scala.util.control.NonFatal

import com.zilliz.milvus.client.api.MilvusClient
import com.zilliz.milvus.storage.compat.backup.BackupSnapshotSource
import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.milvus.storage.io.ObjectStore
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
  ): SnapshotSource = {
    rejectLegacySelectors(milvusOption)
    val selectedPartitions =
      MilvusOption.selectedPartitionIds(milvusOption.options)
    val selectedSegments =
      MilvusOption.selectedSegmentIds(milvusOption.options)
    val source = MilvusOption.readMode(milvusOption.options) match {
      case ReadMode.Snapshot =>
        milvusOption.options
          .get(MilvusOption.SnapshotPath)
          .map(_.trim)
          .filter(_.nonEmpty) match {
          case Some(path) => catalogSource(milvusOption, withSegments, path)
          case None       => new OptionStringsSnapshotSource(milvusOption)
        }
      case ReadMode.Backup =>
        val dir = MilvusOption.backupDir(milvusOption.options).get
        val bucket = StorageOptions
          .snapshotS3BucketForRelativePaths(dir, milvusOption.options)
          .getOrElse("")
        managedSource(
          StorageOptions.storeFor(
            StorageOptions.buildHadoopConfForOptions(milvusOption.options, dir),
            bucket,
            milvusOption.options
          )
        ) { store =>
          new BackupSnapshotSource(
            store = store,
            backupDir = dir,
            databaseName = milvusOption.databaseName,
            collectionName = milvusOption.collectionName,
            applyDeletes = MilvusOption.readApplyDeletes(milvusOption.options),
            maxJsonBytes = StorageOptions.backupMaxJsonBytes(
              caseInsensitive(milvusOption.options)
            ),
            withSegments = withSegments
          ).snapshot().fold(throw _, identity)
        }
      case ReadMode.Client =>
        val bucket =
          StorageOptions.resolveConnectorS3Bucket(milvusOption.options)
        managedSource(
          StorageOptions.storeFor(
            StorageOptions.buildHadoopConfForOptions(milvusOption.options, ""),
            bucket,
            milvusOption.options
          )
        ) { store =>
          new ClientSnapshotSource(
            milvusOption,
            catalog(milvusOption, withSegments, bucket, store)
          ).snapshot().fold(throw _, identity)
        }
    }
    if (withSegments) narrowed(source, selectedPartitions, selectedSegments)
    else source
  }

  /** A catalog bound to the connector's bucket, reading through the driver's
    * object store; V2 segments are materialized through compat's footer
    * resolver, or skipped when the caller wants no segments.
    */
  private def catalogSource(
      milvusOption: MilvusOption,
      withSegments: Boolean,
      path: String
  ): SnapshotSource = {
    val bucket = StorageOptions
      .snapshotS3BucketForRelativePaths(path, milvusOption.options)
      .getOrElse("")
    managedSource(
      StorageOptions.storeFor(
        StorageOptions.buildHadoopConfForOptions(milvusOption.options, path),
        bucket,
        milvusOption.options
      )
    )(store => catalog(milvusOption, withSegments, bucket, store).read(path))
  }

  private def catalog(
      milvusOption: MilvusOption,
      withSegments: Boolean,
      bucket: String,
      store: ObjectStore
  ): SnapshotCatalog = {
    val endpoint =
      StorageOptions.effectiveEndpoint(milvusOption.options).getOrElse("")
    new SnapshotCatalog(
      store,
      bucket,
      if (withSegments)
        V2SegmentResolvers.footer(
          MilvusOption.readApplyDeletes(milvusOption.options),
          endpoint
        )
      else V2SegmentResolver.Skipped,
      StorageOptions.backupMaxJsonBytes(caseInsensitive(milvusOption.options)),
      endpoint
    )
  }

  private[connector] def narrowed(
      source: SnapshotSource,
      partitionIds: Seq[Long],
      segmentIds: Seq[Long]
  ): SnapshotSource =
    SnapshotSource(
      source
        .snapshot()
        .fold(throw _, identity)
        .narrow(partitionIds, segmentIds)
    )

  private def rejectLegacySelectors(milvusOption: MilvusOption): Unit = {
    val legacy = Seq(
      MilvusOption.MilvusPartitionName -> milvusOption.partitionName,
      MilvusOption.MilvusPartitionID -> milvusOption.partitionID,
      MilvusOption.MilvusSegmentID -> milvusOption.segmentID
    ).collect { case (key, value) if value.trim.nonEmpty => key }
    if (legacy.nonEmpty) {
      throw new IllegalArgumentException(
        s"Legacy read selector(s) ${legacy.mkString(", ")} are not supported; " +
          s"use '${MilvusOption.MilvusPartitions}' and '${MilvusOption.MilvusSegments}' with comma-separated numeric ids"
      )
    }
  }

  /** Builds one snapshot while owning one driver-side store. The store closes
    * after all metadata has been materialized, on both success and failure.
    */
  private[connector] def managedSource(
      open: => ObjectStore
  )(build: ObjectStore => Snapshot): SnapshotSource =
    SnapshotSource(withStore(open)(build))

  private[connector] def withStore[A](
      open: => ObjectStore
  )(use: ObjectStore => A): A = {
    val store = open
    useAndClose(store.close())(use(store))
  }

  private[connector] def useAndClose[A](close: => Unit)(use: => A): A = {
    var primaryFailure: Throwable = null
    try use
    catch {
      case failure: Throwable =>
        primaryFailure = failure
        throw failure
    } finally {
      try close
      catch {
        case closeFailure: Throwable =>
          if (primaryFailure == null) throw closeFailure
          if (closeFailure ne primaryFailure)
            primaryFailure.addSuppressed(closeFailure)
      }
    }
  }

  private def caseInsensitive(options: scala.collection.Map[String, String]) = {
    import scala.jdk.CollectionConverters._
    new org.apache.spark.sql.util.CaseInsensitiveStringMap(options.asJava)
  }
}

/** Client mode: the service names the collection id, the snapshot comes from
  * the snapshot directory (`milvus.client.snapshot.name` or the latest), and
  * the common source wrapper applies partition and segment selectors after it
  * resolves.
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
    SnapshotSources.useAndClose(client.close()) {
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
      snapshot
    }
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
      origin = SnapshotOrigin.Options,
      endpoint = StorageOptions
        .effectiveEndpoint(milvusOption.options)
        .getOrElse("")
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
