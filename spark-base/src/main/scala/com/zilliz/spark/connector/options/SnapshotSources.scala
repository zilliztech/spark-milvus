package com.zilliz.spark.connector.options

import java.util.Base64
import scala.collection.{Map => CollectionMap}
import scala.util.control.NonFatal

import org.apache.spark.sql.util.CaseInsensitiveStringMap

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
import io.milvus.grpc.schema.CollectionSchema

/** Which fixed snapshot a table load asks the client-backed source to resolve.
  * The DataSource entry keeps its configured name-or-latest behavior; Catalog
  * overloads state their request explicitly.
  */
private[connector] sealed trait SnapshotReference

private[connector] object SnapshotReference {
  case object Configured extends SnapshotReference
  case object Latest extends SnapshotReference
  final case class Named(name: String) extends SnapshotReference
  final case class AtOrBefore(hybridTimestamp: Long) extends SnapshotReference
}

/** The [[SnapshotSource]] of a read, chosen from its options. This is where
  * compat's backup source and the client are wired in; core knows neither.
  *
  * `MilvusTables` calls `forRead` with `withSegments = true` once for either
  * `getTable` or `loadTable`, then hands the `Snapshot` to the table and scan.
  * `inferSchema` calls it with `withSegments = false`: backup segment loading
  * and V2 parquet footer resolution are skipped. Catalog snapshots still read
  * segment Avro metadata to validate index descriptors; V3 layouts remain.
  */
object SnapshotSources {

  def forRead(
      milvusOption: MilvusOption,
      withSegments: Boolean,
      snapshotReference: SnapshotReference = SnapshotReference.Configured
  ): SnapshotSource = {
    rejectLegacySelectors(milvusOption)
    val selectedPartitions =
      MilvusOption.selectedPartitionIds(milvusOption.options)
    val selectedSegments =
      MilvusOption.selectedSegmentIds(milvusOption.options)
    val source = MilvusOption.readMode(milvusOption.options) match {
      case ReadMode.Snapshot =>
        requireConfiguredReference(ReadMode.Snapshot, snapshotReference)
        milvusOption.options
          .get(MilvusOption.SnapshotPath)
          .map(_.trim)
          .filter(_.nonEmpty) match {
          case Some(path) => catalogSource(milvusOption, withSegments, path)
          case None       => new OptionStringsSnapshotSource(milvusOption)
        }
      case ReadMode.Backup =>
        requireConfiguredReference(ReadMode.Backup, snapshotReference)
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
          if (
            StorageOptions
              .optionValue(milvusOption.options, StorageProperties.StorageType)
              .contains(StorageProperties.StorageTypeLocal)
          ) ""
          else StorageOptions.resolveConnectorS3Bucket(milvusOption.options)
        val conf =
          StorageOptions.buildHadoopConfForOptions(milvusOption.options, "")
        managedSource(
          StorageOptions.storeFor(conf, bucket, milvusOption.options)
        ) { store =>
          new ClientSnapshotSource(
            milvusOption,
            catalog(
              milvusOption,
              withSegments,
              bucket,
              store,
              StorageOptions.storeEndpoint(conf, bucket, milvusOption.options)
            ),
            snapshotReference
          ).snapshot().fold(throw _, identity)
        }
    }
    if (withSegments) narrowed(source, selectedPartitions, selectedSegments)
    else source
  }

  private def requireConfiguredReference(
      mode: ReadMode,
      snapshotReference: SnapshotReference
  ): Unit =
    if (snapshotReference != SnapshotReference.Configured) {
      throw new IllegalArgumentException(
        s"Catalog snapshot selection requires client mode, but the options select ${mode.toString.toLowerCase} mode; " +
          s"""use '${MilvusOption.MilvusUri}' in the catalog and keep offline snapshot or backup reads on format("milvus")"""
      )
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
    val conf =
      StorageOptions.buildHadoopConfForOptions(milvusOption.options, path)
    managedSource(
      StorageOptions.storeFor(conf, bucket, milvusOption.options)
    )(store =>
      catalog(
        milvusOption,
        withSegments,
        bucket,
        store,
        StorageOptions.storeEndpoint(conf, bucket, milvusOption.options)
      ).read(path)
    )
  }

  /** `endpoint` is the one `store` was opened with, so a Milvus-form URI is
    * recognized against the storage actually reached.
    */
  private def catalog(
      milvusOption: MilvusOption,
      withSegments: Boolean,
      bucket: String,
      store: ObjectStore,
      endpoint: String
  ): SnapshotCatalog = {
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

  private def caseInsensitive(options: CollectionMap[String, String]) = {
    import scala.jdk.CollectionConverters._
    new CaseInsensitiveStringMap(options.asJava)
  }
}

/** Client mode: the service names the collection id and the snapshot comes from
  * the snapshot directory. DataSource loads use `milvus.client.snapshot.name`
  * or latest; Catalog loads pass an explicit latest, name, or as-of reference.
  * The common source wrapper applies partition and segment selectors after
  * resolution.
  */
final class ClientSnapshotSource(
    milvusOption: MilvusOption,
    catalog: SnapshotCatalog,
    snapshotReference: SnapshotReference = SnapshotReference.Configured
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
        .fold(
          error =>
            throw new IllegalArgumentException(
              s"Cannot resolve Milvus collection '${milvusOption.databaseName}.${milvusOption.collectionName}': ${error.getMessage}",
              error
            ),
          identity
        )
      val rootPath =
        milvusOption.options.getOrElse(StorageProperties.RootPath, "files")
      val snapshot = snapshotReference match {
        case SnapshotReference.Configured =>
          milvusOption.options
            .get(MilvusOption.ClientSnapshotName)
            .map(_.trim)
            .filter(_.nonEmpty) match {
            case Some(name) =>
              catalog.byName(rootPath, collectionInfo.collectionID, name)
            case None => catalog.latest(rootPath, collectionInfo.collectionID)
          }
        case SnapshotReference.Latest =>
          catalog.latest(rootPath, collectionInfo.collectionID)
        case SnapshotReference.Named(name) if name.nonEmpty =>
          catalog.byName(rootPath, collectionInfo.collectionID, name)
        case SnapshotReference.Named(_) =>
          throw new IllegalArgumentException(
            "Snapshot version must not be empty"
          )
        case SnapshotReference.AtOrBefore(hybridTimestamp) =>
          catalog.asOf(
            rootPath,
            collectionInfo.collectionID,
            hybridTimestamp
          )
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

  private def rawOption(key: String): Option[String] =
    OptionParsing.value(milvusOption.options, key)

  private def option(key: String): Option[String] =
    rawOption(key).map(_.trim).filter(_.nonEmpty)

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
    val partitionIds = OptionParsing.nonNegativeLongList(
      rawOption,
      MilvusOption.SnapshotPartitionIds
    )
    val schemaBytes = option(MilvusOption.SnapshotSchemaBytes)
      .map { base64 =>
        try Base64.getDecoder.decode(base64)
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
        CollectionSchema(name = milvusOption.collectionName).toByteArray
      )
    val collectionId = OptionParsing.positiveLong(
      rawOption,
      MilvusOption.SnapshotCollectionId,
      defaultValue = 0L
    )
    val bucket = StorageOptions
      .connectorS3BucketOption(milvusOption.options)
      .getOrElse("")
    SnapshotCatalog.fromLists(
      name = "options",
      collectionId = collectionId,
      createdAt = None,
      partitionIds = partitionIds,
      schemaBytes = schemaBytes,
      v3Items = v3Items,
      v2Segments = v2Segments,
      bucket = bucket,
      origin = SnapshotOrigin.Options,
      endpoint = StorageOptions.storeEndpoint(
        StorageOptions.buildHadoopConfForOptions(milvusOption.options, ""),
        bucket,
        milvusOption.options
      )
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
