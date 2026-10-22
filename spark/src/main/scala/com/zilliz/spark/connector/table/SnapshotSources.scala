package com.zilliz.spark.connector.table

import java.util.Base64
import scala.collection.{Map => CollectionMap}
import scala.util.control.NonFatal

import org.apache.spark.sql.util.CaseInsensitiveStringMap

import com.zilliz.milvus.storage.io.ObjectStore
import com.zilliz.milvus.storage.snapshot.{
  Snapshot,
  SnapshotCatalog,
  SnapshotOrigin,
  SnapshotSource
}
import com.zilliz.milvus.storage.snapshot.json.{SegmentListJson, SnapshotJson}
import com.zilliz.spark.connector.options.{
  MilvusOption,
  OptionParsing,
  StorageOptions
}

/** Resolves a fixed V3 snapshot file or explicit V3 metadata. Each driver store
  * is closed after metadata is materialized, before tasks are planned.
  */
object SnapshotSources {

  def forRead(
      milvusOption: MilvusOption,
      withSegments: Boolean
  ): SnapshotSource = {
    MilvusOption.validatePublicOptions(caseInsensitive(milvusOption.options))
    MilvusOption.validateSnapshotModeOptions(milvusOption.options)
    val selectedPartitions =
      MilvusOption.selectedPartitionIds(milvusOption.options)
    val selectedSegments = MilvusOption.selectedSegmentIds(milvusOption.options)
    val source = OptionParsing
      .value(milvusOption.options, MilvusOption.SnapshotPath)
      .map(_.trim)
      .filter(_.nonEmpty) match {
      case Some(path) => catalogSource(milvusOption, path)
      case None       => new OptionStringsSnapshotSource(milvusOption)
    }
    if (withSegments) narrowed(source, selectedPartitions, selectedSegments)
    else source
  }

  private def catalogSource(
      milvusOption: MilvusOption,
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
      bucket: String,
      store: ObjectStore,
      endpoint: String
  ): SnapshotCatalog = {
    new SnapshotCatalog(
      store,
      bucket,
      StorageOptions.snapshotMaxJsonBytes(
        caseInsensitive(milvusOption.options)
      ),
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

  /** Builds one snapshot while owning one driver-side store. The store closes
    * after all metadata has been materialized, on both success and failure.
    */
  private[connector] def managedSource(
      open: => ObjectStore
  )(build: ObjectStore => Snapshot): SnapshotSource =
    SnapshotSource(StorageOptions.withStore(open)(build))

  private def caseInsensitive(options: CollectionMap[String, String]) = {
    import scala.jdk.CollectionConverters._
    new CaseInsensitiveStringMap(options.asJava)
  }
}

/** A fixed V3 snapshot described by explicit schema and manifest options. */
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
        throw new IllegalArgumentException(
          s"V3 snapshot metadata requires ${MilvusOption.SnapshotSchemaBytes} or ${MilvusOption.SnapshotSchemaJson}"
        )
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
