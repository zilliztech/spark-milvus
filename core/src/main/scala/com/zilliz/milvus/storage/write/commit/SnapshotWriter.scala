package com.zilliz.milvus.storage.write.commit

import java.nio.charset.StandardCharsets.UTF_8

import com.fasterxml.jackson.databind.node.{IntNode, JsonNodeFactory, LongNode}
import com.fasterxml.jackson.databind.JsonNode

import com.zilliz.milvus.storage.io.ObjectStore
import com.zilliz.milvus.storage.manifest.{
  AvroIndexFileEntry,
  AvroManifestEntry,
  SegmentManifestWriter
}
import com.zilliz.milvus.storage.snapshot.{
  CollectionIndex,
  Segment,
  SegmentLayout,
  Snapshot
}
import com.zilliz.milvus.storage.snapshot.json.{
  CollectionIndexJson,
  CollectionJson,
  CollectionSchemaJson,
  FieldJson,
  KeyValueJson,
  ManifestItemJson,
  SnapshotInfoJson,
  SnapshotJson
}
import com.zilliz.milvus.storage.Logging
import io.milvus.grpc.schema.{CollectionSchema => ProtoSchema, FieldSchema}

/** Where a written snapshot goes and what names it by.
  *
  * The connector writes its own ids: `snapshotId` names the metadata object and
  * the manifest directory, and `createTs` is the Milvus HybridTS the snapshot
  * declares. Milvus assigns its own ids when it restores one.
  */
final case class SnapshotTarget(
    rootPath: String,
    collectionId: Long,
    snapshotId: Long,
    name: String,
    createTs: Long
)

/** What a snapshot write put on object storage. */
final case class WrittenSnapshot(
    metadataKey: String,
    manifestKeys: Seq[String],
    bytes: Long
)

/** Writes a snapshot: one Avro manifest per segment and the snapshot JSON that
  * names them.
  *
  * This is how a build job's output becomes something Milvus can be pointed at:
  * the segments of the snapshot the job pinned, plus the index records it
  * produced, written in the layout a snapshot read expects
  * (docs/design/architecture/vector-search.html section 2.7). Restoring the
  * result into a collection is a separate step and is not implemented.
  *
  * The segments are described from the `Snapshot` entity, so what it does not
  * carry is not invented: a segment must be V3, must know its row count, and
  * its level follows from whether it holds data. A V2 segment is refused rather
  * than written from a guess.
  */
object SnapshotWriter extends Logging {

  private val nodes = JsonNodeFactory.instance

  /** `{root}/snapshots/{collection}`, where both objects live. */
  def prefixOf(target: SnapshotTarget): String = {
    val root =
      Option(target.rootPath).map(_.trim.stripSuffix("/")).getOrElse("")
    (if (root.isEmpty) "" else root + "/") + s"snapshots/${target.collectionId}"
  }

  def metadataKeyOf(target: SnapshotTarget): String =
    s"${prefixOf(target)}/metadata/${target.snapshotId}.json"

  def manifestKeyOf(target: SnapshotTarget, segmentId: Long): String =
    s"${prefixOf(target)}/manifests/${target.snapshotId}/$segmentId.avro"

  def write(
      snapshot: Snapshot,
      indexes: Seq[CommittedIndex],
      target: SnapshotTarget,
      store: ObjectStore
  ): WrittenSnapshot = {
    require(
      target.snapshotId > 0,
      s"A snapshot id is positive, not ${target.snapshotId}"
    )
    require(target.name.trim.nonEmpty, "A snapshot carries a name")
    require(
      !Option(target.rootPath).exists(_.contains("://")),
      s"'rootPath' is a prefix inside the bucket, not a URI: '${target.rootPath}'"
    )
    require(
      snapshot.segments.nonEmpty,
      "A snapshot describes at least one segment"
    )
    val segments = snapshot.segments.map(checked)
    val known = segments.map(_.id).toSet
    val unknown = indexes.map(_.segmentId).filterNot(known).distinct.sorted
    require(
      unknown.isEmpty,
      s"Index records name segment(s) the snapshot does not hold: ${unknown.mkString(", ")}"
    )
    val definitions = definitionsOf(snapshot, indexes, target.collectionId)
    val bySegment = indexes.groupBy(_.segmentId)

    store.createDir(
      s"${prefixOf(target)}/manifests/${target.snapshotId}",
      recursive = true
    )
    store.createDir(s"${prefixOf(target)}/metadata", recursive = true)
    var written = 0L
    val manifestKeys = segments.map { segment =>
      val entry = AvroManifestEntry(
        segmentId = segment.id,
        partitionId = segment.partitionId,
        segmentLevel = if (segment.hasData) 2L else 1L,
        numOfRows = segment.rows.get,
        storageVersion = segment.storageVersion.toLong,
        binlogFiles = Seq.empty,
        deltaLogFiles = Seq.empty,
        statsLogFiles = Seq.empty,
        indexFiles = Some(
          bySegment
            .getOrElse(segment.id, Seq.empty)
            .map(index =>
              indexEntry(index, definitions(index.fieldId), segment.rows.get)
            )
            .toVector
        )
      )
      val bytes = SegmentManifestWriter.encode(
        entry,
        SegmentManifestWriter.SegmentFacts(commitTimestamp = target.createTs)
      )
      val key = manifestKeyOf(target, segment.id)
      store.write(key, bytes)
      written += bytes.length.toLong
      key
    }

    val json =
      document(snapshot, segments, indexes, definitions, manifestKeys, target)
    val metadataKey = metadataKeyOf(target)
    val metadataBytes = SnapshotJson.toJson(json).getBytes(UTF_8)
    store.write(metadataKey, metadataBytes)
    written += metadataBytes.length.toLong
    logInfo(
      s"Snapshot written: name=${target.name}, id=${target.snapshotId}, " +
        s"segments=${segments.size}, indexes=${indexes.size}, bytes=$written, key=$metadataKey"
    )
    WrittenSnapshot(metadataKey, manifestKeys, written)
  }

  /** A segment a snapshot can be written from: V3, laid out as a manifest, with
    * its row count known.
    */
  private def checked(segment: Segment): Segment = {
    segment.layout match {
      case SegmentLayout.Manifest(_, _) =>
      case _ =>
        throw new IllegalArgumentException(
          s"Segment ${segment.id} is not a V3 manifest segment; writing V2 column groups is not implemented"
        )
    }
    require(
      segment.storageVersion == 3,
      s"Segment ${segment.id} declares storage version ${segment.storageVersion}, not 3"
    )
    require(
      segment.rows.exists(_ >= 0L),
      s"Segment ${segment.id} does not say how many rows it holds"
    )
    segment
  }

  /** One index definition per indexed field: the collection's own when the
    * snapshot carries it, so that ids and names survive, and otherwise a
    * definition made from what the build reported.
    */
  private def definitionsOf(
      snapshot: Snapshot,
      indexes: Seq[CommittedIndex],
      collectionId: Long
  ): Map[Long, CollectionIndex] = indexes
    .groupBy(_.fieldId)
    .map { case (fieldId, records) =>
      val existing = snapshot.indexes.flatMap(_.find(_.fieldId == fieldId))
      fieldId -> existing.getOrElse(
        synthesized(snapshot.schema, records.head, collectionId)
      )
    }

  private def synthesized(
      schema: ProtoSchema,
      record: CommittedIndex,
      collectionId: Long
  ): CollectionIndex = {
    val field = schema.fields.find(_.fieldID == record.fieldId)
    val name = field.map(_.name).getOrElse(s"field_${record.fieldId}")
    val parameters = engineParameters(record)
    CollectionIndex(
      collectionId = collectionId,
      fieldId = record.fieldId,
      indexId = record.buildId,
      name = s"${name}_${record.indexType.toLowerCase}",
      typeParameters = field.flatMap(dimensionOf).map("dim" -> _).toMap,
      indexParameters = parameters,
      userIndexParameters = parameters
    )
  }

  private def dimensionOf(field: FieldSchema): Option[String] =
    field.typeParams.find(_.key == "dim").map(_.value)

  private def engineParameters(record: CommittedIndex): Map[String, String] =
    record.params ++ Map(
      "index_type" -> record.indexType,
      "metric_type" -> record.metricType
    )

  /** `segmentRows`, not the number of rows the build indexed: an index covers
    * the segment, and a nullable column's rows without a value are carried by
    * the `valid_data` bitmap inside the index files. A snapshot read checks the
    * two counts against each other.
    */
  private def indexEntry(
      record: CommittedIndex,
      definition: CollectionIndex,
      segmentRows: Long
  ): AvroIndexFileEntry = AvroIndexFileEntry(
    segmentId = record.segmentId,
    fieldId = record.fieldId,
    indexId = definition.indexId,
    buildId = record.buildId,
    name = definition.name,
    parameters = engineParameters(record),
    filePaths = record.filePaths.toVector,
    rowCount = segmentRows,
    serializedSize = record.serializedSize,
    indexVersion = record.indexVersion,
    currentIndexVersion = Some(record.vectorIndexVersion),
    indexStorePathVersion = Some(record.storePathVersion)
  )

  private def document(
      snapshot: Snapshot,
      segments: Seq[Segment],
      indexes: Seq[CommittedIndex],
      definitions: Map[Long, CollectionIndex],
      manifestKeys: Seq[String],
      target: SnapshotTarget
  ): SnapshotJson = {
    val partitionIds = nodes.arrayNode()
    val declared =
      if (snapshot.partitionIds.nonEmpty) snapshot.partitionIds
      else segments.map(_.partitionId).distinct
    declared.foreach(partitionIds.add)
    val dataManifests = segments.map { segment =>
      val SegmentLayout.Manifest(basePath, version) = segment.layout
      ManifestItemJson(
        segment.id,
        s"""{"ver":$version,"base_path":"$basePath"}"""
      )
    }
    val buildIds = nodes.arrayNode()
    indexes.map(_.buildId).distinct.sorted.foreach(buildIds.add)
    SnapshotJson(
      snapshotInfo = SnapshotInfoJson(
        name = target.name,
        rawId = Some(LongNode.valueOf(target.snapshotId)),
        rawCollectionId = Some(LongNode.valueOf(target.collectionId)),
        rawPartitionIds = Some(partitionIds),
        rawCreateTs = Some(LongNode.valueOf(target.createTs))
      ),
      collection = CollectionJson(schema = schemaJson(snapshot.schema)),
      formatVersion = Some(SegmentManifestWriter.CurrentSchemaVersion),
      indexes = Some(definitions.values.toSeq.sortBy(_.fieldId).map(indexJson)),
      manifestList = manifestKeys,
      storageV2ManifestList = Some(dataManifests),
      rawBuildIds = Some(buildIds)
    )
  }

  private def indexJson(index: CollectionIndex): CollectionIndexJson =
    CollectionIndexJson(
      rawCollectionId = Some(LongNode.valueOf(index.collectionId)),
      rawFieldId = Some(LongNode.valueOf(index.fieldId)),
      rawIndexId = Some(LongNode.valueOf(index.indexId)),
      name = index.name,
      typeParameters = pairs(index.typeParameters),
      indexParameters = pairs(index.indexParameters),
      userIndexParameters = pairs(index.userIndexParameters)
    )

  private def pairs(values: Map[String, String]): Seq[KeyValueJson] =
    values.toSeq.sortBy(_._1).map { case (key, value) =>
      KeyValueJson(key, value)
    }

  private def schemaJson(schema: ProtoSchema): CollectionSchemaJson =
    CollectionSchemaJson(
      name = schema.name,
      description = Option(schema.description).filter(_.nonEmpty),
      fields = schema.fields.map(fieldJson),
      autoID = Some(schema.autoID),
      enableDynamicField = Some(schema.enableDynamicField)
    )

  private def fieldJson(field: FieldSchema): FieldJson = FieldJson(
    fieldID = Some(LongNode.valueOf(field.fieldID)),
    name = field.name,
    description = Option(field.description).filter(_.nonEmpty),
    rawDataType = Some(code(field.dataType.value)),
    isPrimaryKey = Some(field.isPrimaryKey),
    isClusteringKey = Some(field.isClusteringKey),
    typeParams = Some(field.typeParams.map(p => KeyValueJson(p.key, p.value))),
    autoID = Some(field.autoID),
    rawState = Some(code(field.state.value)),
    rawElementType = Some(code(field.elementType.value)),
    isDynamic = Some(field.isDynamic),
    isPartitionKey = Some(field.isPartitionKey),
    nullable = Some(field.nullable),
    isFunctionOutput = Some(field.isFunctionOutput)
  )

  private def code(value: Int): JsonNode = IntNode.valueOf(value)
}
