package com.zilliz.milvus.storage.write.commit

import java.util.Locale

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}

import com.zilliz.milvus.storage.io.ObjectStore
import com.zilliz.milvus.storage.manifest.{
  AvroIndexFileEntry,
  SegmentManifestReader,
  SegmentManifestWriter
}
import com.zilliz.milvus.storage.path.StoragePath
import com.zilliz.milvus.storage.snapshot.{
  CollectionIndex,
  Snapshot,
  SnapshotOrigin
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

/** Writes the snapshot a build job produced: the snapshot it was planned
  * against, carrying the indexes it built.
  *
  * The document and the segment manifests are the source snapshot's own bytes,
  * re-encoded with the index registrations replaced. Nothing about a segment is
  * restated — its channel, its positions, its commit timestamp, its data and
  * delete files are the values Milvus wrote — and nothing about the collection
  * is synthesized, so the virtual channels, partitions, shards and properties a
  * restore needs ride along even where this connector does not model them.
  *
  * What the job owns is exactly this: the index registrations in each segment
  * manifest, the collection index definitions, the build ids, the segment id
  * list, and the snapshot's own name, id and timestamp
  * (docs/design/architecture/vector-search.html section 2.7).
  *
  * Restoring the result into a collection is Milvus's side, and asking it to is
  * not implemented here.
  */
object SnapshotWriter extends Logging {

  private val mapper = new ObjectMapper()

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

  /** The key of the document a `Snapshot` was read from, which is what a write
    * copies. A snapshot that did not come from a snapshot document has none.
    */
  def sourceKeyOf(snapshot: Snapshot, endpoint: String = ""): Option[String] =
    snapshot.origin match {
      case SnapshotOrigin.Catalog(location) =>
        Some(StoragePath.parseMilvus(location, snapshot.bucket, endpoint).key)
      case _ => None
    }

  /** @param sourceKey
    *   the snapshot document this one is written from, as a key in `store`.
    */
  def write(
      snapshot: Snapshot,
      indexes: Seq[CommittedIndex],
      target: SnapshotTarget,
      store: ObjectStore,
      sourceKey: String
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
      Option(sourceKey).exists(_.trim.nonEmpty),
      "A snapshot write copies a snapshot document, which this call has to name"
    )
    val known = snapshot.segments.map(_.id).toSet
    val unknown = indexes.map(_.segmentId).filterNot(known).distinct.sorted
    require(
      unknown.isEmpty,
      s"Index records name segment(s) the snapshot does not hold: ${unknown.mkString(", ")}"
    )

    val document = mapper.readTree(store.readAll(sourceKey)) match {
      case node: ObjectNode => node
      case other =>
        throw new IllegalArgumentException(
          s"The snapshot document at '$sourceKey' is not a JSON object: ${other.getNodeType}"
        )
    }
    val schemaVersion =
      Option(document.get("format_version")).map(_.asInt(1)).getOrElse(1)
    val sourceManifests = textsOf(document, "manifest_list")
    require(
      sourceManifests.nonEmpty,
      s"The snapshot document at '$sourceKey' names no segment manifest"
    )

    val definitions = definitionsOf(snapshot, indexes, target.collectionId)
    val bySegment = indexes.groupBy(_.segmentId)
    val rowsById =
      snapshot.segments
        .flatMap(segment => segment.rows.map(segment.id -> _))
        .toMap

    store.createDir(
      s"${prefixOf(target)}/manifests/${target.snapshotId}",
      recursive = true
    )
    store.createDir(s"${prefixOf(target)}/metadata", recursive = true)

    var written = 0L
    val segmentIds = Seq.newBuilder[Long]
    val manifestKeys = sourceManifests.map { sourceManifest =>
      val bytes = store.readAll(sourceManifest)
      val entry = SegmentManifestReader.parse(bytes, schemaVersion) match {
        case Right(value) => value
        case Left(failure) =>
          throw new IllegalArgumentException(
            s"The segment manifest at '$sourceManifest' cannot be read: ${failure.getMessage}",
            failure
          )
      }
      val rows = rowsById.getOrElse(entry.segmentId, entry.numOfRows)
      val records = bySegment
        .getOrElse(entry.segmentId, Seq.empty)
        .map(index => indexEntry(index, definitions(index.fieldId), rows))
        .toVector
      val rewritten =
        SegmentManifestWriter.rewriteIndexes(bytes, records, schemaVersion)
      val key = manifestKeyOf(target, entry.segmentId)
      store.write(key, rewritten)
      segmentIds += entry.segmentId
      written += rewritten.length.toLong
      key
    }

    describe(
      document,
      target,
      definitions,
      indexes,
      manifestKeys,
      segmentIds.result()
    )
    val metadataKey = metadataKeyOf(target)
    val metadataBytes = mapper.writeValueAsBytes(document)
    store.write(metadataKey, metadataBytes)
    written += metadataBytes.length.toLong
    logInfo(
      s"Snapshot written: name=${target.name}, id=${target.snapshotId}, " +
        s"segments=${manifestKeys.size}, indexes=${indexes.size}, bytes=$written, key=$metadataKey"
    )
    WrittenSnapshot(metadataKey, manifestKeys, written)
  }

  /** Replaces what the build job owns and leaves the rest of the document as
    * Milvus wrote it. Milvus prints its 64-bit ids as strings, and so does
    * this.
    */
  private def describe(
      document: ObjectNode,
      target: SnapshotTarget,
      definitions: Map[Long, CollectionIndex],
      indexes: Seq[CommittedIndex],
      manifestKeys: Seq[String],
      segmentIds: Seq[Long]
  ): Unit = {
    val info = Option(document.get("snapshot_info")) match {
      case Some(node: ObjectNode) => node
      case _                      => document.putObject("snapshot_info")
    }
    info.put("name", target.name)
    info.put("id", target.snapshotId.toString)
    info.put("create_ts", target.createTs.toString)

    document.set[JsonNode]("manifest_list", strings(manifestKeys))
    document.set[JsonNode]("segment_ids", strings(segmentIds.map(_.toString)))
    document.set[JsonNode](
      "build_ids",
      strings(indexes.map(_.buildId).distinct.sorted.map(_.toString))
    )
    val declared = document.putArray("indexes")
    definitions.values.toSeq.sortBy(_.fieldId).foreach { index =>
      val node = declared.addObject()
      node.put("collection_id", index.collectionId.toString)
      node.put("field_id", index.fieldId.toString)
      node.put("index_id", index.indexId.toString)
      node.put("index_name", index.name)
      pairs(node.putArray("type_params"), index.typeParameters)
      pairs(node.putArray("index_params"), index.indexParameters)
      pairs(node.putArray("user_index_params"), index.userIndexParameters)
    }
  }

  private def pairs(target: ArrayNode, values: Map[String, String]): Unit =
    values.toSeq.sortBy(_._1).foreach { case (key, value) =>
      target.addObject().put("key", key).put("value", value)
    }

  private def strings(values: Seq[String]): ArrayNode = {
    val array = mapper.createArrayNode()
    values.foreach(array.add)
    array
  }

  private def textsOf(document: ObjectNode, field: String): Seq[String] = {
    val values = Seq.newBuilder[String]
    Option(document.get(field)).filter(_.isArray).foreach { array =>
      array.elements().forEachRemaining(value => values += value.asText())
    }
    values.result().filter(_.nonEmpty)
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
      name = s"${name}_${record.indexType.toLowerCase(Locale.ROOT)}",
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
}
