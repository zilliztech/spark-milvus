package com.zilliz.spark.connector.write

import java.nio.charset.StandardCharsets
import java.nio.file.Paths
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}

import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.milvus.storage.io.{
  NativeObjectStore,
  NativeWritableObjectStore
}
import com.zilliz.milvus.storage.manifest.V3ManifestReader
import com.zilliz.milvus.storage.path.StoragePath
import com.zilliz.milvus.storage.write.{
  ColumnGroupFiles,
  ManifestTransaction,
  WriterResources
}

/** The snapshot a column replacement read its rows from and replaced columns
  * of. The schema version is recorded as the snapshot states it, zero included;
  * whether a receiver accepts that is the receiver's decision.
  */
final case class SourceSnapshot(
    path: String,
    id: Long,
    name: String,
    collectionId: Long,
    schemaVersion: Int
)

/** A collection field whose column the write replaced. */
final case class TargetColumn(name: String, fieldId: Long)

/** One segment as the commit plan records it before any manifest changes: where
  * its manifest is, the revision the new files replace columns of, and the
  * files written for it.
  */
final case class PlannedCommit(
    segmentId: Long,
    partitionId: Long,
    manifestBasePath: String,
    sourceVersion: Long,
    rowCount: Long,
    columnGroups: Seq[ColumnGroupFiles]
)

/** One segment whose new manifest revision committed. */
final case class CommittedSegment(
    planned: PlannedCommit,
    version: Long
)

/** The outcome of one segment in a write that did not complete. `Unknown` means
  * the commit was attempted and the manifest could not be inspected afterwards,
  * so the segment may or may not reference the new files.
  */
sealed abstract class SegmentState(val name: String)
object SegmentState {
  case object Committed extends SegmentState("committed")
  case object NotCommitted extends SegmentState("not_committed")
  case object Unknown extends SegmentState("unknown")
}

final case class SegmentOutcome(
    segmentId: Long,
    sourceVersion: Long,
    state: SegmentState,
    version: Option[Long],
    error: Option[String]
)

/** The receipt of one completed column replacement: which snapshot it read,
  * which fields it replaced, and for every segment the manifest revision it
  * committed. Published once, after every segment committed.
  */
final case class WriteResult(
    operationId: String,
    operation: String,
    createdAt: String,
    committedAt: String,
    applicationId: String,
    snapshot: SourceSnapshot,
    changedFields: Seq[TargetColumn],
    segments: Seq[CommittedSegment]
)

/** Where a column replacement keeps its records: the `.save()` path. Every
  * record is a key under it, read and written with `properties`.
  *
  * The path is read the way `milvus.snapshot.path` is: an absolute path or a
  * `file:` URI is a local directory, a URI with a scheme is an object-storage
  * location in the bucket it names, and a relative path is a key in the
  * snapshot's own storage. Object storage is reached with the snapshot's
  * storage identity.
  */
final case class WriteResultLocation(
    path: String,
    properties: Map[String, String],
    key: String
) {
  private def under(name: String): String = s"$key/$name"

  /** The milvus-storage manifest whose first revision claims the path. */
  def claimBasePath: String = under("_claim")
  def operationKey: String = under("_operation.json")
  def commitPlanKey: String = under("_commit_plan.json")
  def resultKey: String = under("_result.json")
  def failureKey: String = under("_failure.json")

  private[write] def bucket: String =
    properties.getOrElse(StorageProperties.BucketName, "")
}

object WriteResultLocation {

  def resolve(
      path: String,
      storage: Map[String, String]
  ): WriteResultLocation = {
    val trimmed = Option(path).map(_.trim).getOrElse("")
    require(
      trimmed.nonEmpty,
      "a column replacement needs its write result address: .save(path)"
    )
    if (trimmed.startsWith("file:") || trimmed.startsWith("/")) {
      val absolute =
        (if (trimmed.startsWith("file:"))
           Paths.get(java.net.URI.create(trimmed))
         else Paths.get(trimmed)).normalize()
      val key = absolute.toString.stripPrefix("/").stripSuffix("/")
      require(key.nonEmpty, s"the write result address cannot be / : $path")
      WriteResultLocation(
        trimmed,
        StorageProperties.from(
          Map(
            StorageProperties.StorageType -> StorageProperties.StorageTypeLocal,
            StorageProperties.RootPath -> "/"
          )
        ),
        key
      )
    } else if (!trimmed.contains("://")) {
      val key = trimmed.stripPrefix("./").stripSuffix("/")
      require(
        key.nonEmpty && !key.split('/').contains(".."),
        s"the write result address must name a key below the storage root: $path"
      )
      WriteResultLocation(trimmed, storage, key)
    } else {
      val located = StoragePath.parseMilvus(
        trimmed,
        storage.getOrElse(StorageProperties.BucketName, ""),
        storage.getOrElse(StorageProperties.Address, "")
      )
      require(
        located.bucket.nonEmpty,
        s"the write result address names no bucket: $path"
      )
      val key = located.key.stripSuffix("/")
      require(key.nonEmpty, s"the write result address names no key: $path")
      WriteResultLocation(
        trimmed,
        storage + (StorageProperties.BucketName -> located.bucket),
        key
      )
    }
  }
}

/** The records a column replacement keeps under its write result address, and
  * the claim that makes the address one operation's.
  *
  * The claim commits revision 1 of a milvus-storage manifest at `_claim`. That
  * commit is a conditional create, exactly as atomic as the segment manifest
  * commits the write itself makes, so a second operation cannot take the same
  * address. Only the operation holding the claim writes the other records:
  * `_operation.json` right after the claim, `_commit_plan.json` before any
  * segment manifest changes, then either `_result.json` once every segment
  * committed or `_failure.json`. Readers of a result read `_result.json` only.
  */
object WriteResultRecords {
  val Format = "milvus-column-write"
  val FormatVersion = 1

  private val ClaimStatistic = "column_write.operation"
  private val ClaimOperationKey = "operation_id"
  private val mapper = new ObjectMapper()

  /** Takes the address for `operationId`, or fails if any operation took it
    * before.
    *
    * A failed commit is followed by reading the claim back, because the
    * conditional create may have succeeded with its response lost: the claim
    * then names this operation and the address is ours.
    */
  def claim(location: WriteResultLocation, operationId: String): Unit = {
    try {
      ManifestTransaction.createFirst(
        location.claimBasePath,
        location.properties,
        Seq(
          ManifestTransaction.Stat(
            ClaimStatistic,
            Seq(location.operationKey),
            Map(ClaimOperationKey -> operationId)
          )
        )
      )
      ()
    } catch {
      case NonFatal(failure) =>
        val holder =
          try claimHolder(location)
          catch {
            case NonFatal(inspection) =>
              failure.addSuppressed(inspection)
              throw new IllegalStateException(
                s"Cannot claim the write result address ${location.path}: ${failure.getMessage}",
                failure
              )
          }
        holder match {
          case Some(id) if id == operationId => ()
          case Some(_) =>
            throw new IllegalStateException(
              s"The write result address ${location.path} already belongs to another column write; " +
                "give every independent write its own address",
              failure
            )
          case None =>
            throw new IllegalStateException(
              s"Cannot claim the write result address ${location.path}: ${failure.getMessage}",
              failure
            )
        }
    }
  }

  /** The operation named by the claim's first revision, if there is one. */
  private def claimHolder(location: WriteResultLocation): Option[String] =
    withStore(location) { store =>
      val latest = V3ManifestReader
        .latestManifestVersion(location.claimBasePath, location.bucket, store)
        .fold(throw _, identity)
      if (latest <= 0L) None
      else {
        val statistics = V3ManifestReader
          .loadStatistics(location.claimBasePath, 1L, location.bucket, store)
          .fold(throw _, identity)
        Some(
          statistics
            .get(ClaimStatistic)
            .flatMap(_.metadata.get(ClaimOperationKey))
            .getOrElse("")
        )
      }
    }

  def writeOperation(
      location: WriteResultLocation,
      operationId: String,
      operation: String,
      createdAt: String,
      applicationId: String,
      snapshot: SourceSnapshot,
      changedFields: Seq[TargetColumn],
      segmentCount: Int
  ): Unit = {
    val node = header("operation", operationId)
    node.put("operation", operation)
    node.put("created_at", createdAt)
    node.put("spark_application_id", applicationId)
    node.set[JsonNode]("source_snapshot", snapshotNode(snapshot))
    node.set[JsonNode]("changed_fields", fieldsNode(changedFields))
    node.put("segment_count", segmentCount)
    write(location, location.operationKey, node)
  }

  def writeCommitPlan(
      location: WriteResultLocation,
      operationId: String,
      snapshot: SourceSnapshot,
      changedFields: Seq[TargetColumn],
      commits: Seq[PlannedCommit]
  ): Unit = {
    val node = header("commit_plan", operationId)
    node.set[JsonNode]("source_snapshot", snapshotNode(snapshot))
    node.set[JsonNode]("changed_fields", fieldsNode(changedFields))
    val segments = node.putArray("segments")
    commits.foreach(commit => segments.add(plannedNode(commit)))
    write(location, location.commitPlanKey, node)
  }

  def writeResult(location: WriteResultLocation, result: WriteResult): Unit =
    write(location, location.resultKey, resultNode(result))

  def writeFailure(
      location: WriteResultLocation,
      operationId: String,
      failedAt: String,
      stage: String,
      message: String,
      outcomes: Seq[SegmentOutcome]
  ): Unit = {
    val node = header("failure", operationId)
    node.put("failed_at", failedAt)
    node.put("stage", stage)
    node.put("message", message)
    val segments = node.putArray("segments")
    outcomes.sortBy(_.segmentId).foreach { outcome =>
      val segment = segments.addObject()
      segment.put("segment_id", outcome.segmentId)
      segment.put("source_version", outcome.sourceVersion)
      segment.put("state", outcome.state.name)
      outcome.version.foreach(segment.put("version", _))
      outcome.error.foreach(segment.put("error", _))
    }
    write(location, location.failureKey, node)
  }

  /** The published receipt at `location`. A write that did not complete has
    * none; its failure record is named in the error instead.
    */
  def readResult(location: WriteResultLocation): WriteResult =
    withStore(location) { store =>
      if (!store.exists(location.resultKey)) {
        val reason =
          if (store.exists(location.failureKey))
            s"the write did not complete; its failure record is ${location.failureKey}"
          else if (store.exists(location.operationKey))
            "the write has not completed, or stopped without recording why"
          else "no column write used this address"
        throw new IllegalArgumentException(
          s"No write result at ${location.path}: $reason"
        )
      }
      parseResult(
        new String(store.readAll(location.resultKey), StandardCharsets.UTF_8)
      )
    }

  private[write] def resultNode(result: WriteResult): ObjectNode = {
    val node = header("result", result.operationId)
    node.put("operation", result.operation)
    node.put("created_at", result.createdAt)
    node.put("committed_at", result.committedAt)
    node.put("spark_application_id", result.applicationId)
    node.set[JsonNode]("source_snapshot", snapshotNode(result.snapshot))
    node.set[JsonNode]("changed_fields", fieldsNode(result.changedFields))
    val segments = node.putArray("segments")
    result.segments.foreach { committed =>
      val segment = plannedNode(committed.planned)
      segment.put("version", committed.version)
      segments.add(segment)
    }
    node
  }

  private[write] def parseResult(json: String): WriteResult = {
    val node = mapper.readTree(json)
    def text(at: JsonNode, name: String): String = {
      val value = at.get(name)
      require(
        value != null && value.isTextual,
        s"write result field '$name' must be a string"
      )
      value.asText()
    }
    def long(at: JsonNode, name: String): Long = {
      val value = at.get(name)
      require(
        value != null && value.canConvertToLong,
        s"write result field '$name' must be an integer"
      )
      value.asLong()
    }
    def array(at: JsonNode, name: String): Seq[JsonNode] = {
      val value = at.get(name)
      require(
        value != null && value.isArray,
        s"write result field '$name' must be an array"
      )
      // Strict: on Scala 2.12 an iterator's toSeq is a Stream that keeps the
      // Java iterator, and the receipt's rows are serialized into tasks.
      value.elements().asScala.toVector
    }
    require(
      text(node, "format") == Format,
      s"not a column write record: format '${text(node, "format")}'"
    )
    require(
      long(node, "format_version") == FormatVersion,
      s"unsupported column write record version ${long(node, "format_version")}"
    )
    require(
      text(node, "record") == "result",
      s"not a write result record: ${text(node, "record")}"
    )
    val snapshot = node.get("source_snapshot")
    require(snapshot != null, "write result has no source_snapshot")
    WriteResult(
      operationId = text(node, "operation_id"),
      operation = text(node, "operation"),
      createdAt = text(node, "created_at"),
      committedAt = text(node, "committed_at"),
      applicationId = text(node, "spark_application_id"),
      snapshot = SourceSnapshot(
        path = text(snapshot, "path"),
        id = long(snapshot, "id"),
        name = text(snapshot, "name"),
        collectionId = long(snapshot, "collection_id"),
        schemaVersion = long(snapshot, "schema_version").toInt
      ),
      changedFields = array(node, "changed_fields").map(field =>
        TargetColumn(text(field, "name"), long(field, "field_id"))
      ),
      segments = array(node, "segments").map { segment =>
        CommittedSegment(
          PlannedCommit(
            segmentId = long(segment, "segment_id"),
            partitionId = long(segment, "partition_id"),
            manifestBasePath = text(segment, "manifest_base_path"),
            sourceVersion = long(segment, "source_version"),
            rowCount = long(segment, "row_count"),
            columnGroups = array(segment, "column_groups").map { group =>
              val files = array(group, "files")
              ColumnGroupFiles(
                columns = array(group, "columns").map(_.asText()),
                files = files.map(text(_, "path")),
                rowCounts = files.map(long(_, "row_count")),
                format = text(group, "format")
              )
            }
          ),
          version = long(segment, "version")
        )
      }
    )
  }

  private def header(record: String, operationId: String): ObjectNode = {
    val node = mapper.createObjectNode()
    node.put("format", Format)
    node.put("format_version", FormatVersion)
    node.put("record", record)
    node.put("operation_id", operationId)
    node
  }

  private def snapshotNode(snapshot: SourceSnapshot): ObjectNode = {
    val node = mapper.createObjectNode()
    node.put("path", snapshot.path)
    node.put("id", snapshot.id)
    node.put("name", snapshot.name)
    node.put("collection_id", snapshot.collectionId)
    node.put("schema_version", snapshot.schemaVersion)
    node
  }

  private def fieldsNode(fields: Seq[TargetColumn]): ArrayNode = {
    val node = mapper.createArrayNode()
    fields.foreach { field =>
      node.addObject().put("name", field.name).put("field_id", field.fieldId)
    }
    node
  }

  private def plannedNode(commit: PlannedCommit): ObjectNode = {
    val node = mapper.createObjectNode()
    node.put("segment_id", commit.segmentId)
    node.put("partition_id", commit.partitionId)
    node.put("manifest_base_path", commit.manifestBasePath)
    node.put("source_version", commit.sourceVersion)
    node.put("row_count", commit.rowCount)
    val groups = node.putArray("column_groups")
    commit.columnGroups.foreach { group =>
      val groupNode = groups.addObject()
      val columns = groupNode.putArray("columns")
      group.columns.foreach(columns.add)
      groupNode.put("format", group.format)
      val files = groupNode.putArray("files")
      group.files.zip(group.rowCounts).foreach { case (path, rows) =>
        files.addObject().put("path", path).put("row_count", rows)
      }
    }
    node
  }

  private def write(
      location: WriteResultLocation,
      key: String,
      node: JsonNode
  ): Unit = {
    val bytes = mapper
      .writerWithDefaultPrettyPrinter()
      .writeValueAsString(node)
      .getBytes(StandardCharsets.UTF_8)
    val store = NativeWritableObjectStore.open(location.properties)
    WriterResources.closing(store.close())(store.write(key, bytes))
  }

  private def withStore[A](location: WriteResultLocation)(
      use: NativeObjectStore => A
  ): A = {
    val store = NativeObjectStore.Factory(location.properties).open()
    WriterResources.closing(store.close())(use(store))
  }
}
