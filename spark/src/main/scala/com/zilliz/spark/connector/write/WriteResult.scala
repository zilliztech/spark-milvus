package com.zilliz.spark.connector.write

import java.nio.charset.StandardCharsets
import java.nio.file.Paths
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import com.fasterxml.jackson.databind.{
  DeserializationFeature,
  JsonNode,
  ObjectMapper
}
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}

import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.milvus.storage.io.{
  NativeObjectStore,
  NativeWritableObjectStore,
  WritableObjectStore
}
import com.zilliz.milvus.storage.path.StoragePath
import com.zilliz.milvus.storage.write.{ColumnGroupFiles, WriterResources}

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
  * storage did not acknowledge the commit and the driver cannot establish
  * whether the new revision belongs to this write.
  */
sealed abstract class SegmentState(val name: String)
object SegmentState {
  case object Committed extends SegmentState("committed")
  case object NotCommitted extends SegmentState("not_committed")
  // Skipped and rejected commits share the on-disk state. A skipped segment
  // never submitted its manifest transaction.
  private[write] case object NotAttempted
      extends SegmentState(NotCommitted.name)
  case object Unknown extends SegmentState("unknown")
}

final case class SegmentOutcome(
    segmentId: Long,
    sourceVersion: Long,
    state: SegmentState,
    version: Option[Long],
    error: Option[String]
) {
  private[write] def successful: Boolean =
    state == SegmentState.Committed && error.isEmpty
}

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

  /** The first manifest revision that claimed an address under the earlier
    * protocol. An operation also creates it, holding a guard record, so that
    * connectors still using that protocol find the address occupied.
    */
  private[write] def legacyClaimKey: String =
    under("_claim/_metadata/manifest-1.avro")

  /** The conditionally created record that gives the path to one operation. */
  def operationKey: String = under("_operation.json")
  def commitPlanKey: String = under("_commit_plan.json")
  def resultKey: String = under("_result.json")
  def failureKey: String = under("_failure.json")

  /** The properties of a store that writes one record of `size` bytes.
    *
    * Remote records are uploaded by one request: a write smaller than one part
    * is buffered and sent when its writer closes, so a failed upload leaves no
    * partial object and the backend decides a conditional create in that one
    * request. The CRT transport would stream every upload as a multipart upload
    * instead. Local records synchronize directories up to the write result
    * address, not up to the root of its namespace; the address is the directory
    * that the store itself resolves the key to, whether or not the key carries
    * `fs.root_path`.
    */
  private[write] def recordProperties(size: Long): Map[String, String] =
    NativeObjectStore.localPath(properties, key) match {
      case Some(directory) =>
        properties + (StorageProperties.LocalDurabilityRoot -> directory)
      case None =>
        properties ++ Map(
          StorageProperties.S3UseCrt -> "false",
          StorageProperties.MultiPartUploadSize ->
            WriteResultLocation.singleRequestPartSize(size).toString
        )
    }
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

  // The native layer accepts part sizes from 10 MiB to 5 GiB.
  private val MinPartSize = 10L * 1024 * 1024
  private val MaxPartSize = 5L * 1024 * 1024 * 1024

  /** The part size that sends `size` bytes as one request: a writer whose
    * buffered bytes reach a part starts a multipart upload. Doubling from the
    * minimum keeps the number of distinct native configurations, and so of
    * cached filesystems, small.
    */
  private[write] def singleRequestPartSize(size: Long): Long = {
    require(
      size >= 0 && size < MaxPartSize,
      s"a column write record of $size bytes does not fit in one upload request"
    )
    var part = MinPartSize
    while (part <= size) part *= 2
    math.min(part, MaxPartSize)
  }
}

/** The records a column replacement keeps under its write result address, and
  * the claim that makes the address one operation's.
  *
  * The claim conditionally creates `_operation.json` with the operation id. The
  * backend must support atomic creation; an existence check cannot replace it.
  * The claimant then conditionally creates a guard record at the earlier
  * protocol's claim key, so that connectors still using that protocol find the
  * address occupied. Only the operation holding the claim writes the other
  * records: `_commit_plan.json` before any segment manifest changes, then
  * either `_result.json` once every segment committed or `_failure.json`. The
  * receipt is also conditionally created. Every record is uploaded by one
  * request. Readers of a result read `_result.json` only.
  */
object WriteResultRecords {
  val Format = "milvus-column-write"
  val FormatVersion = 1
  private val LegacyClaimGuard = "legacy_claim_guard"

  private val mapper = new ObjectMapper()
  private val recordReader =
    mapper.reader().`with`(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)

  /** Borrows the store to conditionally create an immutable record. A failed
    * create may have succeeded with its response lost; only the complete
    * expected record confirms success. Store cleanup is outside recovery.
    */
  private[write] def createRecord(
      location: WriteResultLocation,
      key: String,
      record: ObjectNode,
      store: WritableObjectStore
  ): Unit = createRecord(location, key, record, encode(record), store)

  private def createRecord(
      location: WriteResultLocation,
      key: String,
      record: ObjectNode,
      bytes: Array[Byte],
      store: WritableObjectStore
  ): Unit = {
    val recordType =
      if (key == location.operationKey) "operation"
      else if (key == location.resultKey) "result"
      else {
        require(
          key == location.legacyClaimKey,
          "only operation, claim guard and result records are immutable"
        )
        LegacyClaimGuard
      }
    val operationId = recordOwner(record, recordType)
    if (key == location.operationKey) rejectEarlierClaim(location, store)
    // Parse both sides alike: small LongNode values become IntNodes in JSON.
    val expected = mapper.readTree(bytes)
    try {
      store.create(key, bytes)
    } catch {
      case unsupported: UnsupportedOperationException => throw unsupported
      case NonFatal(failure) =>
        val stored =
          try if (store.exists(key)) Some(store.readAll(key)) else None
          catch {
            case NonFatal(inspection) =>
              if (failure ne inspection) failure.addSuppressed(inspection)
              throw new IllegalStateException(
                s"Cannot create the $recordType record at $key: ${failure.getMessage}",
                failure
              )
          }
        stored.map(parseRecord(_, recordType)) match {
          case Some(Right(found)) if found == expected => ()
          case Some(Right(found))
              if found.path("operation_id").asText() != operationId =>
            throw new IllegalStateException(
              s"The write result address ${location.path} already belongs to another column write; " +
                "give every independent write its own address",
              failure
            )
          case Some(Right(_)) =>
            throw new IllegalStateException(
              s"Cannot create the $recordType record at $key: the existing record for " +
                s"operation $operationId differs from the attempted publication",
              failure
            )
          case Some(Left(_)) if key == location.legacyClaimKey =>
            // An earlier connector claimed the address between our claim and
            // this guard. Its manifest is neither interpreted nor extended.
            throw earlierClaim(location, failure)
          case Some(Left(invalid)) =>
            failure.addSuppressed(invalid)
            throw new IllegalStateException(
              s"Cannot create the $recordType record at $key: ${failure.getMessage}",
              failure
            )
          case None =>
            throw new IllegalStateException(
              s"Cannot create the $recordType record at $key: ${failure.getMessage}",
              failure
            )
        }
    }
  }

  /** An address claimed under the earlier protocol stays that operation's: an
    * object other than a guard record at its claim key rejects the address,
    * without interpreting or extending its manifest. A guard belongs to an
    * operation of this protocol, whose own record then decides the claim.
    */
  private def rejectEarlierClaim(
      location: WriteResultLocation,
      store: WritableObjectStore
  ): Unit =
    if (
      store.exists(location.legacyClaimKey) &&
      parseRecord(
        store.readAll(location.legacyClaimKey),
        LegacyClaimGuard
      ).isLeft
    ) throw earlierClaim(location, null)

  private def earlierClaim(
      location: WriteResultLocation,
      cause: Throwable
  ): IllegalStateException =
    new IllegalStateException(
      s"The write result address ${location.path} already belongs to another column write; " +
        "its claim was created by an earlier connector version",
      cause
    )

  private def parseRecord(
      bytes: Array[Byte],
      recordType: String
  ): Either[Throwable, JsonNode] =
    try {
      val node = recordReader.readTree(bytes)
      recordOwner(node, recordType)
      Right(node)
    } catch { case NonFatal(invalid) => Left(invalid) }

  /** Validates the header that every record has and returns its operation id.
    * Recovery and readers apply the same rules, so a record that one of them
    * rejects is never served by the other.
    */
  private def recordOwner(node: JsonNode, recordType: String): String = {
    require(
      node != null && node.isObject,
      "the write result record must be an object"
    )
    val format = node.path("format")
    require(
      format.isTextual && format.asText() == Format,
      s"not a column write record: format '${format.asText()}'"
    )
    val version = node.path("format_version")
    require(
      version.isIntegralNumber && version.canConvertToInt &&
        version.asInt() == FormatVersion,
      s"unsupported column write record version ${version.asText()}"
    )
    val kind = node.path("record")
    val label =
      if (recordType == "result") "write result"
      else recordType.replace('_', ' ')
    require(
      kind.isTextual && kind.asText() == recordType,
      s"not a $label record: ${kind.asText()}"
    )
    val owner = node.path("operation_id")
    require(
      owner.isTextual && owner.asText().trim.nonEmpty,
      "the write result record needs a nonempty operation id"
    )
    owner.asText()
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
    create(location, location.operationKey, node)
    create(
      location,
      location.legacyClaimKey,
      header(LegacyClaimGuard, operationId)
    )
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
    create(location, location.resultKey, resultNode(result))

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
      if (
        outcome.state == SegmentState.Unknown && outcome.sourceVersion < Long.MaxValue
      ) {
        segment.put("candidate_version", outcome.sourceVersion + 1L)
      }
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
          else if (
            store.exists(location.operationKey) || store.exists(
              location.legacyClaimKey
            )
          )
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
    val node = recordReader.readTree(json)
    val operationId = recordOwner(node, "result")
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
    val snapshot = node.get("source_snapshot")
    require(snapshot != null, "write result has no source_snapshot")
    WriteResult(
      operationId = operationId,
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

  /** Every record has one encoding; recovery and readers compare parsed JSON,
    * never bytes.
    */
  private def encode(node: JsonNode): Array[Byte] =
    mapper.writeValueAsBytes(node)

  private def create(
      location: WriteResultLocation,
      key: String,
      node: ObjectNode
  ): Unit = {
    val bytes = encode(node)
    withWritableStore(location, bytes.length)(
      createRecord(location, key, node, bytes, _)
    )
  }

  private def write(
      location: WriteResultLocation,
      key: String,
      node: JsonNode
  ): Unit = {
    val bytes = encode(node)
    withWritableStore(location, bytes.length)(_.write(key, bytes))
  }

  private def withWritableStore[A](location: WriteResultLocation, size: Long)(
      use: WritableObjectStore => A
  ): A = {
    val store = NativeWritableObjectStore.open(location.recordProperties(size))
    WriterResources.closing(store.close())(use(store))
  }

  private def withStore[A](location: WriteResultLocation)(
      use: NativeObjectStore => A
  ): A = {
    val store = NativeObjectStore.Factory(location.properties).open()
    WriterResources.closing(store.close())(use(store))
  }
}
