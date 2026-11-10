package com.zilliz.spark.connector.write

import java.{util => ju}
import java.time.Instant
import java.util.concurrent.{Callable, Executors, ThreadFactory}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.UUID
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.catalog.{
  SupportsWrite,
  Table,
  TableCapability
}
import org.apache.spark.sql.connector.distributions.{
  Distribution,
  Distributions
}
import org.apache.spark.sql.connector.expressions.{
  Expression,
  Expressions,
  SortDirection,
  SortOrder
}
import org.apache.spark.sql.connector.write.{
  BatchWrite,
  DataWriterFactory,
  LogicalWriteInfo,
  PhysicalWriteInfo,
  RequiresDistributionAndOrdering,
  Write,
  WriteBuilder,
  WriterCommitMessage
}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.SparkSession

import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.milvus.storage.io.NativeWritableObjectStore
import com.zilliz.milvus.storage.schema.{FieldMetadata, SchemaMapper}
import com.zilliz.milvus.storage.snapshot.{
  CollectionIndex,
  SegmentIndexes,
  SegmentLayout,
  Snapshot
}
import com.zilliz.milvus.storage.write.{
  ManifestTransaction,
  WriterResources,
  WrittenColumnGroups
}
import com.zilliz.spark.connector.options.{MilvusOption, StorageOptions}
import com.zilliz.spark.connector.table.SnapshotSources
import com.zilliz.spark.connector.types.RowIdentity

/** One data segment a column replacement covers: where its manifest is, the
  * revision the snapshot pinned, and how many rows it stores, rows marked
  * deleted included.
  */
final case class PlannedSegment(
    id: Long,
    partitionId: Long,
    basePath: String,
    sourceVersion: Long,
    rows: Long
)

/** A column replacement resolved on the driver from its options, the
  * DataFrame's schema and the fixed source snapshot, before any task starts.
  *
  * @param targetSchema
  *   the replaced columns in DataFrame order, each carrying its collection
  *   field's id, type, dimension and nullability
  * @param segments
  *   every data segment that stores rows, by id
  * @param storage
  *   the `fs.*` properties of the snapshot's bucket, which hold the segments
  */
final case class ColumnReplacePlan(
    snapshot: SourceSnapshot,
    targets: Seq[TargetColumn],
    targetSchema: StructType,
    segments: Seq[PlannedSegment],
    storage: Map[String, String],
    bucket: String,
    result: WriteResultLocation,
    params: WriteOptions
)

/** Builds the write table for `milvus.write.operation=replace_columns`.
  *
  * The DataFrame carries `_milvus_row_id` and one column per replaced field,
  * named after it. Every stored row of the snapshot must arrive exactly once,
  * rows marked deleted included, which is what a physical read with row
  * identities yields; the write replaces the columns of all of them.
  */
private[connector] object ColumnReplaceTables {

  /** Options that select or filter what a read returns. A column replacement
    * covers the whole snapshot, so none of them applies.
    */
  private val ReadOnlyOptions = Seq(
    MilvusOption.MilvusPartitions,
    MilvusOption.MilvusSegments,
    MilvusOption.MilvusFilter,
    MilvusOption.MilvusExtraColumns,
    MilvusOption.ReadMode,
    MilvusOption.ReadIncludeRowId,
    MilvusOption.ReaderFieldIDs,
    MilvusOption.SnapshotManifests,
    MilvusOption.SnapshotSchemaBytes,
    MilvusOption.SnapshotSchemaJson
  )

  def load(
      options: CaseInsensitiveStringMap,
      inputSchema: Option[StructType]
  ): ColumnReplaceTable = {
    val schema = inputSchema
      .filter(_.nonEmpty)
      .getOrElse(
        throw new IllegalArgumentException(
          s"'${MilvusOption.WriteOperation}' writes a DataFrame; it has no table of its own to read"
        )
      )
    new ColumnReplaceTable(plan(options, schema), schema)
  }

  private[write] def plan(
      options: CaseInsensitiveStringMap,
      inputSchema: StructType
  ): ColumnReplacePlan = {
    val operation = MilvusOption.writeOperation(options)
    require(
      operation.contains(MilvusOption.WriteOperationReplaceColumns),
      s"a column replacement needs '${MilvusOption.WriteOperation}=${MilvusOption.WriteOperationReplaceColumns}'"
    )
    ReadOnlyOptions.foreach { key =>
      if (Option(options.get(key)).exists(_.trim.nonEmpty)) {
        throw new IllegalArgumentException(
          s"Option '$key' does not apply to a column replacement, which covers every stored row of the snapshot"
        )
      }
    }
    val snapshotPath = Option(options.get(MilvusOption.SnapshotPath))
      .map(_.trim)
      .filter(_.nonEmpty)
      .getOrElse(
        throw new IllegalArgumentException(
          s"a column replacement needs '${MilvusOption.SnapshotPath}', the snapshot its rows were read from"
        )
      )
    val resultPath = Option(options.get("path"))
      .map(_.trim)
      .filter(_.nonEmpty)
      .getOrElse(
        throw new IllegalArgumentException(
          "a column replacement needs its write result address: .save(path)"
        )
      )

    val milvusOption = MilvusOption(options)
    val snapshot = SnapshotSources
      .forRead(milvusOption, withSegments = true)
      .snapshot()
      .fold(
        {
          case e: IllegalArgumentException => throw e
          case e =>
            throw new IllegalArgumentException(
              s"Cannot resolve the snapshot to replace columns of: ${e.getMessage}",
              e
            )
        },
        identity
      )
    val source = sourceOf(snapshot, snapshotPath)
    val targetSchema = resolveTargets(inputSchema, snapshot)
    val segments = plannedSegments(snapshot)

    val bucket = Option(snapshot.bucket).map(_.trim).getOrElse("")
    val canonical = milvusOption.options ++
      (if (bucket.nonEmpty) Map(StorageProperties.BucketName -> bucket)
       else Map.empty[String, String])
    val storage = StorageOptions.storagePropertiesFor(
      StorageOptions.buildHadoopConfForOptions(
        milvusOption.options,
        snapshotPath
      ),
      bucket,
      canonical
    )
    ColumnReplacePlan(
      snapshot = source,
      targets = targetSchema.fields.toSeq.map(field =>
        TargetColumn(
          field.name,
          field.metadata.getLong(FieldMetadata.MilvusFieldIdMetadataKey)
        )
      ),
      targetSchema = targetSchema,
      segments = segments,
      storage = storage,
      bucket = bucket,
      result = WriteResultLocation.resolve(resultPath, storage),
      params = WriteOptions(replaceColumns = true)
    )
  }

  private def sourceOf(snapshot: Snapshot, path: String): SourceSnapshot = {
    val id = snapshot.id.getOrElse(
      throw new IllegalArgumentException(
        s"Snapshot '${snapshot.name}' has no snapshot_info.id, so row identities cannot be bound to it; " +
          "replace columns of a snapshot file that Milvus created"
      )
    )
    require(
      snapshot.collectionId > 0L,
      s"Snapshot '${snapshot.name}' has no collection id"
    )
    SourceSnapshot(
      path = path,
      id = id,
      name = snapshot.name,
      collectionId = snapshot.collectionId,
      schemaVersion = snapshot.schema.version
    )
  }

  /** The replaced columns: every DataFrame column except the row identity,
    * resolved against the collection. Fields whose values Milvus owns or
    * derives are refused, and so is a field with an index: the new manifest
    * would leave the index describing the old values.
    */
  private[write] def resolveTargets(
      inputSchema: StructType,
      snapshot: Snapshot
  ): StructType = {
    val rowIds = inputSchema.fields.filter(
      _.name.equalsIgnoreCase(RowIdentity.ColumnName)
    )
    if (rowIds.isEmpty) {
      throw new IllegalArgumentException(
        s"The DataFrame has no '${RowIdentity.ColumnName}' column. Read the snapshot with " +
          s"'${MilvusOption.ReadMode}=${MilvusOption.ReadModePhysical}' and " +
          s"'${MilvusOption.ReadIncludeRowId}=true', and keep that column unchanged"
      )
    }
    if (rowIds.length > 1 || rowIds.head.name != RowIdentity.ColumnName) {
      throw new IllegalArgumentException(
        s"The DataFrame must have exactly one column named '${RowIdentity.ColumnName}', got ${rowIds.map(_.name).mkString(", ")}"
      )
    }
    if (
      !DataType.equalsIgnoreNullability(
        rowIds.head.dataType,
        RowIdentity.dataType
      )
    ) {
      throw new IllegalArgumentException(
        s"Column '${RowIdentity.ColumnName}' is ${rowIds.head.dataType.simpleString}, but a row identity is " +
          s"${RowIdentity.dataType.simpleString}; carry the column from the physical read unchanged"
      )
    }
    val columns = inputSchema.fields.filterNot(_.name == RowIdentity.ColumnName)
    if (columns.isEmpty) {
      throw new IllegalArgumentException(
        s"The DataFrame names no column to replace besides '${RowIdentity.ColumnName}'"
      )
    }
    val resolved = WriteSchema.resolve(
      StructType(columns),
      snapshot.schema,
      WriteSchema.Mode.Columns
    )
    val fieldsByName = snapshot.schema.fields.map(f => f.name -> f).toMap
    val systemIds =
      Set(SchemaMapper.RowIdFieldId, SchemaMapper.TimestampFieldId)
    val refused = resolved.fields.toSeq.flatMap { column =>
      val field = fieldsByName(column.name)
      Seq(
        if (field.isPrimaryKey) Some("the primary key") else None,
        if (field.isClusteringKey) Some("the clustering key") else None,
        if (field.isDynamic) Some("the dynamic field") else None,
        if (systemIds(field.fieldID)) Some("a system field") else None
      ).flatten.map(role => s"'${column.name}' ($role)")
    }
    if (refused.nonEmpty) {
      throw new IllegalArgumentException(
        s"These columns cannot be replaced: ${refused.mkString(", ")}"
      )
    }
    val indexed = resolved.fields.toSeq.flatMap { column =>
      val id = column.metadata.getLong(FieldMetadata.MilvusFieldIdMetadataKey)
      val defined = indexes(snapshot)
        .filter(_.fieldId == id)
        .map(index => s"'${column.name}' (index '${index.name}')")
      val built = snapshot.dataSegments.flatMap(_.indexes match {
        case SegmentIndexes.Available(present) =>
          present.filter(_.fieldId == id)
        case _ => Vector.empty
      })
      if (defined.nonEmpty) defined
      else
        built
          .map(_.name)
          .distinct
          .map(name =>
            s"'${column.name}' (index '$name' built on its segments)"
          )
    }
    if (indexed.nonEmpty) {
      throw new IllegalArgumentException(
        s"These columns have indexes: ${indexed.mkString(", ")}. A replaced column would leave its index " +
          "describing the old values, so drop the index before the replacement and build it again after " +
          "the new manifests are registered"
      )
    }
    resolved
  }

  /** The snapshot's index definitions. Milvus writes the snapshot without an
    * empty list, so absent definitions mean none only where every data segment
    * also says which index files it has: a segment whose record cannot say
    * leaves an index on a replaced field possible.
    */
  private def indexes(snapshot: Snapshot): Vector[CollectionIndex] =
    snapshot.indexes.getOrElse {
      val unknown = snapshot.dataSegments.collect {
        case segment if segment.indexes == SegmentIndexes.Unknown => segment.id
      }
      if (unknown.nonEmpty) {
        throw new IllegalArgumentException(
          s"Snapshot '${snapshot.name}' describes neither its index definitions nor the index files of " +
            s"segment(s) ${unknown.sorted.take(10).mkString(", ")}, so a column replacement cannot confirm " +
            "that the replaced fields have none"
        )
      }
      Vector.empty
    }

  private def plannedSegments(snapshot: Snapshot): Seq[PlannedSegment] = {
    val planned = snapshot.dataSegments.map { segment =>
      if (segment.storageVersion != 3) {
        throw new IllegalArgumentException(
          s"Segment ${segment.id} is Storage V${segment.storageVersion}; column replacement writes Storage V3 only"
        )
      }
      val (basePath, version) = segment.layout match {
        case SegmentLayout.Manifest(base, readVersion) if readVersion > 0L =>
          (base, readVersion)
        case SegmentLayout.Manifest(_, readVersion) =>
          throw new IllegalArgumentException(
            s"Segment ${segment.id} pins no committed manifest revision ($readVersion)"
          )
        case _ =>
          throw new IllegalArgumentException(
            s"Segment ${segment.id} has no V3 manifest to replace columns in"
          )
      }
      val rows = segment.rows.getOrElse(
        throw new IllegalArgumentException(
          s"Segment ${segment.id} has no stored row count in the snapshot"
        )
      )
      require(rows >= 0L, s"Segment ${segment.id} has $rows stored rows")
      PlannedSegment(segment.id, segment.partitionId, basePath, version, rows)
    }
    val stored = planned.filter(_.rows > 0L).sortBy(_.id)
    if (stored.isEmpty) {
      throw new IllegalArgumentException(
        s"Snapshot '${snapshot.name}' stores no rows, so there are no columns to replace"
      )
    }
    stored
  }
}

/** The write side of `format("milvus")` for one column replacement. Its schema
  * is the DataFrame's own: Spark resolves the append against it by name, and
  * the plan already checked every column against the collection.
  */
final class ColumnReplaceTable(
    val plan: ColumnReplacePlan,
    inputSchema: StructType
) extends Table
    with SupportsWrite {

  override def name(): String =
    s"column replacement of snapshot ${plan.snapshot.name}"

  override def schema(): StructType = inputSchema

  override def capabilities(): ju.Set[TableCapability] =
    Set(TableCapability.BATCH_WRITE).asJava

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    new WriteBuilder {
      override def build(): Write =
        new ColumnReplaceWrite(plan, ColumnReplaceTask.of(plan, info.schema()))
    }
}

/** Asks Spark to deliver each segment's rows to one writer, in stored order.
  *
  * The distribution clusters by `_milvus_row_id.segment_id` alone: clustering
  * by the whole identity would scatter one segment's offsets over many writers.
  * One writer can still receive several segments, one after another.
  */
final class ColumnReplaceWrite(
    plan: ColumnReplacePlan,
    task: ColumnReplaceTask
) extends Write
    with RequiresDistributionAndOrdering {

  private val segment = Expressions.column(
    s"${RowIdentity.ColumnName}.${RowIdentity.SegmentIdField}"
  )
  private val offset = Expressions.column(
    s"${RowIdentity.ColumnName}.${RowIdentity.RowOffsetField}"
  )

  override def requiredDistribution(): Distribution =
    Distributions.clustered(Array[Expression](segment))

  override def requiredOrdering(): Array[SortOrder] = Array(
    Expressions.sort(segment, SortDirection.ASCENDING),
    Expressions.sort(offset, SortDirection.ASCENDING)
  )

  override def distributionStrictlyRequired(): Boolean = true

  override def requiredNumPartitions(): Int = 0

  override def description(): String =
    s"MilvusColumnReplace(snapshot=${plan.snapshot.name}, " +
      s"fields=${plan.targets.map(_.name).mkString(",")}, segments=${plan.segments.size})"

  override def toBatch(): BatchWrite = new ColumnReplaceBatchWrite(plan, task)
}

/** The driver side of one column replacement.
  *
  * Tasks only write files. The driver claims the result address before any task
  * starts, and once Spark adopted one attempt per partition it checks that
  * every segment arrived complete, records the commit plan, commits each
  * segment's manifest from its pinned revision and publishes the receipt last.
  * Segments commit separately, so a failure can leave some committed; the
  * failure record says which, and files a manifest may reference are kept.
  */
final class ColumnReplaceBatchWrite(
    plan: ColumnReplacePlan,
    task: ColumnReplaceTask
) extends BatchWrite
    with Logging {

  private val operationId = UUID.randomUUID().toString
  @volatile private var claimed = false
  @volatile private var createdAt = ""
  @volatile private var outcomes = Map.empty[Long, SegmentOutcome]
  @volatile private var resultPublished = false
  @volatile private var segmentsBroadcast
      : Option[Broadcast[Map[Long, PlannedSegment]]] = None

  override def createBatchWriterFactory(
      info: PhysicalWriteInfo
  ): DataWriterFactory = {
    WriteResultRecords.claim(plan.result, operationId)
    claimed = true
    createdAt = Instant.now().toString
    // Spark aborts a write only after the factory exists, so a failure from
    // here on records itself: the address is claimed and must say why it has
    // no result.
    try {
      val context = SparkSession.active.sparkContext
      WriteResultRecords.writeOperation(
        plan.result,
        operationId,
        MilvusOption.WriteOperationReplaceColumns,
        createdAt,
        context.applicationId,
        plan.snapshot,
        plan.targets,
        plan.segments.size
      )
      val broadcast =
        context.broadcast(
          plan.segments.map(segment => segment.id -> segment).toMap
        )
      segmentsBroadcast = Some(broadcast)
      new ColumnReplaceWriterFactory(task, broadcast)
    } catch {
      case NonFatal(failure) =>
        try abort(Array.empty)
        catch { case NonFatal(record) => failure.addSuppressed(record) }
        throw failure
    }
  }

  override def useCommitCoordinator(): Boolean = true

  override def commit(messages: Array[WriterCommitMessage]): Unit =
    WriterResources.closing(releaseBroadcast()) {
      val written = writtenSegments(messages)
      val commits = plan.segments.map { segment =>
        PlannedCommit(
          segmentId = segment.id,
          partitionId = segment.partitionId,
          manifestBasePath = segment.basePath,
          sourceVersion = segment.sourceVersion,
          rowCount = segment.rows,
          columnGroups = written(segment.id).columnGroups
        )
      }
      WriteResultRecords.writeCommitPlan(
        plan.result,
        operationId,
        plan.snapshot,
        plan.targets,
        commits
      )
      val committed = SegmentCommits.commitAll(
        commits,
        plan.storage,
        plan.targets.map(_.fieldId.toString)
      )
      outcomes = committed.map(outcome => outcome.segmentId -> outcome).toMap
      val failed = committed.filter(_.state != SegmentState.Committed)
      if (failed.nonEmpty) {
        throw new IllegalStateException(
          s"${failed.size} of ${commits.size} segment manifest(s) did not commit; " +
            s"first: segment ${failed.head.segmentId} ${failed.head.state.name}: ${failed.head.error
                .getOrElse("")}. " +
            s"Committed segments keep their new revisions; see ${plan.result.failureKey}"
        )
      }
      val versions = committed.map(o => o.segmentId -> o.version.get).toMap
      WriteResultRecords.writeResult(
        plan.result,
        WriteResult(
          operationId = operationId,
          operation = MilvusOption.WriteOperationReplaceColumns,
          createdAt = createdAt,
          committedAt = Instant.now().toString,
          applicationId = SparkSession.active.sparkContext.applicationId,
          snapshot = plan.snapshot,
          changedFields = plan.targets,
          segments = commits.map(commit =>
            CommittedSegment(commit, versions(commit.segmentId))
          )
        )
      )
      resultPublished = true
      logInfo(
        s"Column replacement $operationId committed ${commits.size} segment(s) of snapshot " +
          s"${plan.snapshot.name}; receipt ${plan.result.resultKey}"
      )
    }

  override def abort(messages: Array[WriterCommitMessage]): Unit =
    WriterResources.closing(releaseBroadcast()) {
      // Without the claim the address belongs to another operation: nothing
      // here may be recorded there, and this job wrote no files.
      if (claimed && !resultPublished) {
        val written = Option(messages)
          .getOrElse(Array.empty[WriterCommitMessage])
          .toSeq
          .collect { case message: ColumnReplaceCommitMessage =>
            message.segments
          }
          .flatten
        // A file stays when a manifest references it or may reference it.
        val removable = written.filter(segment =>
          outcomes
            .get(segment.segmentId)
            .forall(_.state == SegmentState.NotCommitted)
        )
        deleteQuietly(removable.flatMap(_.columnGroups.flatMap(_.files)))
        val recorded = plan.segments.map { segment =>
          outcomes.getOrElse(
            segment.id,
            SegmentOutcome(
              segment.id,
              segment.sourceVersion,
              SegmentState.NotCommitted,
              None,
              None
            )
          )
        }
        val (stage, message) =
          if (outcomes.isEmpty)
            "write" -> "the job failed before any segment manifest was committed"
          else if (recorded.forall(_.state == SegmentState.Committed))
            "publish" -> "every segment manifest committed, but the receipt was not published"
          else "commit" -> "some segment manifests did not commit"
        WriteResultRecords.writeFailure(
          plan.result,
          operationId,
          Instant.now().toString,
          stage,
          message,
          recorded
        )
      }
    }

  /** The adopted attempt of every partition, as written segments by id. Every
    * planned segment must be there once with all of its rows.
    */
  private def writtenSegments(
      messages: Array[WriterCommitMessage]
  ): Map[Long, WrittenSegment] = {
    val segments = messages.toSeq.flatMap {
      case message: ColumnReplaceCommitMessage => message.segments
      case other =>
        throw new IllegalStateException(
          s"unexpected commit message ${Option(other).map(_.getClass.getName).orNull}"
        )
    }
    val repeated = segments.groupBy(_.segmentId).collect {
      case (id, copies) if copies.size > 1 => id
    }
    if (repeated.nonEmpty) {
      throw new IllegalStateException(
        s"the rows of segment(s) ${repeated.toSeq.sorted.mkString(", ")} reached more than one writer"
      )
    }
    val byId = segments.map(segment => segment.segmentId -> segment).toMap
    val missing = plan.segments.map(_.id).filterNot(byId.contains)
    if (missing.nonEmpty) {
      throw new IllegalArgumentException(
        s"The DataFrame has no rows of segment(s) ${missing.take(10).mkString(", ")}" +
          (if (missing.size > 10) s" and ${missing.size - 10} more" else "") +
          ". A column replacement writes every stored row of the snapshot exactly once, " +
          "rows marked deleted included; read it with " +
          s"'${MilvusOption.ReadMode}=${MilvusOption.ReadModePhysical}' and keep every row"
      )
    }
    byId
  }

  private def deleteQuietly(files: Seq[String]): Unit =
    if (files.nonEmpty) {
      try {
        val store = NativeWritableObjectStore.open(plan.storage)
        WriterResources.closing(store.close()) {
          files.foreach { file =>
            try store.delete(file)
            catch {
              case NonFatal(e) =>
                logWarning(
                  s"Could not delete unreferenced file $file: ${e.getMessage}"
                )
            }
          }
        }
      } catch {
        case NonFatal(e) =>
          logWarning(
            s"Could not delete ${files.size} unreferenced file(s): ${e.getMessage}"
          )
      }
    }

  private def releaseBroadcast(): Unit = {
    segmentsBroadcast.foreach(broadcast =>
      try broadcast.destroy()
      catch {
        case NonFatal(e) =>
          logWarning(
            s"Could not release the segment broadcast: ${e.getMessage}"
          )
      }
    )
    segmentsBroadcast = None
  }
}

/** The driver's commit of every segment's new files. */
private[write] object SegmentCommits {
  private val Parallelism = 8

  /** Commits each segment from its pinned revision, a few at a time, and stops
    * starting new commits after the first failure. A segment whose commit was
    * never started is not committed.
    */
  def commitAll(
      commits: Seq[PlannedCommit],
      storage: Map[String, String],
      columns: Seq[String]
  ): Seq[SegmentOutcome] = {
    val stop = new AtomicBoolean(false)
    val pool = Executors.newFixedThreadPool(
      math.max(1, math.min(Parallelism, commits.size)),
      daemonThreads
    )
    try {
      commits
        .map { commit =>
          pool.submit(new Callable[SegmentOutcome] {
            override def call(): SegmentOutcome =
              if (stop.get())
                SegmentOutcome(
                  commit.segmentId,
                  commit.sourceVersion,
                  SegmentState.NotCommitted,
                  None,
                  Some("not attempted after an earlier segment failed")
                )
              else {
                val outcome = commitOne(commit, storage, columns)
                if (outcome.state != SegmentState.Committed) stop.set(true)
                outcome
              }
          })
        }
        .map(_.get())
    } finally pool.shutdownNow()
  }

  private[write] def commitOne(
      commit: PlannedCommit,
      storage: Map[String, String],
      columns: Seq[String]
  ): SegmentOutcome =
    try {
      val groups = WrittenColumnGroups.of(commit.columnGroups)
      val version = WriterResources.closing(groups.close()) {
        ManifestTransaction.commit(
          commit.manifestBasePath,
          storage,
          groups,
          ManifestTransaction.ReplaceColumns(columns),
          readVersion = commit.sourceVersion
        )
      }
      SegmentOutcome(
        commit.segmentId,
        commit.sourceVersion,
        SegmentState.Committed,
        Some(version),
        None
      )
    } catch {
      case NonFatal(failure) => afterFailure(commit, storage, failure)
    }

  /** What a failed commit left. A commit pinned to revision `v` can only have
    * produced revision `v + 1`, so that revision, if it exists, either
    * references this write's files (the commit landed and its response was
    * lost) or another writer's (the commit lost the race).
    */
  private def afterFailure(
      commit: PlannedCommit,
      storage: Map[String, String],
      failure: Throwable
  ): SegmentOutcome = {
    val error = Option(failure.getMessage).getOrElse(failure.toString)
    try {
      val latest =
        ManifestTransaction.revision(commit.manifestBasePath, storage)
      val produced = commit.sourceVersion + 1L
      if (latest.version < produced)
        SegmentOutcome(
          commit.segmentId,
          commit.sourceVersion,
          SegmentState.NotCommitted,
          None,
          Some(error)
        )
      else {
        val candidate =
          if (latest.version == produced) latest
          else
            ManifestTransaction.revision(
              commit.manifestBasePath,
              storage,
              produced
            )
        val ours = commit.columnGroups.flatMap(_.files).map(fileName).toSet
        if (candidate.files.map(fileName).exists(ours))
          SegmentOutcome(
            commit.segmentId,
            commit.sourceVersion,
            SegmentState.Committed,
            Some(produced),
            Some(s"committed although the commit reported: $error")
          )
        else
          SegmentOutcome(
            commit.segmentId,
            commit.sourceVersion,
            SegmentState.NotCommitted,
            None,
            Some(s"revision $produced was committed by another writer: $error")
          )
      }
    } catch {
      case NonFatal(inspection) =>
        SegmentOutcome(
          commit.segmentId,
          commit.sourceVersion,
          SegmentState.Unknown,
          None,
          Some(
            s"$error; the manifest could not be inspected afterwards: ${inspection.getMessage}"
          )
        )
    }
  }

  private def fileName(path: String): String =
    path.substring(path.lastIndexOf('/') + 1)

  private val daemonThreads: ThreadFactory = (runnable: Runnable) => {
    val thread = new Thread(runnable, "milvus-column-commit")
    thread.setDaemon(true)
    thread
  }
}
