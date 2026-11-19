package com.zilliz.spark.connector.write

import java.{util => ju}
import java.time.Instant
import java.util.concurrent.{Executors, ThreadFactory}
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
import org.apache.spark.sql.connector.metric.{CustomMetric, CustomTaskMetric}
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
import com.zilliz.spark.connector.metrics.{
  OperationToken,
  OperationalMetrics,
  WriteFailureCategory,
  WriteOperationalMetrics,
  WritePhase
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
    MilvusOption.ReadMode,
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
      .forRead(milvusOption)
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
          s"'${MilvusOption.ReadMode}=${MilvusOption.ReadModePhysical}', " +
          s"SELECT ${RowIdentity.ColumnName}, and carry it unchanged to the write"
      )
    }
    if (rowIds.length > 1 || rowIds.head.name != RowIdentity.ColumnName) {
      throw new IllegalArgumentException(
        s"The DataFrame must have exactly one column named '${RowIdentity.ColumnName}', got ${rowIds.map(_.name).mkString(", ")}"
      )
    }
    if (rowIds.head.metadata.contains(FieldMetadata.MilvusFieldIdMetadataKey)) {
      throw new IllegalArgumentException(
        s"Column '${RowIdentity.ColumnName}' is a stored collection field. " +
          s"Omit it from the physical read's external schema, then SELECT ${RowIdentity.ColumnName} to carry the hidden row identity"
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
) extends DriverMeasuredWrite
    with RequiresDistributionAndOrdering {

  private val operationId = UUID.randomUUID().toString
  private val metrics = new WriteOperationalMetrics(
    plan.segments.map(segment => segment.id -> segment.rows).toMap
  )

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

  override def supportedCustomMetrics(): Array[CustomMetric] =
    WriteOperationalMetrics.supported

  override protected def driverMetricsValues: Array[CustomTaskMetric] =
    metrics.driverValues

  override def toBatch(): BatchWrite = {
    val token =
      OperationalMetrics.begin("write", metrics.operationalValues, operationId)
    new ColumnReplaceBatchWrite(
      plan,
      task.copy(operationId = operationId, operationToken = Some(token)),
      metrics,
      operationId,
      token
    )
  }
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
    task: ColumnReplaceTask,
    metrics: WriteOperationalMetrics,
    operationId: String,
    token: OperationToken
) extends BatchWrite
    with Logging {

  @volatile private var claimed = false
  @volatile private var createdAt = ""
  @volatile private var outcomes = Map.empty[Long, SegmentOutcome]
  @volatile private var resultPublished = false
  private var failureReported = false
  @volatile private var segmentsBroadcast
      : Option[Broadcast[Map[Long, PlannedSegment]]] = None

  private def report(): Unit = synchronized {
    metrics.operationalValues.foreach { case (name, value) =>
      OperationalMetrics.set(token, name, value)
    }
  }

  private def enter(phase: Long): Unit = {
    metrics.enter(phase)
    OperationalMetrics.phase(token, phaseName(phase))
    report()
  }

  private def phaseName(phase: Long): String = phase match {
    case WritePhase.Prepared   => "planning"
    case WritePhase.CommitPlan => "commit"
    case WritePhase.Validating => "validation"
    case WritePhase.Committing => "commit"
    case WritePhase.Publishing => "publish"
    case _                     => WritePhase.name(phase)
  }

  private def failed(failure: Throwable): Unit = {
    metrics.fail(failure)
    report()
    reportFailure()
  }

  private def reportFailure(): Unit = synchronized {
    val state = metrics.values
    if (
      !failureReported && state(
        WriteOperationalMetrics.FailureCategory
      ) != WriteFailureCategory.None
    ) {
      OperationalMetrics.failure(
        token,
        phaseName(state(WriteOperationalMetrics.FailurePhase)),
        WriteFailureCategory.name(
          state(WriteOperationalMetrics.FailureCategory)
        )
      )
      failureReported = true
    }
  }

  private def finishFailure(): Unit = {
    report()
    val state = metrics.values
    val outcome =
      if (
        state(WriteOperationalMetrics.UnknownSegments) > 0L || state(
          WriteOperationalMetrics.FailureCategory
        ) == WriteFailureCategory.PublicationUnconfirmed
      ) "unknown"
      else "failed"
    OperationalMetrics.finish(token, outcome)
  }

  override def createBatchWriterFactory(
      info: PhysicalWriteInfo
  ): DataWriterFactory = {
    enter(WritePhase.Claiming)
    createdAt = Instant.now().toString
    val context = SparkSession.active.sparkContext
    try
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
    catch {
      case NonFatal(failure) =>
        failed(failure)
        finishFailure()
        throw failure
    }
    claimed = true
    // Spark aborts a write only after the factory exists, so a failure from
    // here on records itself: the address is claimed and must say why it has
    // no result.
    try {
      val broadcast =
        context.broadcast(
          plan.segments.map(segment => segment.id -> segment).toMap
        )
      segmentsBroadcast = Some(broadcast)
      enter(WritePhase.Writing)
      new ColumnReplaceWriterFactory(
        task.copy(operationId = operationId),
        broadcast
      )
    } catch {
      case NonFatal(failure) =>
        failed(failure)
        try abort(Array.empty)
        catch { case NonFatal(record) => failure.addSuppressed(record) }
        throw failure
    }
  }

  override def useCommitCoordinator(): Boolean = true

  override def commit(messages: Array[WriterCommitMessage]): Unit = try {
    WriterResources.closing(releaseBroadcast()) {
      enter(WritePhase.Validating)
      val written = writtenSegments(messages)
      metrics.filesReady(
        written.values.map(_.rows).sum,
        written.values.map(_.columnGroups.map(_.files.size.toLong).sum).sum
      )
      report()
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
      enter(WritePhase.CommitPlan)
      WriteResultRecords.writeCommitPlan(
        plan.result,
        operationId,
        plan.snapshot,
        plan.targets,
        commits
      )
      enter(WritePhase.Committing)
      // A worker can enter storage as soon as the pool starts. If the driver is
      // interrupted, commitAll identifies the segments still waiting for a worker.
      outcomes = commits.map { commit =>
        commit.segmentId -> SegmentOutcome(
          commit.segmentId,
          commit.sourceVersion,
          SegmentState.Unknown,
          None,
          Some("the manifest commit did not report its outcome")
        )
      }.toMap
      val columns = plan.targets.map(_.fieldId.toString)
      SegmentCommits.commitAll(
        commits,
        outcome => {
          synchronized { outcomes += outcome.segmentId -> outcome }
          if (
            outcome.state == SegmentState.Committed && outcome.error.nonEmpty
          ) {
            metrics.cleanupFailed()
          }
          metrics.segmentOutcome(outcome.segmentId, outcome.state.name)
          report()
        }
      )(commit => SegmentCommits.commitOne(commit, plan.storage, columns))
      val committed = commits.map(commit => outcomes(commit.segmentId))
      committed.find(outcome => !outcome.successful).foreach { first =>
        throw new IllegalStateException(
          s"Manifest commit phase failed for segment ${first.segmentId} " +
            s"(${first.state.name}): ${first.error.getOrElse("no outcome reported")}. " +
            s"Committed segments keep their new revisions; see ${plan.result.failureKey}"
        )
      }
      val versions = committed.map(o => o.segmentId -> o.version.get).toMap
      enter(WritePhase.Publishing)
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
      metrics.published()
      report()
      logInfo(
        s"Column replacement $operationId committed ${commits.size} segment(s) of snapshot " +
          s"${plan.snapshot.name}; receipt ${plan.result.resultKey}"
      )
    }
    report()
    OperationalMetrics.finish(token, "success")
  } catch {
    case interrupted: InterruptedException =>
      failed(interrupted)
      throw interrupted
    case NonFatal(failure) =>
      failed(failure)
      throw failure
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = try {
    WriterResources.closing(releaseBroadcast()) {
      // Without the claim the address belongs to another operation: nothing
      // here may be recorded there, and this job wrote no files.
      if (claimed && !resultPublished) {
        metrics.taskFailed()
        report()
        val written = Option(messages)
          .getOrElse(Array.empty[WriterCommitMessage])
          .toSeq
          .collect { case message: ColumnReplaceCommitMessage =>
            message.segments
          }
          .flatten
        // Even a rejected commit's files may be referenced by another writer.
        val removable = written.filter(segment =>
          outcomes
            .get(segment.segmentId)
            .forall(_.state == SegmentState.NotAttempted)
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
        recorded.foreach(outcome =>
          metrics.segmentOutcome(outcome.segmentId, outcome.state.name)
        )
        report()
        val (stage, message) =
          if (outcomes.isEmpty)
            "write" -> "the job failed before any segment manifest was committed"
          else if (recorded.forall(_.successful))
            "publish" -> "every segment manifest committed, but receipt publication could not be confirmed"
          else
            "commit" -> "the segment manifest commit phase failed"
        try
          WriteResultRecords.writeFailure(
            plan.result,
            operationId,
            Instant.now().toString,
            stage,
            message,
            recorded
          )
        finally {
          reportFailure()
        }
      }
    }
  } finally {
    if (claimed && !resultPublished) finishFailure()
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
    val expected =
      plan.segments.map(segment => segment.id -> segment.rows).toMap
    require(
      byId.keySet == expected.keySet,
      "the adopted write messages contain unplanned segments"
    )
    byId.values.foreach(segment =>
      require(
        segment.rows == expected(segment.segmentId),
        s"segment ${segment.segmentId} reported ${segment.rows} rows, expected ${expected(segment.segmentId)}"
      )
    )
    byId
  }

  private def cleanupFailed(): Unit = {
    metrics.cleanupFailed()
    report()
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
                cleanupFailed()
                logWarning(
                  s"Could not delete unreferenced file $file: ${e.getMessage}"
                )
            }
          }
        }
      } catch {
        case NonFatal(e) =>
          cleanupFailed()
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
          cleanupFailed()
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

  /** Workers claim one segment at a time. Closing the queue prevents further
    * claims and reports exactly which segments never reached a worker. A caller
    * interrupted while a worker is in storage keeps its unknown outcome.
    */
  def commitAll(
      commits: Seq[PlannedCommit],
      reportOutcome: SegmentOutcome => Unit,
      parallelism: Int = Parallelism
  )(commitOne: PlannedCommit => SegmentOutcome): Unit = {
    if (commits.isEmpty) return
    val pending = commits.iterator
    val gate = new Object
    var stopped = false
    var closed = false

    def claim(): Option[PlannedCommit] = gate.synchronized {
      if (stopped || !pending.hasNext) None else Some(pending.next())
    }

    val worker = new Runnable {
      override def run(): Unit = try {
        var next = claim()
        while (next.nonEmpty) {
          val outcome = commitOne(next.get)
          gate.synchronized {
            if (!outcome.successful) stopped = true
            if (!closed) reportOutcome(outcome)
          }
          next = claim()
        }
      } finally {
        // A failed worker can leave its last commit unconfirmed.
        gate.synchronized { stopped = true }
      }
    }

    val count = math.min(parallelism, commits.size)
    val pool = Executors.newFixedThreadPool(count, daemonThreads)
    WriterResources.closing {
      try
        gate.synchronized {
          stopped = true
          closed = true
          pending.foreach { commit =>
            reportOutcome(
              SegmentOutcome(
                commit.segmentId,
                commit.sourceVersion,
                SegmentState.NotAttempted,
                None,
                Some("not attempted before segment commits stopped")
              )
            )
          }
        }
      finally pool.shutdown()
    } {
      if (Thread.interrupted())
        throw new InterruptedException(
          "segment commits interrupted before dispatch"
        )
      val workers = Vector.fill(count)(pool.submit(worker))
      workers.foreach(_.get())
    }
  }

  private[write] def commitOne(
      commit: PlannedCommit,
      storage: Map[String, String],
      columns: Seq[String]
  ): SegmentOutcome =
    commitOne(
      commit,
      version =>
        ManifestTransaction.revision(commit.manifestBasePath, storage, version)
    ) { submit =>
      val groups = WrittenColumnGroups.of(commit.columnGroups)
      WriterResources.closing(groups.close()) {
        ManifestTransaction.withPrepared(
          commit.manifestBasePath,
          storage,
          groups,
          ManifestTransaction.ReplaceColumns(columns),
          readVersion = commit.sourceVersion
        )(submit)
      }
    }

  private[write] def commitOne(
      commit: PlannedCommit,
      readRevision: Long => ManifestTransaction.Revision
  )(
      write: (ManifestTransaction.PreparedCommit => Long) => Unit
  ): SegmentOutcome = {
    var submitted = false
    var acknowledged = Option.empty[Long]
    try {
      write { prepared =>
        submitted = true
        val version = prepared()
        acknowledged = Some(version)
        version
      }
      require(
        acknowledged.nonEmpty,
        "manifest commit returned without an acknowledgement"
      )
      SegmentOutcome(
        commit.segmentId,
        commit.sourceVersion,
        SegmentState.Committed,
        acknowledged,
        None
      )
    } catch {
      case NonFatal(failure) =>
        acknowledged match {
          case Some(version) =>
            SegmentOutcome(
              commit.segmentId,
              commit.sourceVersion,
              SegmentState.Committed,
              Some(version),
              Some(
                s"revision $version was acknowledged, but cleanup failed: ${failureMessage(failure)}"
              )
            )
          case None if submitted => afterFailure(commit, failure)(readRevision)
          case None =>
            SegmentOutcome(
              commit.segmentId,
              commit.sourceVersion,
              SegmentState.NotAttempted,
              None,
              Some(
                s"manifest commit was not submitted: ${failureMessage(failure)}"
              )
            )
        }
    }
  }

  /** Inspect the one revision a pinned commit could have produced. Data file
    * references can rule out this write. Even a complete file match leaves the
    * transaction identity unknown.
    */
  private[write] def afterFailure(
      commit: PlannedCommit,
      failure: Throwable
  )(readRevision: Long => ManifestTransaction.Revision): SegmentOutcome = {
    val error = failureMessage(failure)
    val (state, detail) =
      try {
        val produced = Math.addExact(commit.sourceVersion, 1L)
        val candidate = readRevision(produced)
        require(
          candidate.version == produced,
          s"expected manifest revision $produced, got ${candidate.version}"
        )
        // Native manifests and task output can spell a path differently. A
        // matching filename conservatively leaves the outcome unresolved.
        val ours = commit.columnGroups.flatMap(_.files).map(fileName).toSet
        if (candidate.files.exists(file => ours.contains(fileName(file))))
          SegmentState.Unknown -> s"revision $produced may reference this write's files"
        else
          SegmentState.NotCommitted -> s"revision $produced was committed by another writer"
      } catch {
        case NonFatal(inspection) =>
          SegmentState.Unknown -> s"the manifest could not be inspected afterwards: ${failureMessage(inspection)}"
      }
    SegmentOutcome(
      commit.segmentId,
      commit.sourceVersion,
      state,
      None,
      Some(s"$error; $detail")
    )
  }

  private def failureMessage(failure: Throwable): String =
    Option(failure.getMessage).getOrElse(failure.toString)

  private def fileName(path: String): String =
    path.substring(path.lastIndexOf('/') + 1)

  private val daemonThreads: ThreadFactory = (runnable: Runnable) => {
    val thread = new Thread(runnable, "milvus-column-commit")
    thread.setDaemon(true)
    thread
  }
}
