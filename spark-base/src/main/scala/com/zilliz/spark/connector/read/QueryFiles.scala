package com.zilliz.spark.connector.read

import java.util.concurrent.{ExecutionException, Executors, ThreadFactory}
import scala.jdk.CollectionConverters._
import scala.util.Try

import org.apache.arrow.memory.BufferAllocator
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  Attribute,
  AttributeReference
}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{
  FileFormat,
  HadoopFsRelation,
  LogicalRelation,
  PartitionedFile
}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

import com.zilliz.milvus.storage.index.{QueryMatrix, SearchPlan}
import com.zilliz.milvus.storage.schema.{VectorElementType, VectorLayout}

/** A query set that is nothing but Parquet files, read by the search tasks
  * themselves (docs/design/architecture/vector-search.html section 2.1).
  *
  * The broadcast path evaluates the query frame on the executors, brings the
  * packed bytes to the driver because `SparkContext.broadcast` takes a driver
  * value, and ships them back; the driver is the one receiver of that collect.
  * When the frame is a plain scan of Parquet files -- a `LogicalRelation` over
  * `HadoopFsRelation` under nothing but column selection -- none of that is
  * needed: the driver reads the footers for the row counts that planning wants,
  * and every first-stage task opens the files with the reader Spark itself
  * would have used. The bytes read are the same, eight tasks reading a file
  * each instead of one driver receiving eight blocks; what goes is the serial
  * hop.
  *
  * `readFile` is the function `FileFormat.buildReaderWithPartitionValues`
  * returns: it captures the Hadoop configuration and the reader settings on the
  * driver and runs on the executors, which is how `FileSourceScanExec` reads a
  * file too. Its output is rows, or columnar batches typed as rows when the
  * vectorized reader applies; both are taken one row at a time and nothing is
  * retained.
  *
  * A task that keeps its queries decodes them by row group, on several threads,
  * straight into each group's matrix ([[decode]]): a row group is independently
  * decodable, and the reader given one row group's byte range reads that row
  * group alone. Single-threaded, the decode of 250,000 queries of 768 floats
  * took 3.5 s at the start of every P3 task while the executor's other cores
  * waited (2026-09-24 decision).
  */
private[read] final case class QueryFiles(
    files: Seq[QueryFiles.File],
    readFile: PartitionedFile => Iterator[InternalRow]
) extends Serializable {

  def queries: Long = files.iterator.map(_.rows).sum

  /** Every row group of every file, in the order of the query sequence. */
  def rowGroups: Seq[QueryFiles.RowGroup] = files.flatMap(_.rowGroups)

  /** The task's groups of the range, packed from the files as they are read.
    *
    * Groups are ranges of the query sequence, and the sequence is the files in
    * order and each file's rows in order, the order the driver counted them in.
    * A range that starts past the first group skips the rows before it; the
    * index mode's one range starts at the first row.
    */
  def groups(
      range: Range,
      planned: Seq[SearchPlan.QueryGroup],
      layout: VectorLayout,
      metric: String
  ): Iterator[SearchQueries.Group] = {
    if (range.isEmpty) return Iterator.empty
    val rows = this.rows(
      files.map(file =>
        QueryFiles.RowGroup(file.path, 0L, file.length, file.rows, 0L)
      )
    )
    var skip = planned(range.head).firstQuery
    while (skip > 0 && rows.hasNext) {
      rows.next()
      skip -= 1
    }
    // Not `rows.take(n)`: a slice of a Scala iterator may be the same object
    // with its bound rewritten, and a second slice of it is empty.
    def bounded(wanted: Int): Iterator[InternalRow] =
      new Iterator[InternalRow] {
        private var left = wanted
        def hasNext: Boolean = left > 0 && rows.hasNext
        def next(): InternalRow = {
          left -= 1
          rows.next()
        }
      }
    range.iterator.map { index =>
      val group = planned(index)
      val (ids, vectors) =
        SearchQueries.packPartition(bounded(group.queries), layout, metric)
      require(
        ids.length == group.queries,
        s"Query group $index was planned as ${group.queries} queries from the files' footers, but the files gave ${ids.length}"
      )
      SearchQueries.Group(ids, vectors, 0)
    }
  }

  /** The task's groups of the range as matrices, decoded row group by row group
    * on `threads` threads at once.
    *
    * Each group's matrix and id array are allocated first; every thread takes a
    * row group, reads it with the same reader as [[groups]], and writes each
    * row into the group and position its global row number puts it at. The
    * planner's groups are contiguous ranges of the sequence, so a row group
    * fills at most a few groups and a thread's cursor over them only moves
    * forward. Rows outside the range are skipped. A row group that fails fails
    * the task; the matrices of a failed decode are released.
    */
  def decode(
      range: Range,
      planned: Seq[SearchPlan.QueryGroup],
      layout: VectorLayout,
      metric: String,
      allocator: BufferAllocator,
      threads: Int
  ): Seq[(Array[Long], QueryMatrix)] = {
    require(threads > 0, s"Decoding takes at least one thread: $threads")
    if (range.isEmpty) return Seq.empty
    val first = planned(range.head).firstQuery.toLong
    val until = planned(range.last).untilQuery.toLong
    val wanted = rowGroups.filter(rowGroup =>
      rowGroup.firstRow < until && rowGroup.firstRow + rowGroup.rows > first
    )
    val ids = range.map(index => new Array[Long](planned(index).queries))
    val builders =
      range.map(index =>
        QueryMatrix.builder(planned(index).queries, layout, allocator)
      )
    val written = range.map(_ => new java.util.concurrent.atomic.AtomicInteger)
    val pool = Executors.newFixedThreadPool(
      math.min(threads, math.max(1, wanted.size)),
      new ThreadFactory {
        def newThread(runnable: Runnable): Thread = {
          val thread = new Thread(runnable, "query-file-decode")
          thread.setDaemon(true)
          thread
        }
      }
    )
    try {
      val futures = wanted.map { rowGroup =>
        pool.submit[Unit] { () =>
          decodeRowGroup(
            rowGroup,
            range,
            planned,
            first,
            until,
            layout,
            metric,
            ids,
            builders,
            written
          )
        }
      }
      futures.foreach { future =>
        try future.get()
        catch {
          case wrapped: ExecutionException =>
            throw Option(wrapped.getCause).getOrElse(wrapped)
        }
      }
      range.zipWithIndex.foreach { case (index, at) =>
        require(
          written(at).get() == planned(index).queries,
          s"Query group $index was planned as ${planned(index).queries} queries from the files' footers, but the files gave ${written(at).get()}"
        )
      }
      ids.zip(builders.map(_.finish()))
    } catch {
      case failure: Throwable =>
        builders.foreach(builder => Try(builder.close()))
        throw failure
    } finally pool.shutdownNow()
  }

  private def decodeRowGroup(
      rowGroup: QueryFiles.RowGroup,
      range: Range,
      planned: Seq[SearchPlan.QueryGroup],
      first: Long,
      until: Long,
      layout: VectorLayout,
      metric: String,
      ids: Seq[Array[Long]],
      builders: Seq[QueryMatrix.Builder],
      written: Seq[java.util.concurrent.atomic.AtomicInteger]
  ): Unit = {
    val rows = this.rows(Seq(rowGroup))
    var row = rowGroup.firstRow
    // The group the cursor is in: planned groups are contiguous, so it only
    // ever moves forward.
    var at = 0
    while (at < range.size && planned(range(at)).untilQuery <= row) at += 1
    var seen = 0L
    while (rows.hasNext) {
      val record = rows.next()
      if (row >= first && row < until) {
        while (planned(range(at)).untilQuery <= row) at += 1
        val group = planned(range(at))
        val position = (row - group.firstQuery).toInt
        require(
          !record.isNullAt(0),
          s"Query at row $row of the set has no ${SearchQueries.IdColumn}"
        )
        val id = record.getLong(0)
        require(
          !record.isNullAt(1),
          s"Query $id has no ${SearchQueries.VectorColumn}"
        )
        layout.elementType match {
          case VectorElementType.Int8 =>
            builders(at).writeBytes(
              position,
              SearchQueries.int8Values(
                id,
                record.getArray(1).toShortArray(),
                layout
              )
            )
          case VectorElementType.Bit =>
            builders(at).writeBytes(position, record.getBinary(1))
          case _ =>
            builders(at).writeFloats(
              position,
              SearchQueries.finiteValues(
                id,
                record.getArray(1).toFloatArray(),
                layout,
                metric
              )
            )
        }
        ids(at)(position) = id
        written(at).incrementAndGet()
      }
      row += 1
      seen += 1
    }
    require(
      seen == rowGroup.rows,
      s"Row group at ${rowGroup.path}:${rowGroup.start} has $seen rows; its footer says ${rowGroup.rows}"
    )
  }

  private def rows(parts: Seq[QueryFiles.RowGroup]): Iterator[InternalRow] =
    parts.iterator.flatMap { part =>
      readFile(
        PartitionedFile(
          InternalRow.empty,
          SparkPath.fromPathString(part.path),
          part.start,
          part.length,
          Array.empty[String],
          0L,
          files.find(_.path == part.path).map(_.length).getOrElse(part.length),
          Map.empty[String, Any]
        )
      ).flatMap { produced =>
        // The vectorized reader hands out ColumnarBatch objects typed as rows,
        // as FileSourceScanExec expects; a row iterator over the batch reuses
        // one row object, and the packer copies every value out as it goes.
        (produced: Any) match {
          case batch: ColumnarBatch => batch.rowIterator().asScala
          case row: InternalRow     => Iterator.single(row)
        }
      }
    }
}

private[read] object QueryFiles {

  /** One Parquet file of the set: where it is, how long, how many rows, and its
    * row groups.
    */
  final case class File(
      path: String,
      length: Long,
      rows: Long,
      rowGroups: Seq[RowGroup]
  )

  /** One row group: its byte range in the file (the reader given exactly this
    * range reads this row group alone), its rows, and the number of its first
    * row in the whole query sequence.
    */
  final case class RowGroup(
      path: String,
      start: Long,
      length: Long,
      rows: Long,
      firstRow: Long
  )

  /** Threads a task decodes with: one per row group up to the executor's cores
    * less one, which stays with the task thread.
    */
  def decodeThreads(rowGroups: Int): Int =
    math.max(
      1,
      math.min(rowGroups, Runtime.getRuntime.availableProcessors() - 1)
    )

  /** The query frame's files, when the frame is a plain scan of Parquet files
    * with at most a selection or renaming of its columns on top; None sends the
    * frame down the broadcast or shuffle path.
    */
  def of(spark: SparkSession, selected: DataFrame): Option[QueryFiles] = {
    val plan = selected.queryExecution.optimizedPlan
    source(plan, plan.output).flatMap { case (relation, columns) =>
      relation.relation match {
        case fs: HadoopFsRelation
            if fs.fileFormat.isInstanceOf[ParquetFileFormat] &&
              fs.partitionSchema.isEmpty && fs.bucketSpec.isEmpty =>
          val fields = columns.flatMap(column =>
            fs.dataSchema.fields.find(_.name == column.name)
          )
          val listed = fs.location
            .listFiles(Nil, Nil)
            .flatMap(_.files)
            .map(status => (status.getPath.toString, status.getLen))
            .sortBy(_._1)
          if (fields.size != columns.size || listed.isEmpty) None
          else {
            val required = StructType(fields)
            val conf = spark.sessionState.newHadoopConfWithOptions(fs.options)
            var firstRow = 0L
            val files = listed.map { case (path, length) =>
              val reader = ParquetFileReader.open(
                HadoopInputFile.fromPath(new Path(path), conf)
              )
              val blocks =
                try reader.getFooter.getBlocks.asScala.toSeq
                finally reader.close()
              val rowGroups = blocks.map { block =>
                val rowGroup = RowGroup(
                  path,
                  block.getStartingPos,
                  block.getCompressedSize,
                  block.getRowCount,
                  firstRow
                )
                firstRow += block.getRowCount
                rowGroup
              }
              File(path, length, rowGroups.map(_.rows).sum, rowGroups)
            }
            // Rows, not columnar batches: the Parquet reader insists on being
            // told which, as FileSourceScanExec tells it. The vectorized reader
            // still decodes the pages; it hands them out a row at a time.
            val readFile = fs.fileFormat.buildReaderWithPartitionValues(
              spark,
              fs.dataSchema,
              fs.partitionSchema,
              required,
              Nil,
              fs.options + (FileFormat.OPTION_RETURNING_BATCH -> "false"),
              conf
            )
            Some(QueryFiles(files, readFile))
          }
        case _ => None
      }
    }
  }

  /** The relation under a stack of projections that only pick or rename
    * columns, with the wanted output attributes traced to the relation's own.
    */
  private def source(
      plan: LogicalPlan,
      wanted: Seq[Attribute]
  ): Option[(LogicalRelation, Seq[Attribute])] = plan match {
    case Project(list, child) =>
      val traced = wanted.map { attribute =>
        list.find(_.exprId == attribute.exprId) match {
          case Some(reference: AttributeReference)           => Some(reference)
          case Some(Alias(reference: AttributeReference, _)) => Some(reference)
          case _                                             => None
        }
      }
      if (traced.exists(_.isEmpty)) None
      else source(child, traced.flatten)
    case relation: LogicalRelation =>
      val own = wanted.map(attribute =>
        relation.output.find(_.exprId == attribute.exprId)
      )
      if (own.exists(_.isEmpty)) None else Some((relation, own.flatten))
    case _ => None
  }
}
