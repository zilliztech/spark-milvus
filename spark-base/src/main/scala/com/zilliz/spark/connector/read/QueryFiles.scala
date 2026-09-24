package com.zilliz.spark.connector.read

import scala.jdk.CollectionConverters._

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

import com.zilliz.milvus.storage.index.SearchPlan
import com.zilliz.milvus.storage.schema.VectorLayout

/** A query set that is nothing but Parquet files, read by the search tasks
  * themselves (docs/design/architecture/vector-search.html section 2.1).
  *
  * The broadcast path evaluates the query frame on the executors, brings the
  * packed bytes to the driver because `SparkContext.broadcast` takes a driver
  * value, and ships them back; the driver is the one receiver of that collect,
  * and on P3 a 738 MiB set spent 8.6 s there. When the frame is a plain scan of
  * Parquet files -- a `LogicalRelation` over `HadoopFsRelation` under nothing
  * but column selection -- none of that is needed: the driver reads the footers
  * for the row counts that planning wants, and every first-stage task opens the
  * files with the reader Spark itself would have used and packs its query
  * groups as the rows arrive. The bytes read are the same, eight tasks reading
  * a file each instead of one driver receiving eight blocks; what goes is the
  * serial hop.
  *
  * `readFile` is the function `FileFormat.buildReaderWithPartitionValues`
  * returns: it captures the Hadoop configuration and the reader settings on the
  * driver and runs on the executors, which is how `FileSourceScanExec` reads a
  * file too. Its output is rows, or columnar batches typed as rows when the
  * vectorized reader applies; both are taken one row at a time and nothing is
  * retained.
  */
private[read] final case class QueryFiles(
    files: Seq[QueryFiles.File],
    readFile: PartitionedFile => Iterator[InternalRow]
) extends Serializable {

  def queries: Long = files.iterator.map(_.rows).sum

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
    val rows = this.rows()
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

  private def rows(): Iterator[InternalRow] =
    files.iterator.flatMap { file =>
      readFile(
        PartitionedFile(
          InternalRow.empty,
          SparkPath.fromPathString(file.path),
          0L,
          file.length,
          Array.empty[String],
          0L,
          file.length,
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

  /** One Parquet file of the set: where it is, how long, how many rows. */
  final case class File(path: String, length: Long, rows: Long)

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
            val files = listed.map { case (path, length) =>
              val reader = ParquetFileReader.open(
                HadoopInputFile.fromPath(new Path(path), conf)
              )
              val rows =
                try reader.getFooter.getBlocks.asScala.map(_.getRowCount).sum
                finally reader.close()
              File(path, length, rows)
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
