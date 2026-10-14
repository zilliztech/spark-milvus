package com.zilliz.spark.connector.write

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.write.BatchWrite
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.DataFrame

import com.zilliz.spark.connector.options.{
  MilvusOption,
  OptionParsing,
  StorageOptions
}
import io.milvus.grpc.schema.CollectionSchema

/** The direct entry point, for callers that are not Spark's write protocol:
  * integration tests use it. `df.write.format("milvus")` goes through
  * `MilvusTable.newWriteBuilder` and ends in the same `MilvusV3BatchWrite`.
  */
object MilvusV3Writer extends Logging {

  /** Writes a DataFrame as V3 segments, one per Spark partition, and commits
    * the job through `core.write.commit`.
    *
    * The columns are checked against `collection` first: with
    * `milvus.writer.commitType=addfield` (backfill) only the given columns have
    * to be fields; otherwise the DataFrame has to carry every field. The `fs.*`
    * options name the storage; `fs.root_path` is the root the staging prefix
    * goes under; `milvus.writer.customPath` names an existing segment directory
    * instead (backfill).
    *
    * @return
    *   the base path of every segment written
    */
  def writeDataFrame(
      df: DataFrame,
      options: Map[String, String],
      collection: CollectionSchema
  ): Try[Seq[String]] = {

    try {
      val optionsMap = new CaseInsensitiveStringMap(options.asJava)
      val milvusOption = MilvusOption(optionsMap)
      val mode =
        if (
          OptionParsing
            .value(milvusOption.options, MilvusOption.WriterCommitType)
            .flatMap(Option(_))
            .exists(_.trim.equalsIgnoreCase("addfield"))
        ) WriteSchema.Mode.Columns
        else WriteSchema.Mode.Append
      val schema = WriteSchema.resolve(df.schema, collection, mode)
      val storage =
        StorageOptions.writeStorageProperties(milvusOption.options, "")
      Success(
        writeV3(df, new MilvusV3BatchWrite(schema, milvusOption, storage))
      )
    } catch {
      case e: Exception =>
        logError(
          s"Failed to write DataFrame as V3 segments: ${e.getMessage}",
          e
        )
        Failure(e)
    }
  }

  private[write] def writeV3(
      df: DataFrame,
      batchWrite: BatchWrite
  ): Seq[String] = {
    val messages =
      try {
        val writerFactory = batchWrite.createBatchWriterFactory(null)
        df.queryExecution.toRdd
          .mapPartitionsWithIndex { (partitionId, rows) =>
            val writer =
              writerFactory.createWriter(
                partitionId,
                System.currentTimeMillis()
              )
            try {
              rows.foreach(writer.write)
              Iterator(writer.commit())
            } catch {
              case e: Exception =>
                writer.abort()
                throw e
            } finally {
              writer.close()
            }
          }
          .collect()
      } catch {
        case failure: Throwable =>
          try batchWrite.abort(Array.empty)
          catch {
            case aborted: Throwable =>
              if (aborted ne failure) failure.addSuppressed(aborted)
          }
          throw failure
      }

    // Commit stops the heartbeat itself. Its marker may already be durable if
    // closing storage fails, so a commit failure must not delete the output.
    batchWrite.commit(messages)
    messages.collect { case msg: MilvusV3CommitMessage =>
      msg.manifestPath
    }.toSeq
  }
}
