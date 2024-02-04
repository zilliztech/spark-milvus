package org.apache.spark.sql.execution.datasources.parquet

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.{Job, OutputCommitter, TaskAttemptContext}
import org.apache.parquet.hadoop.ParquetOutputFormat.JobSummaryLevel
import org.apache.parquet.hadoop.codec.CodecConfig
import org.apache.parquet.hadoop.metadata.{ColumnChunkMetaData, ParquetMetadata}
import org.apache.parquet.hadoop.util.ContextUtil
import org.apache.parquet.hadoop.{ParquetFileWriter, ParquetOutputCommitter, ParquetOutputFormat}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.{PrimitiveType, Types}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.expressions.aggregate._
import org.apache.spark.sql.execution.datasources.v2.V2ColumnUtils
import org.apache.spark.sql.execution.datasources.{OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.PARQUET_AGGREGATE_PUSHDOWN_ENABLED
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

import java.util
import scala.collection.mutable

object MilvusBinlogUtils extends Logging {

  def inferSchema(
                   sparkSession: SparkSession,
                   parameters: Map[String, String],
                   files: Seq[FileStatus]): Option[StructType] = {
    val parquetOptions = new ParquetOptions(parameters, sparkSession.sessionState.conf)

    // Should we merge schemas from all Parquet part-files?
    val shouldMergeSchemas = parquetOptions.mergeSchema

    val mergeRespectSummaries = sparkSession.sessionState.conf.isParquetSchemaRespectSummaries

    val filesByType = splitFiles(files)

    // Sees which file(s) we need to touch in order to figure out the schema.
    //
    // Always tries the summary files first if users don't require a merged schema.  In this case,
    // "_common_metadata" is more preferable than "_metadata" because it doesn't contain row
    // groups information, and could be much smaller for large Parquet files with lots of row
    // groups.  If no summary file is available, falls back to some random part-file.
    //
    // NOTE: Metadata stored in the summary files are merged from all part-files.  However, for
    // user defined key-value metadata (in which we store Spark SQL schema), Parquet doesn't know
    // how to merge them correctly if some key is associated with different values in different
    // part-files.  When this happens, Parquet simply gives up generating the summary file.  This
    // implies that if a summary file presents, then:
    //
    //   1. Either all part-files have exactly the same Spark SQL schema, or
    //   2. Some part-files don't contain Spark SQL schema in the key-value metadata at all (thus
    //      their schemas may differ from each other).
    //
    // Here we tend to be pessimistic and take the second case into account.  Basically this means
    // we can't trust the summary files if users require a merged schema, and must touch all part-
    // files to do the merge.
    val filesToTouch =
    if (shouldMergeSchemas) {
      // Also includes summary files, 'cause there might be empty partition directories.

      // If mergeRespectSummaries config is true, we assume that all part-files are the same for
      // their schema with summary files, so we ignore them when merging schema.
      // If the config is disabled, which is the default setting, we merge all part-files.
      // In this mode, we only need to merge schemas contained in all those summary files.
      // You should enable this configuration only if you are very sure that for the parquet
      // part-files to read there are corresponding summary files containing correct schema.

      // As filed in SPARK-11500, the order of files to touch is a matter, which might affect
      // the ordering of the output columns. There are several things to mention here.
      //
      //  1. If mergeRespectSummaries config is false, then it merges schemas by reducing from
      //     the first part-file so that the columns of the lexicographically first file show
      //     first.
      //
      //  2. If mergeRespectSummaries config is true, then there should be, at least,
      //     "_metadata"s for all given files, so that we can ensure the columns of
      //     the lexicographically first file show first.
      //
      //  3. If shouldMergeSchemas is false, but when multiple files are given, there is
      //     no guarantee of the output order, since there might not be a summary file for the
      //     lexicographically first file, which ends up putting ahead the columns of
      //     the other files. However, this should be okay since not enabling
      //     shouldMergeSchemas means (assumes) all the files have the same schemas.

      val needMerged: Seq[FileStatus] =
        if (mergeRespectSummaries) {
          Seq.empty
        } else {
          filesByType.data
        }
      needMerged ++ filesByType.metadata ++ filesByType.commonMetadata
    } else {
      // Tries any "_common_metadata" first. Parquet files written by old versions or Parquet
      // don't have this.
      filesByType.commonMetadata.headOption
        // Falls back to "_metadata"
        .orElse(filesByType.metadata.headOption)
        // Summary file(s) not found, the Parquet file is either corrupted, or different part-
        // files contain conflicting user defined metadata (two or more values are associated
        // with a same key in different files).  In either case, we fall back to any of the
        // first part-file, and just assume all schemas are consistent.
        .orElse(filesByType.data.headOption)
        .toSeq
    }
    MilvusBinlogFileFormat.mergeSchemasInParallel(parameters, filesToTouch, sparkSession)
  }

  case class FileTypes(
                        data: Seq[FileStatus],
                        metadata: Seq[FileStatus],
                        commonMetadata: Seq[FileStatus])

  private def splitFiles(allFiles: Seq[FileStatus]): FileTypes = {
    val leaves = allFiles.toArray.sortBy(_.getPath.toString)

    FileTypes(
      data = leaves.filterNot(f => isSummaryFile(f.getPath)),
      metadata =
        leaves.filter(_.getPath.getName == ParquetFileWriter.PARQUET_METADATA_FILE),
      commonMetadata =
        leaves.filter(_.getPath.getName == ParquetFileWriter.PARQUET_COMMON_METADATA_FILE))
  }

  private def isSummaryFile(file: Path): Boolean = {
    file.getName == ParquetFileWriter.PARQUET_COMMON_METADATA_FILE ||
      file.getName == ParquetFileWriter.PARQUET_METADATA_FILE
  }

  /**
   * A StructField metadata key used to set the field id of a column in the Parquet schema.
   */
  val FIELD_ID_METADATA_KEY = "parquet.field.id"

  /**
   * Whether there exists a field in the schema, whether inner or leaf, has the parquet field
   * ID metadata.
   */
  def hasFieldIds(schema: StructType): Boolean = {
    def recursiveCheck(schema: DataType): Boolean = {
      schema match {
        case st: StructType =>
          st.exists(field => hasFieldId(field) || recursiveCheck(field.dataType))

        case at: ArrayType => recursiveCheck(at.elementType)

        case mt: MapType => recursiveCheck(mt.keyType) || recursiveCheck(mt.valueType)

        case _ =>
          // No need to really check primitive types, just to terminate the recursion
          false
      }
    }

    if (schema.isEmpty) false else recursiveCheck(schema)
  }

  def hasFieldId(field: StructField): Boolean =
    field.metadata.contains(FIELD_ID_METADATA_KEY)

  def getFieldId(field: StructField): Int = {
    require(hasFieldId(field),
      s"The key `$FIELD_ID_METADATA_KEY` doesn't exist in the metadata of " + field)
    try {
      Math.toIntExact(field.metadata.getLong(FIELD_ID_METADATA_KEY))
    } catch {
      case _: ArithmeticException | _: ClassCastException =>
        throw new IllegalArgumentException(
          s"The key `$FIELD_ID_METADATA_KEY` must be a 32-bit integer")
    }
  }

  /**
   * Whether columnar read is supported for the input `schema`.
   */
  def isBatchReadSupportedForSchema(sqlConf: SQLConf, schema: StructType): Boolean =
    sqlConf.parquetVectorizedReaderEnabled &&
      schema.forall(f => isBatchReadSupported(sqlConf, f.dataType))

  def isBatchReadSupported(sqlConf: SQLConf, dt: DataType): Boolean = dt match {
    case _: AtomicType =>
      true
    case at: ArrayType =>
      sqlConf.parquetVectorizedReaderNestedColumnEnabled &&
        isBatchReadSupported(sqlConf, at.elementType)
    case mt: MapType =>
      sqlConf.parquetVectorizedReaderNestedColumnEnabled &&
        isBatchReadSupported(sqlConf, mt.keyType) &&
        isBatchReadSupported(sqlConf, mt.valueType)
    case st: StructType =>
      sqlConf.parquetVectorizedReaderNestedColumnEnabled &&
        st.fields.forall(f => isBatchReadSupported(sqlConf, f.dataType))
    case udt: UserDefinedType[_] =>
      isBatchReadSupported(sqlConf, udt.sqlType)
    case _ =>
      false
  }

  /**
   * Calculate the pushed down aggregates (Max/Min/Count) result using the statistics
   * information from Parquet footer file.
   *
   * @return A tuple of `Array[PrimitiveType]` and Array[Any].
   *         The first element is the Parquet PrimitiveType of the aggregate column,
   *         and the second element is the aggregated value.
   */
  private[sql] def getPushedDownAggResult(
                                           footer: ParquetMetadata,
                                           filePath: String,
                                           dataSchema: StructType,
                                           partitionSchema: StructType,
                                           aggregation: Aggregation)
  : (Array[PrimitiveType], Array[Any]) = {
    val footerFileMetaData = footer.getFileMetaData
    val fields = footerFileMetaData.getSchema.getFields
    val blocks = footer.getBlocks
    val primitiveTypeBuilder = mutable.ArrayBuilder.make[PrimitiveType]
    val valuesBuilder = mutable.ArrayBuilder.make[Any]

    aggregation.aggregateExpressions.foreach { agg =>
      var value: Any = None
      var rowCount = 0L
      var isCount = false
      var index = 0
      var schemaName = ""
      blocks.forEach { block =>
        val blockMetaData = block.getColumns
        agg match {
          case max: Max if V2ColumnUtils.extractV2Column(max.column).isDefined =>
            val colName = V2ColumnUtils.extractV2Column(max.column).get
            index = dataSchema.fieldNames.toList.indexOf(colName)
            schemaName = "max(" + colName + ")"
            val currentMax = getCurrentBlockMaxOrMin(filePath, blockMetaData, index, true)
            if (value == None || currentMax.asInstanceOf[Comparable[Any]].compareTo(value) > 0) {
              value = currentMax
            }
          case min: Min if V2ColumnUtils.extractV2Column(min.column).isDefined =>
            val colName = V2ColumnUtils.extractV2Column(min.column).get
            index = dataSchema.fieldNames.toList.indexOf(colName)
            schemaName = "min(" + colName + ")"
            val currentMin = getCurrentBlockMaxOrMin(filePath, blockMetaData, index, false)
            if (value == None || currentMin.asInstanceOf[Comparable[Any]].compareTo(value) < 0) {
              value = currentMin
            }
          case count: Count if V2ColumnUtils.extractV2Column(count.column).isDefined =>
            val colName = V2ColumnUtils.extractV2Column(count.column).get
            schemaName = "count(" + colName + ")"
            rowCount += block.getRowCount
            var isPartitionCol = false
            if (partitionSchema.fields.map(_.name).toSet.contains(colName)) {
              isPartitionCol = true
            }
            isCount = true
            if (!isPartitionCol) {
              index = dataSchema.fieldNames.toList.indexOf(colName)
              // Count(*) includes the null values, but Count(colName) doesn't.
              rowCount -= getNumNulls(filePath, blockMetaData, index)
            }
          case _: CountStar =>
            schemaName = "count(*)"
            rowCount += block.getRowCount
            isCount = true
          case _ =>
        }
      }
      if (isCount) {
        valuesBuilder += rowCount
        primitiveTypeBuilder += Types.required(PrimitiveTypeName.INT64).named(schemaName);
      } else {
        valuesBuilder += value
        val field = fields.get(index)
        primitiveTypeBuilder += Types.required(field.asPrimitiveType.getPrimitiveTypeName)
          .as(field.getLogicalTypeAnnotation)
          .length(field.asPrimitiveType.getTypeLength)
          .named(schemaName)
      }
    }
    (primitiveTypeBuilder.result, valuesBuilder.result)
  }

  /**
   * Get the Max or Min value for ith column in the current block
   *
   * @return the Max or Min value
   */
  private def getCurrentBlockMaxOrMin(
                                       filePath: String,
                                       columnChunkMetaData: util.List[ColumnChunkMetaData],
                                       i: Int,
                                       isMax: Boolean): Any = {
    val statistics = columnChunkMetaData.get(i).getStatistics
    if (!statistics.hasNonNullValue) {
      throw new UnsupportedOperationException(s"No min/max found for Parquet file $filePath. " +
        s"Set SQLConf ${PARQUET_AGGREGATE_PUSHDOWN_ENABLED.key} to false and execute again")
    } else {
      if (isMax) statistics.genericGetMax else statistics.genericGetMin
    }
  }

  private def getNumNulls(
                           filePath: String,
                           columnChunkMetaData: util.List[ColumnChunkMetaData],
                           i: Int): Long = {
    val statistics = columnChunkMetaData.get(i).getStatistics
    if (!statistics.isNumNullsSet) {
      throw new UnsupportedOperationException(s"Number of nulls not set for Parquet file" +
        s" $filePath. Set SQLConf ${PARQUET_AGGREGATE_PUSHDOWN_ENABLED.key} to false and execute" +
        s" again")
    }
    statistics.getNumNulls;
  }

  def prepareWrite(
                    sqlConf: SQLConf,
                    job: Job,
                    dataSchema: StructType,
                    parquetOptions: ParquetOptions): OutputWriterFactory = {
    val conf = ContextUtil.getConfiguration(job)

    val committerClass =
      conf.getClass(
        SQLConf.PARQUET_OUTPUT_COMMITTER_CLASS.key,
        classOf[ParquetOutputCommitter],
        classOf[OutputCommitter])

    if (conf.get(SQLConf.PARQUET_OUTPUT_COMMITTER_CLASS.key) == null) {
      logInfo("Using default output committer for Parquet: " +
        classOf[ParquetOutputCommitter].getCanonicalName)
    } else {
      logInfo("Using user defined output committer for Parquet: " + committerClass.getCanonicalName)
    }

    conf.setClass(
      SQLConf.OUTPUT_COMMITTER_CLASS.key,
      committerClass,
      classOf[OutputCommitter])

    // We're not really using `ParquetOutputFormat[Row]` for writing data here, because we override
    // it in `ParquetOutputWriter` to support appending and dynamic partitioning.  The reason why
    // we set it here is to setup the output committer class to `ParquetOutputCommitter`, which is
    // bundled with `ParquetOutputFormat[Row]`.
    job.setOutputFormatClass(classOf[ParquetOutputFormat[Row]])

    ParquetOutputFormat.setWriteSupportClass(job, classOf[ParquetWriteSupport])

    // This metadata is useful for keeping UDTs like Vector/Matrix.
    ParquetWriteSupport.setSchema(dataSchema, conf)

    // Sets flags for `ParquetWriteSupport`, which converts Catalyst schema to Parquet
    // schema and writes actual rows to Parquet files.
    conf.set(
      SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key,
      sqlConf.writeLegacyParquetFormat.toString)

    conf.set(
      SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key,
      sqlConf.parquetOutputTimestampType.toString)

    conf.set(
      SQLConf.PARQUET_FIELD_ID_WRITE_ENABLED.key,
      sqlConf.parquetFieldIdWriteEnabled.toString)

    // Sets compression scheme
    conf.set(ParquetOutputFormat.COMPRESSION, parquetOptions.compressionCodecClassName)

    // SPARK-15719: Disables writing Parquet summary files by default.
    if (conf.get(ParquetOutputFormat.JOB_SUMMARY_LEVEL) == null
      && conf.get(ParquetOutputFormat.ENABLE_JOB_SUMMARY) == null) {
      conf.setEnum(ParquetOutputFormat.JOB_SUMMARY_LEVEL, JobSummaryLevel.NONE)
    }

    if (ParquetOutputFormat.getJobSummaryLevel(conf) != JobSummaryLevel.NONE
      && !classOf[ParquetOutputCommitter].isAssignableFrom(committerClass)) {
      // output summary is requested, but the class is not a Parquet Committer
      logWarning(s"Committer $committerClass is not a ParquetOutputCommitter and cannot" +
        s" create job summaries. " +
        s"Set Parquet option ${ParquetOutputFormat.JOB_SUMMARY_LEVEL} to NONE.")
    }

    new OutputWriterFactory {
      override def newInstance(
                                path: String,
                                dataSchema: StructType,
                                context: TaskAttemptContext): OutputWriter = {
        new ParquetOutputWriter(path, context)
      }

      override def getFileExtension(context: TaskAttemptContext): String = {
        CodecConfig.from(context).getCodec.getExtension + ".parquet"
      }
    }
  }
}
