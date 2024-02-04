package zilliztech.spark.milvus.binlog

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.execution.datasources.parquet.{ParquetOptions, ParquetReadSupport, ParquetWriteSupport}
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.execution.datasources.{AggregatePushDownUtils, PartitioningAwareFileIndex, parquet}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration

import scala.collection.JavaConverters.mapAsScalaMapConverter

case class MilvusBinlogScan(
                             sparkSession: SparkSession,
                             hadoopConf: Configuration,
                             fileIndex: PartitioningAwareFileIndex,
                             dataSchema: StructType,
                             readDataSchema: StructType,
                             readPartitionSchema: StructType,
                             pushedFilters: Array[Filter],
                             options: CaseInsensitiveStringMap,
                             pushedAggregate: Option[Aggregation] = None,
                             partitionFilters: Seq[Expression] = Seq.empty,
                             dataFilters: Seq[Expression] = Seq.empty) extends FileScan {
  override def isSplitable(path: Path): Boolean = {
    // If aggregate is pushed down, only the file footer will be read once,
    // so file should not be split across multiple tasks.
    pushedAggregate.isEmpty
  }

  override def readSchema(): StructType = {
    // If aggregate is pushed down, schema has already been pruned in `ParquetScanBuilder`
    // and no need to call super.readSchema()
    if (pushedAggregate.nonEmpty) readDataSchema else super.readSchema()
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    val readDataSchemaAsJson = readDataSchema.json
    hadoopConf.set(ParquetInputFormat.READ_SUPPORT_CLASS, classOf[ParquetReadSupport].getName)
    hadoopConf.set(
      ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA,
      readDataSchemaAsJson)
    hadoopConf.set(
      ParquetWriteSupport.SPARK_ROW_SCHEMA,
      readDataSchemaAsJson)
    hadoopConf.set(
      SQLConf.SESSION_LOCAL_TIMEZONE.key,
      sparkSession.sessionState.conf.sessionLocalTimeZone)
    hadoopConf.setBoolean(
      SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key,
      sparkSession.sessionState.conf.nestedSchemaPruningEnabled)
    hadoopConf.setBoolean(
      SQLConf.CASE_SENSITIVE.key,
      sparkSession.sessionState.conf.caseSensitiveAnalysis)

    // Sets flags for `ParquetToSparkSchemaConverter`
    hadoopConf.setBoolean(
      SQLConf.PARQUET_BINARY_AS_STRING.key,
      sparkSession.sessionState.conf.isParquetBinaryAsString)
    hadoopConf.setBoolean(
      SQLConf.PARQUET_INT96_AS_TIMESTAMP.key,
      sparkSession.sessionState.conf.isParquetINT96AsTimestamp)

    hadoopConf.setBoolean(
      SQLConf.LEGACY_PARQUET_NANOS_AS_LONG.key,
      sparkSession.sessionState.conf.legacyParquetNanosAsLong)

    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf))
    val sqlConf = sparkSession.sessionState.conf
    parquet.MilvusBinlogPartitionReaderFactory(
      sqlConf,
      broadcastedConf,
      dataSchema,
      readDataSchema,
      readPartitionSchema,
      pushedFilters,
      pushedAggregate,
      new ParquetOptions(options.asCaseSensitiveMap.asScala.toMap, sqlConf))
  }

  override def equals(obj: Any): Boolean = obj match {
    case p: MilvusBinlogScan =>
      val pushedDownAggEqual = if (pushedAggregate.nonEmpty && p.pushedAggregate.nonEmpty) {
        AggregatePushDownUtils.equivalentAggregations(pushedAggregate.get, p.pushedAggregate.get)
      } else {
        pushedAggregate.isEmpty && p.pushedAggregate.isEmpty
      }
      super.equals(p) && dataSchema == p.dataSchema && options == p.options &&
        equivalentFilters(pushedFilters, p.pushedFilters) && pushedDownAggEqual
    case _ => false
  }

  override def hashCode(): Int = getClass.hashCode()

  lazy private val (pushedAggregationsStr, pushedGroupByStr) = if (pushedAggregate.nonEmpty) {
    (seqToString(pushedAggregate.get.aggregateExpressions),
      seqToString(pushedAggregate.get.groupByExpressions))
  } else {
    ("[]", "[]")
  }

  override def description(): String = {
    super.description() + ", PushedFilters: " + seqToString(pushedFilters) +
      ", PushedAggregation: " + pushedAggregationsStr +
      ", PushedGroupBy: " + pushedGroupByStr
  }

  override def getMetaData(): Map[String, String] = {
    super.getMetaData() ++ Map("PushedFilters" -> seqToString(pushedFilters)) ++
      Map("PushedAggregation" -> pushedAggregationsStr) ++
      Map("PushedGroupBy" -> pushedGroupByStr)
  }
}
