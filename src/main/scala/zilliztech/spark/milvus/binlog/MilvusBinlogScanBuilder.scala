package zilliztech.spark.milvus.binlog

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.RebaseDateTime.RebaseSpec
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Scan, SupportsPushDownAggregates}
import org.apache.spark.sql.execution.datasources.parquet.{MSparkToParquetSchemaConverter, ParquetFilters}
import org.apache.spark.sql.execution.datasources.v2.FileScanBuilder
import org.apache.spark.sql.execution.datasources.{AggregatePushDownUtils, PartitioningAwareFileIndex}
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters.mapAsScalaMapConverter

case class MilvusBinlogScanBuilder(
                                    sparkSession: SparkSession,
                                    fileIndex: PartitioningAwareFileIndex,
                                    schema: StructType,
                                    dataSchema: StructType,
                                    options: CaseInsensitiveStringMap)
  extends FileScanBuilder(sparkSession, fileIndex, dataSchema)
    with SupportsPushDownAggregates {
  lazy val hadoopConf = {
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    // Hadoop Configurations are case sensitive.
    sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
  }

  lazy val pushedParquetFilters = {
    val sqlConf = sparkSession.sessionState.conf
    if (sqlConf.parquetFilterPushDown) {
      val pushDownDate = sqlConf.parquetFilterPushDownDate
      val pushDownTimestamp = sqlConf.parquetFilterPushDownTimestamp
      val pushDownDecimal = sqlConf.parquetFilterPushDownDecimal
      val pushDownStringStartWith = sqlConf.parquetFilterPushDownStringStartWith
      val pushDownInFilterThreshold = sqlConf.parquetFilterPushDownInFilterThreshold
      val isCaseSensitive = sqlConf.caseSensitiveAnalysis
      val parquetSchema =
        new MSparkToParquetSchemaConverter(sparkSession.sessionState.conf).convert(readDataSchema())
      val parquetFilters = new ParquetFilters(
        parquetSchema,
        pushDownDate,
        pushDownTimestamp,
        pushDownDecimal,
        pushDownStringStartWith,
        pushDownInFilterThreshold,
        isCaseSensitive,
        // The rebase mode doesn't matter here because the filters are used to determine
        // whether they is convertible.
        RebaseSpec(LegacyBehaviorPolicy.CORRECTED))
      parquetFilters.convertibleFilters(pushedDataFilters).toArray
    } else {
      Array.empty[Filter]
    }
  }

  private var finalSchema = new StructType()

  private var pushedAggregations = Option.empty[Aggregation]

  override protected val supportsNestedSchemaPruning: Boolean = true

  override def pushDataFilters(dataFilters: Array[Filter]): Array[Filter] = dataFilters

  // Note: for Parquet, the actual filter push down happens in [[ParquetPartitionReaderFactory]].
  // It requires the Parquet physical schema to determine whether a filter is convertible.
  // All filters that can be converted to Parquet are pushed down.
  //  override def pushedFilters: Array[Predicate] = pushedParquetFilters.map(_.toV2)
  override def pushedFilters: Array[Predicate] = Array.empty[Predicate]

  override def pushAggregation(aggregation: Aggregation): Boolean = {
    if (!sparkSession.sessionState.conf.parquetAggregatePushDown) {
      return false
    }

    AggregatePushDownUtils.getSchemaForPushedAggregation(
      aggregation,
      schema,
      partitionNameSet,
      dataFilters) match {

      case Some(schema) =>
        finalSchema = schema
        this.pushedAggregations = Some(aggregation)
        true
      case _ => false
    }
  }

  override def build(): Scan = {
    // the `finalSchema` is either pruned in pushAggregation (if aggregates are
    // pushed down), or pruned in readDataSchema() (in regular column pruning). These
    // two are mutual exclusive.
    if (pushedAggregations.isEmpty) {
      finalSchema = readDataSchema()
    }

    MilvusBinlogScan(sparkSession, hadoopConf, fileIndex, dataSchema, finalSchema,
      readPartitionSchema(), pushedParquetFilters, options, pushedAggregations,
      partitionFilters, dataFilters)
  }
}
