package zilliztech.spark.milvus.binlog

import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.MilvusBinlogFileFormat
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import zilliztech.spark.milvus.binlog

class MilvusBinlogDataSource extends FileDataSourceV2 {

  override def fallbackFileFormat: Class[_ <: FileFormat] = classOf[MilvusBinlogFileFormat]

  override def shortName(): String = "milvusbinlog"

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    val paths = getPaths(options)
    val tableName = getTableName(options, paths)
    val optionsWithoutPaths = getOptionsWithoutPaths(options)
    MilvusBinlogTable(tableName, sparkSession, optionsWithoutPaths, paths, None, fallbackFileFormat)
  }

  override def getTable(options: CaseInsensitiveStringMap, schema: StructType): Table = {
    val paths = getPaths(options)
    val tableName = getTableName(options, paths)
    val optionsWithoutPaths = getOptionsWithoutPaths(options)
    binlog.MilvusBinlogTable(tableName, sparkSession, optionsWithoutPaths, paths, Some(schema), fallbackFileFormat)
  }
}
