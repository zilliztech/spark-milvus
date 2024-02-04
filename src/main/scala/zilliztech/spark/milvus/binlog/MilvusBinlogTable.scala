package zilliztech.spark.milvus.binlog

import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, Write, WriteBuilder}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.parquet.MilvusBinlogUtils
import org.apache.spark.sql.execution.datasources.v2.FileTable
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters.mapAsScalaMapConverter

case class MilvusBinlogTable(name: String,
                             sparkSession: SparkSession,
                             options: CaseInsensitiveStringMap,
                             paths: Seq[String],
                             userSpecifiedSchema: Option[StructType],
                             fallbackFileFormat: Class[_ <: FileFormat])
  extends FileTable(sparkSession, options, paths, userSpecifiedSchema) {

  override def newScanBuilder(options: CaseInsensitiveStringMap): MilvusBinlogScanBuilder =
    MilvusBinlogScanBuilder(sparkSession, fileIndex, schema, dataSchema, options)

  override def inferSchema(files: Seq[FileStatus]): Option[StructType] = {
    MilvusBinlogUtils.inferSchema(sparkSession, options.asScala.toMap, files)
  }

  override def supportsDataType(dataType: DataType): Boolean = dataType match {
    //    case _: AtomicType => true
    case _: BinaryType => true
    case _: BooleanType => true
    case _: ByteType => true
    case _: CharType => true
    case _: DateType => true
    case _: DayTimeIntervalType => true
    case _: NumericType => true
    case _: StringType => true
    case _: VarcharType => true
    case _: TimestampType => true
    case _: YearMonthIntervalType => true

    case st: StructType => st.forall { f => supportsDataType(f.dataType) }

    case ArrayType(elementType, _) => supportsDataType(elementType)

    case MapType(keyType, valueType, _) =>
      supportsDataType(keyType) && supportsDataType(valueType)

    case udt: UserDefinedType[_] => supportsDataType(udt.sqlType)

    case _ => false
  }

  override def formatName: String = "Parquet"

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    new WriteBuilder {
      override def build(): Write = MilvusBinlogWrite(paths, formatName, supportsDataType, info)
    }

}
