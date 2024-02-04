package zilliztech.spark.milvus.reader

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.jdk.CollectionConverters.mapAsScalaMapConverter

case class MilvusCollectionScanBuilder(sparkSession: SparkSession,
                                       schema: StructType,
                                       options: CaseInsensitiveStringMap) extends ScanBuilder {

  lazy val hadoopConf = {
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    // Hadoop Configurations are case sensitive.
    sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
  }

  override def build(): Scan = {
    MilvusCollectionScan(sparkSession, hadoopConf, schema, options)
  }
}
