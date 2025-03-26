import com.aliyun.oss.OSS
import com.zilliztech.spark.l0data.DeltaLogUtils
import milvus.proto.backup.Backup
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import java.util
import scala.collection.mutable
object L0SegmentUtils {
  private val log = LoggerFactory.getLogger(getClass)

  def getOSSPartitionID2DF(
                           segments: mutable.Buffer[Backup.SegmentBackupInfo],
                           bucketName: String,
                           backupPath: String,
                           spark: SparkSession,
                           pkType: milvus.proto.backup.Backup.DataType,
                           ossClient: OSS
                         ): util.HashMap[Long, DataFrame] = {

      val partitionID2deltaPaths = new util.HashMap[Long, util.List[String]]()

      // Iterate through all L0 segments and collect delta log file paths for each partition
      segments.filter(_.getIsL0()).foreach { segment =>
        log.info("found l0 segment and begin to add delta path in oss")
        val partitionId = segment.getPartitionId
        val deltaDir = s"oss://$bucketName/$backupPath/binlogs/delta_log/${segment.getCollectionId}/$partitionId/${segment.getSegmentId}/*"
        val deltaPaths = partitionID2deltaPaths.getOrDefault(partitionId, new util.ArrayList[String]())
        deltaPaths.addAll(DeltaLogUtils.expandOssGlob(ossClient, deltaDir))
        partitionID2deltaPaths.put(partitionId, deltaPaths)
      }

      val partitionID2DF = new util.HashMap[Long, DataFrame]()

      // Iterate through the collected delta log paths and create DataFrames
      partitionID2deltaPaths.forEach { (partitionID, deltaPaths) =>
        if (deltaPaths.size() > 0) { // Ensure there are valid delta log files
          log.info(s"Partition $partitionID - delta log file count: ${deltaPaths.size()}")
          val l0DF = DeltaLogUtils.createDataFrame(deltaPaths, spark, getDataTypeFrom(pkType), ossClient)

          // Ensure that the resulting DataFrame is not empty
          if (!l0DF.isEmpty) {
            partitionID2DF.put(partitionID, l0DF)
            log.info(s"Finished reading partition $partitionID L0 segments, count: ${l0DF.count()}")
          }
        }
      }

      partitionID2DF
    }

  def getPartitionID2DF(
                                    segments: mutable.Buffer[Backup.SegmentBackupInfo],
                                    bucketName: String,
                                    backupPath: String,
                                    spark: SparkSession,
                                    pkType: milvus.proto.backup.Backup.DataType
                                  ): util.HashMap[Long, DataFrame] = {

    val partitionID2deltaPaths = new util.HashMap[Long, util.List[String]]()

    // Iterate through all L0 segments and collect delta log file paths for each partition
    segments.filter(_.getIsL0()).foreach { segment =>
      log.info("found l0 segment and begin to add delta path")
      val partitionId = segment.getPartitionId
      val deltaDir = s"$bucketName/$backupPath/binlogs/delta_log/${segment.getCollectionId}/$partitionId/${segment.getSegmentId}/*"
      val deltaPaths = partitionID2deltaPaths.getOrDefault(partitionId, new util.ArrayList[String]())
      //deltaPaths.addAll(DeltaLogUtils.expandGlobPattern(client, deltaDir))
      deltaPaths.addAll(DeltaLogUtils.expandGlobPattern(deltaDir))

      log.info(s"added deltapath, deltaDir: ${deltaDir}, delpaths:${deltaPaths}")
      partitionID2deltaPaths.put(partitionId, deltaPaths)
    }

    val partitionID2DF = new util.HashMap[Long, DataFrame]()

    // Iterate through the collected delta log paths and create DataFrames
    partitionID2deltaPaths.forEach { (partitionID, deltaPaths) =>
      if (deltaPaths.size() > 0) { // Ensure there are valid delta log files
        log.info(s"Partition $partitionID - delta log file count: ${deltaPaths.size()}")
        val l0DF = DeltaLogUtils.createDataFrame(deltaPaths, spark, getDataTypeFrom(pkType), null)

        // Ensure that the resulting DataFrame is not empty
        if (!l0DF.isEmpty) {
          partitionID2DF.put(partitionID, l0DF)
          log.info(s"Finished reading partition $partitionID L0 segments, count: ${l0DF.count()}")
        }
      }
    }

    partitionID2DF
  }

  def getGlobalL0DF(coll: Backup.CollectionBackupInfo,
                            bucketName: String,
                            backupPath: String,
                            spark: SparkSession,
                            pkType: milvus.proto.backup.Backup.DataType): DataFrame = {
    val deltaPaths = new util.ArrayList[String]()

    // Collect L0 delta log paths for the entire collection
    coll.getL0SegmentsList().forEach { segment =>
      val deltaDir = "%s/%s/binlogs/delta_log/%d/%d/%d/*".format(
        bucketName, backupPath,
        segment.getCollectionId, segment.getPartitionId,
        segment.getSegmentId
      )
      deltaPaths.addAll(DeltaLogUtils.expandGlobPattern(deltaDir))
    }

    // Create DataFrame from collected delta logs
    if (deltaPaths.size() > 0) {
      log.info(s"Global delta log file count: ${deltaPaths.size()}")
      val globalL0DF = DeltaLogUtils.createDataFrame(deltaPaths, spark, getDataTypeFrom(pkType), null)
      globalL0DF.show(10)
      log.info(s"Finished reading global L0 segments, count: ${globalL0DF.count()}")
      globalL0DF
    } else {
      log.info("No global L0 delta logs found.")
      spark.emptyDataFrame
    }
  }

  def getOSSGlobalL0DF(coll: Backup.CollectionBackupInfo,
                    bucketName: String,
                    backupPath: String,
                    spark: SparkSession,
                    pkType: milvus.proto.backup.Backup.DataType,
                    ossClient: OSS): DataFrame = {
    val deltaPaths = new util.ArrayList[String]()

    // Collect L0 delta log paths for the entire collection
    coll.getL0SegmentsList().forEach { segment =>
      val deltaDir = "oss://%s/%s/binlogs/delta_log/%d/%d/%d/*".format(
        bucketName, backupPath,
        segment.getCollectionId, segment.getPartitionId,
        segment.getSegmentId
      )
      deltaPaths.addAll(DeltaLogUtils.expandOssGlob(ossClient, deltaDir))
    }

    // Create DataFrame from collected delta logs
    if (deltaPaths.size() > 0) {
      log.info(s"Global delta log file count: ${deltaPaths.size()}")
      val globalL0DF = DeltaLogUtils.createDataFrame(deltaPaths, spark, getDataTypeFrom(pkType), ossClient)
      globalL0DF.show(10)
      log.info(s"Finished reading global L0 segments, count: ${globalL0DF.count()}")
      globalL0DF
    } else {
      log.info("No global L0 delta logs found.")
      spark.emptyDataFrame
    }
  }

  private def getDataTypeFrom(dt: milvus.proto.backup.Backup.DataType): org.apache.spark.sql.types.DataType = {
    dt match {
      case milvus.proto.backup.Backup.DataType.Bool => org.apache.spark.sql.types.BooleanType
      case milvus.proto.backup.Backup.DataType.Int8 => org.apache.spark.sql.types.ByteType
      case milvus.proto.backup.Backup.DataType.Int16 => org.apache.spark.sql.types.ShortType
      case milvus.proto.backup.Backup.DataType.Int32 => org.apache.spark.sql.types.IntegerType
      case milvus.proto.backup.Backup.DataType.Int64 => org.apache.spark.sql.types.LongType
      case milvus.proto.backup.Backup.DataType.Float => org.apache.spark.sql.types.FloatType
      case milvus.proto.backup.Backup.DataType.Double => org.apache.spark.sql.types.DoubleType
      case milvus.proto.backup.Backup.DataType.String | milvus.proto.backup.Backup.DataType.VarChar => org.apache.spark.sql.types.StringType
      case milvus.proto.backup.Backup.DataType.BinaryVector => org.apache.spark.sql.types.BinaryType
      case milvus.proto.backup.Backup.DataType.FloatVector => org.apache.spark.sql.types.ArrayType(org.apache.spark.sql.types.FloatType)
      case _ => throw new IllegalArgumentException(s"Unsupported data type: $dt")
    }
  }
}