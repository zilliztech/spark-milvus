import com.aliyun.oss.OSSClientBuilder
import com.aliyun.oss.common.auth.CredentialsProviderFactory
import com.typesafe.config.ConfigFactory
import com.zilliztech.spark.l0data.DeltaLogUtils
import io.milvus.grpc.DataType
import milvus.proto.backup.{Backup, BackupUtil}
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{json_tuple, monotonically_increasing_id, udf}
import org.slf4j.LoggerFactory
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import zilliztech.spark.milvus.binlog.MilvusBinlogUtil

import java.util
import scala.collection.convert.ImplicitConversions.`list asScalaBuffer`
import scala.collection.mutable

object BackupFileUtils {
  private val log = LoggerFactory.getLogger(getClass)

  /**
   * Processes backups and returns a single aggregated DataFrame.
   */
  def processBackup(spark: SparkSession, defaultConfigs: Map[String, String]): sql.DataFrame = {
    val mutableMap = mutable.Map.empty[String, String]
    lazy val userConf = ConfigFactory.load()
    userConf.entrySet().forEach(e => {
      if (e.getKey.startsWith("conf.")) {
        mutableMap(e.getKey.split("conf.")(1)) = userConf.getString(e.getKey)
      }
    })
    log.info("===================== Configurations =====================")
    val mergedConfigs = defaultConfigs ++ mutableMap
    mergedConfigs.foreach(x => log.info(s"${x._1} = ${x._2}"))

    val collectionName = mergedConfigs("backup_collection")
    val database = mergedConfigs("backup_database")
    val backupPath = mergedConfigs("backup_path")
    val bucketName = mergedConfigs("bucket")

    // Initialize storage client and filesystem prefix
    val (backupInfo, fs) = initializeStorageClient(spark, mergedConfigs)

    val collectionBackups = backupInfo.getCollectionBackupsList
    val segmentRowId = "segment_rowid"

    val parseVectorFunc = udf(MilvusBinlogUtil.littleEndianBinaryToFloatArray(_: Array[Byte]): Array[Float])

    // Process each collection and aggregate results
    val allRenamedResultDFs = collectionBackups.filter(c => c.getCollectionName == collectionName && c.getDbName == database).flatMap(coll => {
      val schema = coll.getSchema
      schema.getFieldsList.foreach(f => println(s"==Field: ${f.getName}, Type: ${f.getDataType}"))

      val fieldDict = schema.getFieldsList.map(field => (field.getFieldID, field.getName)).toMap
      val pkField = schema.getFieldsList.filter(field => field.getIsPrimaryKey).head
      val segments = coll.getPartitionBackupsList.flatMap(p => p.getSegmentBackupsList)
      val vecFeilds = schema.getFieldsList.filter(field =>
        Seq(DataType.BinaryVector.getNumber, DataType.FloatVector.getNumber).contains(field.getDataType.getNumber)
      ).map(field => field.getFieldID)
      log.info(s"total segment number: ${segments.length}")

      // Collect L0 data
      val deltaPaths = new util.ArrayList[String]()
      segments.filter(_.getIsL0()).foreach(segment => {
        val deltaDir = "%s/%s/binlogs/delta_log/%d/%d/%d/%d/*".format(bucketName, backupPath, segment.getCollectionId, segment.getPartitionId, segment.getSegmentId, segment.getSegmentId)
        deltaPaths.addAll(DeltaLogUtils.expandGlobPattern(deltaDir))
      })

      var l0DF = spark.emptyDataFrame
      if (deltaPaths.size() > 0) {
        log.info(s"delta log file count: ${deltaPaths.size()}")
        l0DF = DeltaLogUtils.createDataFrame(deltaPaths, spark)
        l0DF.show(10)
      }
      log.info(s"finish read L0 segment, count: ${l0DF.count()}")

      // Process non-L0 segments
      val nonL0Segments = segments.filterNot(_.getIsL0())
      var insertedRows: Long = 0
      var index = 0
      nonL0Segments.map(segment => {
        val fieldBinlogs = segment.getBinlogsList
        val dfs = fieldBinlogs.map(field => {
          val insertPath = "%s%s/%s/binlogs/insert_log/%d/%d/%d/%d/%d".format(fs, bucketName, backupPath, segment.getCollectionId, segment.getPartitionId, segment.getSegmentId, segment.getSegmentId, field.getFieldID)
          log.info(s"start insert segment ${segment.getSegmentId} field ${field.getFieldID} from ${insertPath}")

          val fieldName = field.getFieldID.toString
          val fieldColumn = spark.read.format("milvusbinlog").load(insertPath)
            .withColumnRenamed("val", fieldName)
            .withColumn(segmentRowId, monotonically_increasing_id())

          if (vecFeilds.contains(field.getFieldID)) {
            fieldColumn.withColumn(fieldName, parseVectorFunc(fieldColumn(fieldName)))
          } else {
            fieldColumn
          }
        })

        val insertDF = dfs.reduce { (leftDF, rightDF) =>
          leftDF.join(rightDF, segmentRowId)
        }

        val segmentDF = if (!segment.getDeltalogsList.isEmpty && segment.getDeltalogsList.get(0).getBinlogsList.nonEmpty) {
          val deltaPath = "%s%s/%s/binlogs/delta_log/%d/%d/%d/%d".format(fs, bucketName, backupPath, segment.getCollectionId, segment.getPartitionId, segment.getSegmentId, segment.getSegmentId)
          val delta = spark.read.format("milvusbinlog").load(deltaPath)
          val deltaDF = delta.select(json_tuple(delta.col("val"), "pk", "ts"))

          // Remove rows that are already in deltaDF from insertDF
          var compactedDF = insertDF.join(
            deltaDF,
            insertDF(pkField.getFieldID.toString) === deltaDF("c0"),
            "left_anti"
          )

          // Remove rows that are in l0DF and have ts <= l0DF.ts
          if (l0DF.count() > 0) {
            compactedDF = compactedDF.join(
              l0DF,
              compactedDF(pkField.getFieldID.toString) === l0DF("c0") && compactedDF("ts") <= l0DF("ts"),
              "left_anti"
            )
          }

          compactedDF
        } else {
          insertDF
        }

        val resultDf = segmentDF.drop("0", "1").drop(segmentRowId)
        val renamedResultDF = fieldDict.foldLeft(resultDf) { case (df, (oldName, newName)) =>
          df.withColumnRenamed(oldName.toString, newName)
        }
        insertedRows += renamedResultDF.count()
        log.info(s"finish segment ${segment.getSegmentId} progress: ${index+1}/${nonL0Segments.length} inserted rows: ${insertedRows}")
        renamedResultDF
      })
    })

    // Aggregate all renamedResultDFs into a single DataFrame
    if (allRenamedResultDFs.isEmpty) {
      log.warn("No segments processed. Returning an empty DataFrame.")
      spark.emptyDataFrame
    } else {
      allRenamedResultDFs.reduce(_ union _)
    }
  }

  private def initializeStorageClient(
                                       spark: SparkSession,
                                       mergedConfigs: Map[String, String]
                                     ): (Backup.BackupInfo, String) = {

    val storage = mergedConfigs("storage")
    val backupPath = mergedConfigs("backup_path")
    val bucketName = mergedConfigs("bucket")

    val ak = mergedConfigs("ak")
    val sk = mergedConfigs("sk")
    val region = mergedConfigs("region")
    val minioEndPoint = mergedConfigs("minio_endpoint")
    storage match {
      case "local" =>
        (BackupUtil.GetBackupInfoFromLocal(backupPath), "file://")
      case "s3" =>
        val s3Client = S3Client.builder()
          .region(Region.of(region))
          .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(ak, sk)))
          .build()

        spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", minioEndPoint)
        spark.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access", "true")
        spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        spark.sparkContext.hadoopConfiguration.set("fs.s3a.connection.ssl.enabled", "false")
        spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", ak)
        spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", sk)

        (BackupUtil.GetBackupInfoFromS3(s3Client, bucketName, backupPath), "s3a://")
      case "oss" =>
        val credentialsProvider = CredentialsProviderFactory.newDefaultCredentialProvider(ak, sk)
        val ossClient = OSSClientBuilder.create()
          .endpoint(minioEndPoint)
          .credentialsProvider(credentialsProvider)
          .region(region)
          .build()

        spark.sparkContext.hadoopConfiguration.set("fs.oss.endpoint", minioEndPoint)
        spark.sparkContext.hadoopConfiguration.set("fs.oss.impl", "org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem")
        spark.sparkContext.hadoopConfiguration.set("fs.oss.accessKeyId", ak)
        spark.sparkContext.hadoopConfiguration.set("fs.oss.accessKeySecret", sk)

        (BackupUtil.GetBackupInfoFromOSS(ossClient, bucketName, backupPath), "oss://")
      case _ =>
        throw new IllegalArgumentException("Unsupported storage type: " + storage)
    }
  }

}