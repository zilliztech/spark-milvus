import com.aliyun.oss.common.auth.CredentialsProviderFactory
import com.aliyun.oss.{OSS, OSSClientBuilder}
import com.typesafe.config.ConfigFactory
import io.milvus.grpc.DataType
import milvus.proto.backup.{Backup, BackupUtil}
import org.apache.spark.sql.functions.{json_tuple, monotonically_increasing_id, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import zilliztech.spark.milvus.binlog.MilvusBinlogUtil

import java.util
import java.util.concurrent.{Executors, TimeUnit}
import scala.collection.convert.ImplicitConversions.`list asScalaBuffer`
import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future => ScalaFuture}

object BackupFileUtils {
  private val log = LoggerFactory.getLogger(getClass)

  private val parseVectorFunc = udf(MilvusBinlogUtil.littleEndianBinaryToFloatArray(_: Array[Byte]): Array[Float])

  var ossClient: OSS = _
  var s3Client: S3Client = _

  /**
   * Processes backups and returns a single aggregated DataFrame.
   */
  def processBackup(spark: SparkSession, defaultConfigs: Map[String, String]): DataFrame = {
    val mutableMap = mutable.Map.empty[String, String]
    lazy val userConf = ConfigFactory.load()
    userConf.entrySet().forEach(e => {
      if (e.getKey.startsWith("conf.")) {
        mutableMap(e.getKey.split("conf.")(1)) = userConf.getString(e.getKey)
      }
    })

    log.info("===================== Configurations =====================\n{}", defaultConfigs)
    val mergedConfigs = defaultConfigs ++ mutableMap
    mergedConfigs.foreach(x => {log.info(s"${x._1} = ${x._2}")})

    val backupPath = mergedConfigs("backup_path").stripSuffix("/")

    val bucketName = mergedConfigs("bucket")
    val collectionName = mergedConfigs("backup_collection")
    val database = mergedConfigs("backup_database")
    val parallelism = mergedConfigs("parallelism").toInt

    val (backupInfo, fs) = initializeStorageClient(spark, mergedConfigs)

    val collectionBackups = backupInfo.getCollectionBackupsList
    val segmentRowId = "segment_rowid"
    val startTime = System.currentTimeMillis()
    val finalDF = collectionBackups.find(c => c.getCollectionName == collectionName && c.getDbName == database).map(coll => {
      val collStartTime = System.currentTimeMillis()
      log.info(s"Processing collection: ${coll.getCollectionName} in database: ${coll.getDbName}")

      val schema = coll.getSchema
      schema.getFieldsList.foreach(f => println(s"==Field: ${f.getName}, Type: ${f.getDataType}"))

      val fieldDict = schema.getFieldsList.map(field => (field.getFieldID, field.getName)).toMap
      val pkField = schema.getFieldsList.filter(field => field.getIsPrimaryKey).toArray.apply(0)
      val segments = coll.getPartitionBackupsList.flatMap(p => p.getSegmentBackupsList)
      val vecFeilds = schema.getFieldsList.filter(field =>
        Seq(DataType.BinaryVector.getNumber, DataType.FloatVector.getNumber).contains(field.getDataType.getNumber)
      ).map(field => field.getFieldID)
      log.info(s"total segment number:${segments.length}")

      val l0StartTime = System.currentTimeMillis()
      log.info("Collecting L0 data for filtering...")

      var partitionID2DF: java.util.HashMap[Long, DataFrame] = null
      var globalL0DF: DataFrame = null

      if(fs.equals("file://")){
        // collect l0 data paths of partition level
        partitionID2DF = L0SegmentUtils.getPartitionID2DF(segments, bucketName, backupPath, spark, pkField.getDataType)
        // collect l0 data paths of collection level
        globalL0DF = L0SegmentUtils.getGlobalL0DF(coll, bucketName, backupPath, spark, pkField.getDataType)
      }else if(fs.equals("oss://")){
        partitionID2DF = L0SegmentUtils.getOSSPartitionID2DF(segments, bucketName, backupPath, spark, pkField.getDataType, ossClient)
        globalL0DF = L0SegmentUtils.getOSSGlobalL0DF(coll, bucketName, backupPath, spark, pkField.getDataType, ossClient)
      }

      val l0EndTime = System.currentTimeMillis()
      log.info(s"L0 data collection completed in ${(l0EndTime - l0StartTime)/1000} seconds")

      val nonL0Segment = segments.filter(s => !s.getIsL0)
      log.info(s"Non-L0 segment count: ${nonL0Segment.length}, processing with parallelism: $parallelism")

      // Process segments in parallel
      val procStartTime = System.currentTimeMillis()
      val finalDF = processSegmentsInParallel(nonL0Segment, fs, bucketName, backupPath, spark, vecFeilds,
        segmentRowId, partitionID2DF, globalL0DF, pkField, fieldDict, parallelism)

      val procEndTime = System.currentTimeMillis()
      val collEndTime = System.currentTimeMillis()

      log.info(s"Collection processing statistics:")
      log.info(s"- Collection: ${coll.getCollectionName}")
      log.info(s"- Total segments processed: ${nonL0Segment.length}")
      log.info(s"- Total rows converted: ${finalDF.count()}")
      log.info(s"- Segment processing time: ${(procEndTime - procStartTime)/1000} seconds")
      log.info(s"- Overall collection processing time: ${(collEndTime - collStartTime)/1000} seconds")
      finalDF
    }).getOrElse(spark.emptyDataFrame)

    val endTime = System.currentTimeMillis()
    log.info(s"Conversion completed in ${(endTime - startTime)/1000} seconds")
    finalDF
  }

  private def processSegmentsInParallel(
                                         segments: mutable.Buffer[Backup.SegmentBackupInfo],
                                         fs: String,
                                         bucketName: String,
                                         backupPath: String,
                                         spark: SparkSession,
                                         vecFields: Seq[Long],
                                         segmentRowId: String,
                                         partitionID2DF: util.HashMap[Long, DataFrame],
                                         globalL0DF: DataFrame,
                                         pkField: Backup.FieldSchema,
                                         fieldDict: Map[Long, String],
                                         parallelism: Int,
                                       ): DataFrame = {

    implicit val executionContext: ExecutionContextExecutorService =
      ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(parallelism))

    val resultDFs = mutable.ListBuffer[DataFrame]()

    try {
      segments.grouped(parallelism).foreach { segmentBatch =>
        log.info(s"Processing batch of ${segmentBatch.size} segments")

        val futures = segmentBatch.map { segment =>
          ScalaFuture {
            val segmentStartTime = System.currentTimeMillis()
            try {
              log.info(s"Starting processing of segment ${segment.getSegmentId}")
              val insertDF = loadFieldBinlogs(segment, fs, bucketName, backupPath, spark, vecFields, segmentRowId)
              var compactedDF = applyDeltaLogs(insertDF, segment, fs, bucketName, backupPath, spark, pkField)
              compactedDF = applyPartitionL0Filter(compactedDF, segment, partitionID2DF, pkField)
              compactedDF = applyGlobalL0Filter(compactedDF, globalL0DF, pkField)

              val resultDf = compactedDF.drop("0", "ts").drop(segmentRowId)
              val renamedResultDF = fieldDict.foldLeft(resultDf) { case (df, (oldName, newName)) =>
                df.withColumnRenamed(oldName.toString, newName)
              }

              log.info(s"Segment ${segment.getSegmentId} completed with ${renamedResultDF.count()} rows")

              Some(renamedResultDF)
            } catch {
              case e: Exception =>
                log.error(s"Error processing segment ${segment.getSegmentId}: ${e.getMessage}", e)
                None
            }
          }
        }

        val results = scala.concurrent.Await.result(ScalaFuture.sequence(futures), Duration.Inf)
        resultDFs ++= results.flatten
      }
    } finally {
      executionContext.shutdown()
      executionContext.awaitTermination(1, TimeUnit.HOURS)
    }

    // Merge all result DataFrames
    if (resultDFs.nonEmpty) resultDFs.reduce(_ unionByName _) else spark.emptyDataFrame
  }


  private def loadFieldBinlogs(
                                segment: Backup.SegmentBackupInfo,
                                fs: String,
                                bucketName: String,
                                backupPath: String,
                                spark: SparkSession,
                                vecFields: Seq[Long],
                                segmentRowId: String
                              ): DataFrame = {

    val fieldBinlogs = segment.getBinlogsList
    var idx = 1

    val dfs = fieldBinlogs.map { field =>
      val insertPath = "%s%s/%s/binlogs/insert_log/%d/%d/%d/%d/%d".format(
        fs, bucketName, backupPath,
        segment.getCollectionId, segment.getPartitionId,
        segment.getSegmentId, segment.getSegmentId, field.getFieldID
      )

      log.info(s"Start insert segment ${segment.getSegmentId} field ${field.getFieldID} from ${insertPath}")

      val fieldName = field.getFieldID.toString
      val fieldColumn = spark.read.format("milvusbinlog").load(insertPath)
        .withColumnRenamed("val", fieldName)
        .withColumnRenamed("1", "ts")
        .withColumn(segmentRowId, monotonically_increasing_id())

      log.info(s"Finish read segment ${segment.getSegmentId} field ${field.getFieldID} progress: ${idx}/${fieldBinlogs.length} count: ${fieldColumn.count()}")

      idx += 1

      if (vecFields.contains(field.getFieldID)) {
        fieldColumn.withColumn(fieldName, parseVectorFunc(fieldColumn(fieldName)))
      } else {
        fieldColumn
      }
    }

    // Join all DataFrames on `segmentRowId`
    dfs.reduce { (leftDF, rightDF) =>
      leftDF.join(rightDF, segmentRowId)
    }
  }

  private def applyDeltaLogs(insertDF: DataFrame,
                             segment: Backup.SegmentBackupInfo,
                             fs: String,
                             bucketName: String,
                             backupPath: String,
                             spark: SparkSession,
                             pkField: Backup.FieldSchema): DataFrame = {
    if (!segment.getDeltalogsList.isEmpty && segment.getDeltalogsList.get(0).getBinlogsList.length > 0) {
      val deltaPath = "%s%s/%s/binlogs/delta_log/%d/%d/%d/%d".format(fs, bucketName, backupPath, segment.getCollectionId, segment.getPartitionId, segment.getSegmentId, segment.getSegmentId)
      val delta = spark.read.format("milvusbinlog").load(deltaPath)
      val deltaDF = delta.select(json_tuple(delta.col("val"), "pk", "ts"))

      // Remove rows that already exist in deltaDF
      insertDF.join(
        deltaDF,
        insertDF(pkField.getFieldID.toString) === deltaDF("c0"),
        "left_anti"
      )
    } else {
      insertDF
    }
  }

  private def applyPartitionL0Filter(compactedDF: DataFrame,
                                     segment: Backup.SegmentBackupInfo,
                                     partitionID2DF: util.HashMap[Long, DataFrame],
                                     pkField: Backup.FieldSchema): DataFrame = {
    val partitionL0DF = partitionID2DF.get(segment.getPartitionId)
    if (partitionL0DF != null && !partitionL0DF.isEmpty) {
      compactedDF.show(10)
      partitionL0DF.show(5)
      compactedDF.join(
        partitionL0DF,
        compactedDF(pkField.getFieldID.toString) === partitionL0DF("pk") &&
          compactedDF("ts") <= partitionL0DF("ts"),
        "left_anti"
      )
    } else {
      compactedDF
    }
  }

  private def applyGlobalL0Filter(compactedDF: DataFrame,
                                  globalL0DF: DataFrame,
                                  pkField: Backup.FieldSchema): DataFrame = {
    if (globalL0DF != null && !globalL0DF.isEmpty) {
      compactedDF.join(
        globalL0DF,
        compactedDF(pkField.getFieldID.toString) === globalL0DF("pk") &&
          compactedDF("ts") <= globalL0DF("ts"),
        "left_anti"
      )
    } else {
      compactedDF
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
        (BackupUtil.GetBackupInfoFromLocal(backupPath+'/'), "file://")
      case "s3" =>
        s3Client = S3Client.builder()
          .region(Region.of(region))
          .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(ak, sk)))
          .build()

        spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", minioEndPoint)
        spark.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access", "true")
        spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        spark.sparkContext.hadoopConfiguration.set("fs.s3a.connection.ssl.enabled", "false")
        spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", ak)
        spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", sk)

        (BackupUtil.GetBackupInfoFromS3(s3Client, bucketName, backupPath+'/'), "s3a://")
      case "oss" =>
        val credentialsProvider = CredentialsProviderFactory.newDefaultCredentialProvider(ak, sk)
        ossClient = OSSClientBuilder.create()
          .endpoint(minioEndPoint)
          .credentialsProvider(credentialsProvider)
          .region(region)
          .build()

        spark.sparkContext.hadoopConfiguration.set("fs.oss.endpoint", minioEndPoint)
        spark.sparkContext.hadoopConfiguration.set("fs.oss.impl", "org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem")
        spark.sparkContext.hadoopConfiguration.set("fs.oss.accessKeyId", ak)
        spark.sparkContext.hadoopConfiguration.set("fs.oss.accessKeySecret", sk)

        (BackupUtil.GetBackupInfoFromOSS(ossClient, bucketName, backupPath+'/'), "oss://")
      case _ =>
        throw new IllegalArgumentException("Unsupported storage type: " + storage)
    }
  }

}