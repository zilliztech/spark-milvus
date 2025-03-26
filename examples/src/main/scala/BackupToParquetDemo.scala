import com.aliyun.oss.{OSS, OSSClientBuilder}
import com.aliyun.oss.common.auth.CredentialsProviderFactory
import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import com.zilliztech.spark.l0data.DeltaLogUtils
import io.milvus.grpc.DataType
import milvus.proto.backup.{Backup, BackupUtil}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{json_tuple, monotonically_increasing_id, udf}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.slf4j.LoggerFactory
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import zilliztech.spark.milvus.MilvusOptions._
import zilliztech.spark.milvus.binlog.MilvusBinlogUtil

import java.util
import java.util.concurrent.{Executors, Future, TimeUnit}
import scala.collection.convert.ImplicitConversions.`list asScalaBuffer`
import scala.collection.mutable
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future => ScalaFuture}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

object BackupToParquetDemo {
  private val log = LoggerFactory.getLogger(getClass)

  private val parseVectorFunc = udf(MilvusBinlogUtil.littleEndianBinaryToFloatArray(_: Array[Byte]): Array[Float])

  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()
    log.info("Starting Milvus Backup to Parquet conversion")
    
    val sparkConf = new SparkConf()
      .setMaster("local")

    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

//    val defaultConfigs = Map(
//      "storage" -> "local",
//      "bucket" -> "",
//      "backup_path" -> "/Users/zilliz/Downloads/newBackup/",
//      "minio_endpoint" -> "http://localhost:9000",
//      "region" -> "cn-hangzhou",
//      "ak" -> "minioadmin",
//      "sk" -> "minioadmin",
//      "backup_collection" -> "test_col_lxelwhu",
//      "backup_database" -> "test_db",
//      "parallelism" -> "4",          // Number of parallel segment conversions
//      "output_directory" -> "/tmp",  // Output directory for Parquet files
//      "coalesce_partitions" -> "1",  // Number of partitions for output Parquet files
//    )
val defaultConfigs = Map(
  "storage" -> "oss",
  "bucket" -> "backup-zilliz",
  "backup_path" -> "newBackup",
  "minio_endpoint" -> "https://oss-cn-hangzhou.aliyuncs.com",
  "region" -> "cn-hangzhou",
  "ak" -> "",
  "sk" -> "",
  "backup_collection" -> "test_col_lxelwhu",
  "backup_database" -> "test_db",
  "parallelism" -> "4",          // Number of parallel segment conversions
  "output_directory" -> "/tmp",  // Output directory for Parquet files
  "coalesce_partitions" -> "1",  // Number of partitions for output Parquet files
)

    val mutableMap = mutable.Map.empty[String, String]
    lazy val userConf = ConfigFactory.load()
    userConf.entrySet().forEach(e => {
      if (e.getKey.startsWith("conf.")) {
        mutableMap(e.getKey.split("conf.")(1)) = userConf.getString(e.getKey)
      }
    })

    log.info("===================== Configurations =====================", defaultConfigs)
    val mergedConfigs = defaultConfigs ++ mutableMap
    mergedConfigs.foreach(x => {log.info(s"${x._1} = ${x._2}")})

    val storage = mergedConfigs("storage")
    val backupPath = mergedConfigs("backup_path")
    val bucketName = mergedConfigs("bucket")
    val collectionName = mergedConfigs("backup_collection")
    val database = mergedConfigs("backup_database")
    val parallelism = mergedConfigs("parallelism").toInt
    val outputDirectory = mergedConfigs("output_directory")
    val coalescePartitions = mergedConfigs("coalesce_partitions").toInt

    var ossClient: OSS = null
    var s3Client: S3Client = null
    val (backupInfo, fs) = if (storage.equals("local")) {
      (BackupUtil.GetBackupInfoFromLocal(backupPath), "file://")
    } else if (storage.equals("s3")){
      val ak = mergedConfigs("ak")
      val sk = mergedConfigs("sk")
      val s3Client = S3Client.builder().region(Region.US_WEST_2).credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(ak, sk))).build()
      (BackupUtil.GetBackupInfoFromS3(s3Client, bucketName, backupPath+"/"), "s3a://")
    }else if(storage.equals("oss")){
      val ak = mergedConfigs("ak")
      val sk = mergedConfigs("sk")
      val region = mergedConfigs("region")
      val minioEndPoint = mergedConfigs("minio_endpoint")
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

      (BackupUtil.GetBackupInfoFromOSS(ossClient, bucketName, backupPath+"/"), "oss://")
    } else { // minio
      val ak = mergedConfigs("ak")
      val sk = mergedConfigs("sk")
      val minioEndPoint = mergedConfigs("minio_endpoint")
      s3Client = S3Client.builder()
        .region(Region.AWS_GLOBAL)
        .endpointOverride(java.net.URI.create(minioEndPoint))
        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(ak, sk)))
        .build()

      spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", minioEndPoint)
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access", "true")
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.connection.ssl.enabled", "false")
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", ak)
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", sk)

      (BackupUtil.GetBackupInfoFromS3(s3Client, bucketName, backupPath+"/"), "s3a://")
    }

    val collectionBackups = backupInfo.getCollectionBackupsList
    val segmentRowId = "segment_rowid"

    collectionBackups.filter(c => c.getCollectionName == collectionName && c.getDbName == database).map(coll => {
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
      
      val nonL0Segment = segments.filter(s => !s.getIsL0())
      log.info(s"Non-L0 segment count: ${nonL0Segment.length}, processing with parallelism: $parallelism")
      
      // Process segments in parallel
      val procStartTime = System.currentTimeMillis()
      val totalRows = processSegmentsInParallel(nonL0Segment, coll.getCollectionName, fs, bucketName, backupPath, spark, vecFeilds,
        segmentRowId, partitionID2DF, globalL0DF, pkField, fieldDict, parallelism, outputDirectory, coalescePartitions)
      
      val procEndTime = System.currentTimeMillis()
      val collEndTime = System.currentTimeMillis()
      
      log.info(s"Collection processing statistics:")
      log.info(s"- Collection: ${coll.getCollectionName}")
      log.info(s"- Total segments processed: ${nonL0Segment.length}")
      log.info(s"- Total rows converted: $totalRows")
      log.info(s"- Segment processing time: ${(procEndTime - procStartTime)/1000} seconds")
      log.info(s"- Overall collection processing time: ${(collEndTime - collStartTime)/1000} seconds")
    })
    
    val endTime = System.currentTimeMillis()
    log.info(s"Conversion completed in ${(endTime - startTime)/1000} seconds")
  }
  
  private def processSegmentsInParallel(
    segments: mutable.Buffer[Backup.SegmentBackupInfo],
    collectionName: String,
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
    outputDirectory: String,
    coalescePartitions: Int
  ): Long = {
    // Create a thread pool with the specified parallelism
    implicit val executionContext: ExecutionContextExecutorService = 
      ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(parallelism))
    
    var totalInsertedRows: Long = 0
    
    try {
      val totalSegments = segments.length
      var processedSegments = 0
      
      // Track successful, failed, and processing times
      var successfulSegments = 0
      var failedSegments = 0

      // Process segments in batches to control memory usage
      segments.grouped(parallelism).foreach { segmentBatch =>
        log.info(s"Processing batch of ${segmentBatch.size} segments (${processedSegments + 1}-${processedSegments + segmentBatch.size} of $totalSegments)")
        
        // Create futures for each segment in this batch
        val futures = segmentBatch.map { segment =>
          ScalaFuture {
            val segmentStartTime = System.currentTimeMillis()
            try {
              log.info(s"Starting processing of segment ${segment.getSegmentId}")
              // Load binlog data
              val insertDF = loadFieldBinlogs(segment, fs, bucketName, backupPath, spark, vecFields, segmentRowId)

              log.info(s"total row: ${insertDF.count()} before compacting")
              // Compact, merge delete data
              var compactedDF = applyDeltaLogs(insertDF, segment, fs, bucketName, backupPath, spark, pkField)
              compactedDF = applyPartitionL0Filter(compactedDF, segment, partitionID2DF, pkField)
              compactedDF = applyGlobalL0Filter(compactedDF, globalL0DF, pkField)

              val resultDf = compactedDF.drop("0", "ts").drop(segmentRowId)
              val renamedResultDF = fieldDict.foldLeft(resultDf) { case (df, (oldName, newName)) =>
                df.withColumnRenamed(oldName.toString, newName)
              }

              log.info(s"total row: ${renamedResultDF.count()} after compaction")

              // Save to Parquet file
              val rowCount = saveToParquet(
                renamedResultDF, 
                collectionName, 
                segment.getSegmentId, 
                outputDirectory, 
                coalescePartitions
              )
              
              val segmentEndTime = System.currentTimeMillis()
              val processingTime = segmentEndTime - segmentStartTime
              
              (segment.getSegmentId, rowCount, processingTime, true) // Success
            } catch {
              case e: Exception => 
                val segmentEndTime = System.currentTimeMillis()
                val processingTime = segmentEndTime - segmentStartTime
                log.error(s"Error processing segment ${segment.getSegmentId}: ${e.getMessage}", e)
                (segment.getSegmentId, 0L, processingTime, false) // Failure
            }
          }
        }
        
        // Wait for all futures in this batch to complete
        val results = scala.concurrent.Await.result(
          ScalaFuture.sequence(futures), 
          Duration.Inf
        )
        
        // Update progress
        processedSegments += segmentBatch.size
        val batchRows = results.map(_._2).sum
        totalInsertedRows += batchRows
        
        // Update statistics
        successfulSegments += results.count(_._4)
        failedSegments += results.count(!_._4)

        log.info(s"Batch complete: ${processedSegments}/${totalSegments} segments processed")
        log.info(s"Success: $successfulSegments, Failed: $failedSegments")
      }
      
      log.info(s"Completed processing all ${segments.length} segments")
      log.info(s"Success: $successfulSegments, Failed: $failedSegments")
      log.info(s"Total rows inserted: ${totalInsertedRows}")
      
    } finally {
      // Shutdown the executor service
      executionContext.shutdown()
      executionContext.awaitTermination(1, TimeUnit.HOURS)
    }
    
    totalInsertedRows
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
    compactedDF.show(10)
    val partitionL0DF = partitionID2DF.get(segment.getPartitionId)
    partitionL0DF.show(5)
    if (partitionL0DF != null && !partitionL0DF.isEmpty) {
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

  private def saveToParquet(
    df: DataFrame, 
    collectionName: String, 
    segmentId: Long, 
    outputDirectory: String,
    coalescePartitions: Int
  ): Long = {
    val outputPath = s"${outputDirectory}/${collectionName}_${segmentId}.parquet"
    log.info(s"Saving as Parquet: $outputPath with $coalescePartitions partitions")

    // Count before saving to return the number of rows
    val count = df.count()
    
    df.coalesce(coalescePartitions)
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save(outputPath)

    log.info(s"Successfully saved as Parquet: $outputPath with $count rows")
    count
  }
}


