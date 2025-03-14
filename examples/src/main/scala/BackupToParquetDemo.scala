import io.milvus.grpc.DataType
import milvus.proto.backup.BackupUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{json_tuple, monotonically_increasing_id, udf}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.LoggerFactory
import zilliztech.spark.milvus.MilvusOptions._
import zilliztech.spark.milvus.binlog.MilvusBinlogUtil

import scala.collection.convert.ImplicitConversions.`list asScalaBuffer`
import scala.collection.mutable

object BackupToParquetDemo {
  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val defaultConfigs = Map(
      "storage" -> "local",
      "bucket" -> "",
      "backup_path" -> "/Users/zilliz/Downloads/newBackup/",
      "minio_endpoint" -> "http://localhost:9000",
      "ak" -> "minioadmin",
      "sk" -> "minioadmin",
      "backup_collection" -> "hello_milvus",
      "backup_database" -> "default",
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

    val (backupInfo, fs) = if (storage.equals("local")) {
      (BackupUtil.GetBackupInfoFromLocal(backupPath), "file://")
    } else if (storage.equals("s3")){
      val ak = mergedConfigs("ak")
      val sk = mergedConfigs("sk")
      val s3Client = S3Client.builder().region(Region.US_WEST_2).credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(ak, sk))).build()
      (BackupUtil.GetBackupInfoFromS3(s3Client, bucketName, backupPath), "s3a://")
    } else { // minio
      val ak = mergedConfigs("ak")
      val sk = mergedConfigs("sk")
      val minioEndPoint = mergedConfigs("minio_endpoint")
      val s3Client = S3Client.builder()
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

      (BackupUtil.GetBackupInfoFromS3(s3Client, bucketName, backupPath), "s3a://")
    }

    val collectionBackups = backupInfo.getCollectionBackupsList
    val segmentRowId = "segment_rowid"


    val parseVectorFunc = udf(MilvusBinlogUtil.littleEndianBinaryToFloatArray(_: Array[Byte]): Array[Float])

    collectionBackups.filter(c => c.getCollectionName == collectionName && c.getDbName == database).map(coll => {
      val schema = coll.getSchema
      schema.getFieldsList.foreach(f => println(s"===========Field: ${f.getName}, Type: ${f.getDataType}"))

      val fieldDict = schema.getFieldsList.map(field => (field.getFieldID, field.getName)).toMap
      val pkField = schema.getFieldsList.filter(field => field.getIsPrimaryKey).toArray.apply(0)
      val segments = coll.getPartitionBackupsList.flatMap(p => p.getSegmentBackupsList)
      val vecFeilds = schema.getFieldsList.filter(field =>
        Seq(DataType.BinaryVector.getNumber, DataType.FloatVector.getNumber).contains(field.getDataType.getNumber)
      ).map(field => field.getFieldID)
      log.info(s"total segment number:${segments.length}")

      // collect l0 data
      var l0DF = spark.emptyDataFrame
      segments.zipWithIndex.map(x => {
        val segment = x._1
        // if partitionId is -1, the segment is l0 segment
         if (segment.getPartitionId == -1) {
           val deltaPath = "%s%s/%s/binlogs/delta_log/%d/%d/%d/%d".format(fs, bucketName, backupPath, segment.getCollectionId, segment.getPartitionId, segment.getSegmentId, segment.getSegmentId)
           val delta = spark.read.format("milvusbinlog").load(deltaPath)
           val deltaDF = delta.select(json_tuple(delta.col("val"), "pk", "ts"))
           l0DF = l0DF.union(deltaDF)
         }
      })
      log.info(s"finish read l0 segment progress, count: ${l0DF.count()}")

      var insertedRows: Long = 0
      segments.zipWithIndex.map(x => {
        val segment = x._1
        val index = x._2

        // skip l0 segment
        if (segment.getPartitionId == -1) {
          return
        }

        val fieldBinlogs = segment.getBinlogsList
        val dfs = fieldBinlogs.map(field => {
          val insertPath = "%s%s/%s/binlogs/insert_log/%d/%d/%d/%d/%d".format(fs, bucketName, backupPath, segment.getCollectionId, segment.getPartitionId, segment.getSegmentId, segment.getSegmentId, field.getFieldID)
          log.info(s"start insert segment ${segment.getSegmentId} field ${field.getFieldID} from ${insertPath}")

          val fieldColumn = spark.read.format("milvusbinlog").load(insertPath)
            .withColumn(segmentRowId, monotonically_increasing_id())
          log.info(s"finish read segment ${segment.getSegmentId} field ${field.getFieldID} progress: ${index+1}/${segments.length} count: ${fieldColumn.count()}")

          if (vecFeilds.contains(field.getFieldID)) {
            fieldColumn.withColumn(field.getFieldID.toString, parseVectorFunc(fieldColumn(field.getFieldID.toString)))
          } else {
            fieldColumn
          }
        })

        val insertDF = dfs.reduce { (leftDF, rightDF) =>
          leftDF.join(rightDF, segmentRowId)
        }

        val segmentDF = if (!segment.getDeltalogsList.isEmpty && segment.getDeltalogsList.get(0).getBinlogsList.length > 0) {
          val deltaPath = "%s%s/%s/binlogs/delta_log/%d/%d/%d/%d".format(fs, bucketName, backupPath, segment.getCollectionId, segment.getPartitionId, segment.getSegmentId, segment.getSegmentId)
          val delta = spark.read.format("milvusbinlog").load(deltaPath)
          val deltaDF = delta.select(json_tuple(delta.col("val"), "pk", "ts"))


          // remove rows that are already in deltaDF from insertDF
          var compactedDF = insertDF.join(
            deltaDF,
            insertDF(pkField.getFieldID.toString) === deltaDF("c0"),
            "left_anti")

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

        // Save the DataFrame to Parquet
        val outputPath = s"/tmp/${coll.getCollectionName}_${segment.getSegmentId}.parquet"
        renamedResultDF.write.format("parquet").mode(SaveMode.Overwrite).save(outputPath)

        insertedRows += renamedResultDF.count()
        log.info(s"finish insert segment ${segment.getSegmentId} progress: ${index+1}/${segments.length} inserted rows: ${insertedRows}")
      })
    })
  }
}


