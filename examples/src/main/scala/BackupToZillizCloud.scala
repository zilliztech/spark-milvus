import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import io.milvus.grpc.DataType
import milvus.proto.backup.BackupUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{json_tuple, monotonically_increasing_id, udf}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.LoggerFactory
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import zilliztech.spark.milvus.MilvusOptions._
import zilliztech.spark.milvus.binlog.MilvusBinlogUtil

import scala.collection.convert.ImplicitConversions.`list asScalaBuffer`
import scala.collection.mutable

object BackupToZillizCloud {
  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val defaultConfigs = Map(
      "storage" -> "local",
      "bucket" -> "",
      "backup_path" -> "/backup/backupname",
      "minio_endpoint" -> "http://localhost:9000",
      "ak" -> "minioadmin",
      "sk" -> "minioadmin",
      "uri" -> "https://xxxxxxxx.ali-cn-hangzhou.vectordb.zilliz.com.cn:19530",
      "token" -> "db_admin:xxxxxx",
      "backup_collection" -> "hello_milvus_pk",
      "backup_database" -> "default",
      "recover_collection" -> "hello_milvus_pk",
      "recover_database" -> "default"
    )

    val mutableMap = mutable.Map.empty[String, String]
    lazy val userConf = ConfigFactory.load()
    userConf.entrySet().forEach(e => {
      if (e.getKey.startsWith("conf.")) {
        mutableMap(e.getKey.split("conf.")(1)) = userConf.getString(e.getKey)
      }
    })

    val mergedConfigs = defaultConfigs ++ mutableMap
    mergedConfigs.foreach(x => {log.info(s"${x._1} = ${x._2}")})

    val storage = mergedConfigs("storage")
    val backupPath = mergedConfigs("backup_path")
    val bucketName = mergedConfigs("bucket")
    val collectionName = mergedConfigs("backup_collection")
    val database = mergedConfigs("backup_database")
    val recoverDatabase = mergedConfigs("recover_database")
    val recoverCollectionName = mergedConfigs("recover_collection")

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
    val milvusRowId = "milvus_rowid"
    val milvusTs = "milvus_ts"
    val segmentRowId = "segment_rowid"

    val milvusOptions = Map(
      MILVUS_URI -> mergedConfigs("uri"),
      MILVUS_TOKEN -> mergedConfigs("token"),
      MILVUS_COLLECTION_NAME -> recoverCollectionName,
      MILVUS_DATABASE_NAME -> recoverDatabase,
      MILVUS_COLLECTION_PRIMARY_KEY -> "count",
      MILVUS_COLLECTION_VECTOR_FIELD -> "embeddings",
      MILVUS_COLLECTION_VECTOR_DIM -> "8",
    )

    val parseVectorFunc = udf(MilvusBinlogUtil.littleEndianBinaryToFloatArray(_: Array[Byte]): Array[Float])

    collectionBackups.filter(c => c.getCollectionName == collectionName && c.getDbName == database).map(coll => {
      val schema = coll.getSchema
      val fieldDict = schema.getFieldsList.map(field => (field.getFieldID, field.getName)).toMap
      val pkField = schema.getFieldsList.filter(field => field.getIsPrimaryKey).toArray.apply(0)
      val pkFieldName = pkField.getName
      val segments = coll.getPartitionBackupsList.flatMap(p => p.getSegmentBackupsList)
      val vecFeilds = schema.getFieldsList.filter(field =>
        Seq(DataType.BinaryVector.getNumber, DataType.FloatVector.getNumber).contains(field.getDataType.getNumber)
      ).map(field => field.getFieldID)
      log.info(s"total segment number:${segments.length}")

      var insertedRows: Long = 0
      segments.zipWithIndex.map(x => {
        val segment = x._1
        val index = x._2
        val fieldBinlogs = segment.getBinlogsList
        val dfs = fieldBinlogs.map(field => {
          val insertPath = "%s%s/%s/binlogs/insert_log/%d/%d/%d/%d/%d".format(fs, bucketName, backupPath, segment.getCollectionId, segment.getPartitionId, segment.getSegmentId, segment.getSegmentId, field.getFieldID)
          val fieldName = if (field.getFieldID == 0) {
            milvusRowId
          } else if (field.getFieldID == 1) {
            milvusTs
          } else {
            fieldDict(field.getFieldID)
          }
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

        val segmentDF = if (!segment.getDeltalogsList.isEmpty && segment.getDeltalogsList.get(0).getBinlogsList.length > 0) {
          val deltaPath = "%s%s/%s/binlogs/delta_log/%d/%d/%d/%d".format(fs, bucketName, backupPath, segment.getCollectionId, segment.getPartitionId, segment.getSegmentId, segment.getSegmentId)
          val delta = spark.read.format("milvusbinlog").load(deltaPath)
          val deltaDF = delta.select(json_tuple(delta.col("val"), "pk", "ts"))
          val compactedDF = insertDF.join(deltaDF, insertDF(pkFieldName) === deltaDF("c0"), "left_anti")
          compactedDF
        } else {
          insertDF
        }

        val resultDf = segmentDF.drop(milvusRowId, milvusTs).drop(segmentRowId)

        resultDf.write
          .options(milvusOptions)
          .format("milvus")
          .mode(SaveMode.Append)
          .save()

        insertedRows += resultDf.count()
        log.info(s"finish insert segment ${segment.getSegmentId} progress: ${index+1}/${segments.length} inserted rows: ${insertedRows}")
      })
    })
  }
}


