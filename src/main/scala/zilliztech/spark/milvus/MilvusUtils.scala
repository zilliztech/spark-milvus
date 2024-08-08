package zilliztech.spark.milvus

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.gson.JsonObject
import io.milvus.grpc._
import io.milvus.param.R
import io.milvus.param.bulkinsert.{BulkInsertParam, GetBulkInsertStateParam}
import io.milvus.param.collection.{DescribeCollectionParam, FlushParam}
import io.milvus.param.control.GetPersistentSegmentInfoParam
import org.apache.hadoop.fs.s3a.S3AFileSystem
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{json_tuple, monotonically_increasing_id, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import zilliztech.spark.milvus.binlog.MilvusBinlogUtil

import java.io.{BufferedReader, DataOutputStream, InputStreamReader}
import java.net.{HttpURLConnection, URI, URL}
import java.nio.{ByteBuffer, ByteOrder}
import java.util
import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions.{`collection AsScalaIterable`, `collection asJava`};

object MilvusUtils {

  private val log = LoggerFactory.getLogger(getClass)

  def configS3Storage(spark: SparkSession, milvusOptions: MilvusOptions): FileSystem = {
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", s"${milvusOptions.storageEndpoint}")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access", "true")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.connection.ssl.enabled", "false")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", milvusOptions.storageUser)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", milvusOptions.storagePassword)
    val fsConf = spark.sparkContext.hadoopConfiguration
    val fileSystem = new S3AFileSystem()
    fileSystem.initialize(new URI(s"s3a://${milvusOptions.bucket}/"), fsConf)
    fileSystem
  }

  def readMilvusCollection(spark: SparkSession, milvusOptions: MilvusOptions): DataFrame = {
    val fileSystem = configS3Storage(spark, milvusOptions)

    val milvusClient = MilvusConnection.acquire(milvusOptions)

    // describe the collection
    val describeCollectionParam = DescribeCollectionParam.newBuilder
      .withDatabaseName(milvusOptions.databaseName)
      .withCollectionName(milvusOptions.collectionName)
      .build
    val describeCollectionResp: R[DescribeCollectionResponse] = milvusClient.describeCollection(describeCollectionParam)
    if (describeCollectionResp.getStatus != 0 || describeCollectionResp.getException != null) {
      log.info(s"Fail to describe collection ${milvusOptions.databaseName}.${milvusOptions.collectionName} status: ${describeCollectionResp.getStatus}, exception: ${describeCollectionResp.getException}")
    }
    val collection = describeCollectionResp.getData

    // should call flush first, if you want to read growing segments

    // get segments
    val getPersistentSegmentInfoParam: GetPersistentSegmentInfoParam = GetPersistentSegmentInfoParam.newBuilder
      .withCollectionName(milvusOptions.collectionName)
      .build
    val getPersistentSegmentInfoResp: R[GetPersistentSegmentInfoResponse] = milvusClient.getPersistentSegmentInfo(getPersistentSegmentInfoParam)
    val segments: util.List[PersistentSegmentInfo] = getPersistentSegmentInfoResp.getData.getInfosList

    val fieldIDs = Array(0L, 1L) ++ collection.getSchema.getFieldsList.map(field => field.getFieldID)
    val fieldDict = collection.getSchema.getFieldsList.map(field => (field.getFieldID, field.getName)).toMap
    val pkField = collection.getSchema.getFieldsList.filter(field => field.getIsPrimaryKey).toArray.apply(0)
    val pkFieldName = pkField.getName
    val vecFeilds = collection.getSchema.getFieldsList.filter(field =>
      Seq(DataType.BinaryVector.getNumber, DataType.FloatVector.getNumber).contains(field.getDataType.getNumber)
    ).map(field => field.getFieldID)

    val parseVectorFunc = udf(MilvusBinlogUtil.littleEndianBinaryToFloatArray(_: Array[Byte]): Array[Float])

    // read segments to DF
    val milvusRowId = "milvus_rowid"
    val milvusTs = "milvus_ts"
    val segmentRowId = "segment_rowid"
    val segmentDfs = segments.map(segment => {
      val dfs = fieldIDs.map(fieldID => {
        val insertPath = "%s%s/%s/insert_log/%d/%d/%d/%d".format(milvusOptions.fs, milvusOptions.bucket, milvusOptions.rootPath, segment.getCollectionID, segment.getPartitionID, segment.getSegmentID, fieldID)

        val fieldName = if (fieldID == 0) {
          milvusRowId
        } else if (fieldID == 1) {
          milvusTs
        } else {
          fieldDict(fieldID)
        }
        val fieldColumn = spark.read.format("milvusbinlog").load(insertPath)
          .withColumnRenamed("val", fieldName)
          .withColumn(segmentRowId, monotonically_increasing_id())

        if (vecFeilds.contains(fieldID)) {
          fieldColumn.withColumn(fieldName, parseVectorFunc(fieldColumn(fieldName)))
        } else {
          fieldColumn
        }
      })
      val insertDF = dfs.reduce { (leftDF, rightDF) =>
        leftDF.join(rightDF, segmentRowId)
      }

      val deltaPath = "%s%s/%s/delta_log/%d/%d/%d".format(milvusOptions.fs, milvusOptions.bucket, milvusOptions.rootPath, segment.getCollectionID, segment.getPartitionID, segment.getSegmentID)
      val path = new Path(deltaPath)
      val segmentDF = if (fileSystem.exists(path)) {
        val delta = spark.read.format("milvusbinlog").load(deltaPath)
        val deltaDF = delta.select(json_tuple(delta.col("val"), "pk", "ts"))
        // deltaDF("c0") is pk column
        val compactedDF = insertDF.join(deltaDF, insertDF(pkFieldName) === deltaDF("c0"), "left_anti")
        compactedDF
      } else {
        insertDF
      }
      segmentDF
    })
    val collectionDF = segmentDfs.reduce { (leftDF, rightDF) =>
      leftDF.union(rightDF)
    }
    milvusClient.close()
    collectionDF.drop(milvusRowId, milvusTs).drop(segmentRowId)
  }

  private def extractPathWithoutBucket(s3Path: String): String = {
    val uri = new URI(s3Path)
    val pathWithoutBucket = uri.getPath.drop(1) // Drop the leading '/'
    pathWithoutBucket
  }

  def bulkInsertFromSpark(spark: SparkSession, milvusOptions: MilvusOptions, path: String, format: String): Unit = {
    if (milvusOptions.isZillizCloud()) {
      batchBulkinsertZillizCloud(spark, milvusOptions, path, format)
    } else {
      batchBulkinsert(spark, milvusOptions, path, format)
    }
    val milvusClient = MilvusConnection.acquire(milvusOptions)
    val flushParam: FlushParam = FlushParam.newBuilder
      .withDatabaseName(milvusOptions.databaseName)
      .addCollectionName(milvusOptions.collectionName)
      .build
    val flushR: R[FlushResponse] = milvusClient.flush(flushParam)
    log.info(s"flush response ${flushR}")
    milvusClient.close()
  }

  private def batchBulkinsert(spark: SparkSession, milvusOptions: MilvusOptions, path: String, format: String): Unit = {
    // 4. As the vector data has been stored in the s3 bucket as files, here we list the directory and get the file paths
    // to prepare input of Zilliz Cloud Import Data API call.
    val hadoopConfig = spark.sparkContext.hadoopConfiguration
    val directory = new Path(path)
    val fs = FileSystem.get(directory.toUri, hadoopConfig)
    val files = fs.listStatus(directory)
    val validFiles = files.filter(file => {
      file.getPath.getName.endsWith(s".${format}")
    })

    val milvusClient = MilvusConnection.acquire(milvusOptions)

    validFiles.foreach(file => {
      val ouputFilePathWithoutBucket = extractPathWithoutBucket(file.getPath.toString)

      // 5. Make a call to Milvus bulkinsert API.
      val bulkInsertFiles: List[String] = List(ouputFilePathWithoutBucket)
      val bulkInsertParam: BulkInsertParam = if (milvusOptions.partitionName.isEmpty) {
        BulkInsertParam.newBuilder
          .withCollectionName(milvusOptions.collectionName)
          .withFiles(bulkInsertFiles.asJava)
          .build()
      } else {
        BulkInsertParam.newBuilder
          .withCollectionName(milvusOptions.collectionName)
          .withPartitionName(milvusOptions.partitionName)
          .withFiles(bulkInsertFiles.asJava)
          .build()
      }

      val bulkInsertR: R[ImportResponse] = milvusClient.bulkInsert(bulkInsertParam)
      log.info(s"bulkinsert ${milvusOptions.collectionName} resp: ${bulkInsertR.toString}")
      val taskId: Long = bulkInsertR.getData.getTasksList.get(0)

      var bulkloadState = milvusClient.getBulkInsertState(GetBulkInsertStateParam.newBuilder.withTask(taskId).build)
      while (bulkloadState.getData.getState.getNumber != 1 &&
        bulkloadState.getData.getState.getNumber != 6 &&
        bulkloadState.getData.getState.getNumber != 7) {
        bulkloadState = milvusClient.getBulkInsertState(GetBulkInsertStateParam.newBuilder.withTask(taskId).build)
        log.info(s"bulkinsert ${milvusOptions.collectionName} resp: ${bulkInsertR.toString} state: ${bulkloadState}")
        Thread.sleep(3000)
      }
      if (bulkloadState.getData.getState.getNumber != 6) {
        log.error(s"bulkinsert failed ${milvusOptions.collectionName} state: ${bulkloadState}")
      }
    })
    milvusClient.close()
  }

  private def batchBulkinsertZillizCloud(spark: SparkSession, milvusOptions: MilvusOptions, path: String, format: String): Unit = {
    // 4. As the vector data has been stored in the s3 bucket as files, here we list the directory and get the file paths
    // to prepare input of Zilliz Cloud Import Data API call.
    val hadoopConfig = spark.sparkContext.hadoopConfiguration
    val directory = new Path(path)
    val fs = FileSystem.get(directory.toUri, hadoopConfig)
    val files = fs.listStatus(directory)
    val validFiles = files.filter(file => {
      file.getPath.getName.endsWith(s".${format}")
    })

    if (milvusOptions.zillizRegion() == "") {
      throw new Exception("zilliz cloud region is not set, please set it in milvus option: zillizcloud.region=xxx")
    }

    validFiles.foreach(file => {
      val completePath = file.getPath.toString.replace("s3a://", "s3://")

      val jobID = doBulkinsertRestAPI(milvusOptions, completePath)
      var res = getBulkinsertStateRestAPI(milvusOptions, jobID)
      var code = res._1
      var percent = res._2
      var errorMessage = res._3
      while (code == 200 && percent < 1 && errorMessage == "null") {
        Thread.sleep(3000)
        res = getBulkinsertStateRestAPI(milvusOptions, jobID)
        code = res._1
        percent = res._2
        errorMessage = res._3
      }
      log.info(s"Bulkinsert State: ${res.toString}")
    })
  }

  def doBulkinsertRestAPI(milvusOptions: MilvusOptions, completePath: String): String = {
    val importApiUrl = s"https://controller.api.${milvusOptions.zillizRegion()}.zillizcloud.com/v1/vector/collections/import"
    val postData =
      s"""
         |{
         |  "clusterId": "${milvusOptions.zillizInstanceID()}",
         |  "collectionName": "${milvusOptions.collectionName}",
         |  "objectUrl": "${completePath}",
         |  "accessKey": "${milvusOptions.storageUser}",
         |  "secretKey": "${milvusOptions.storagePassword}"
         |}
         |""".stripMargin
    val url = new URL(importApiUrl)
    val connection = url.openConnection().asInstanceOf[HttpURLConnection]
    try {
      // Set up the request method and headers
      connection.setRequestMethod("POST")
      connection.setRequestProperty("Authorization", s"Bearer ${milvusOptions.zillizCloudAPIKey}")
      connection.setRequestProperty("Content-Type", "application/json")
      connection.setRequestProperty("accept", "application/json")
      connection.setDoOutput(true)

      // Write the POST data to the connection
      val out = new DataOutputStream(connection.getOutputStream)
      out.writeBytes(postData)
      out.flush()
      out.close()

      // Read the response
      val inputStream = new BufferedReader(new InputStreamReader(connection.getInputStream))
      var inputLine: String = null
      val response = new StringBuilder

      while ( {
        inputLine = inputStream.readLine();
        inputLine != null
      }) {
        response.append(inputLine)
      }
      inputStream.close()

      // Print the response
      log.info(s"Bulkinsert Response body: ${response.toString}")
      println(s"Bulkinsert Response body: ${response.toString}")
      val objectMapper = new ObjectMapper()
      val jsonNode = objectMapper.readTree(response.toString)
      val code = jsonNode.get("code").asInt()
      if (code == 200) {
        val jobID = jsonNode.get("data").get("jobId").asText
        jobID
      } else {
        throw new Exception(s"Bulkinsert RESTAPI error ${response.toString}")
      }
    } finally {
      // Disconnect to release resources
      connection.disconnect()
    }
  }

  def getBulkinsertStateRestAPI(milvusOptions: MilvusOptions, jobID: String): (Int, Double, String) = {
    val getImportURL = s"https://controller.api.${milvusOptions.zillizRegion()}.zillizcloud.com/v1/vector/collections/import/get?jobId=${jobID}&clusterId=${milvusOptions.zillizInstanceID()}"
    val url = new URL(getImportURL)
    val connection = url.openConnection().asInstanceOf[HttpURLConnection]
    try {
      // Set the request method to GET
      connection.setRequestMethod("GET")
      connection.setRequestProperty("Authorization", s"Bearer ${milvusOptions.zillizCloudAPIKey}")
      connection.setRequestProperty("Content-Type", "application/json")
      connection.setRequestProperty("accept", "application/json")
      // Read the response
      val inputStream = new BufferedReader(new InputStreamReader(connection.getInputStream))
      var inputLine: String = null
      val response = new StringBuilder

      while ( {
        inputLine = inputStream.readLine();
        inputLine != null
      }) {
        response.append(inputLine)
      }
      inputStream.close()
      // log the response
      log.info(s"Get Bulkinsert State Response: ${response.toString}")
      val objectMapper = new ObjectMapper()
      val jsonNode = objectMapper.readTree(response.toString)
      val code = jsonNode.get("code").asInt()
      val percent = jsonNode.get("data").get("readyPercentage").asDouble()
      val errorMessage = jsonNode.get("data").get("errorMessage").toString
      (code, percent, errorMessage)
    } finally {
      // Disconnect to release resources
      connection.disconnect()
    }
  }

  def bytesToSparseVector(bytes: Array[Byte]): util.SortedMap[Integer, Float] = {
    val length = bytes.length
    var p = 0
    val map = new util.TreeMap[Integer, Float]
    while ( {
      p < length
    }) {
      var buffer = ByteBuffer.wrap(bytes, p, 4)
      buffer.order(ByteOrder.LITTLE_ENDIAN)
      val k = buffer.getInt
      p = p + 4
      buffer = ByteBuffer.wrap(bytes, p, 4)
      buffer.order(ByteOrder.LITTLE_ENDIAN)
      val v = buffer.getFloat
      p = p + 4
      map.put(k, v)
    }
    map
  }

  def jsonToSparseVector(json: JsonObject): util.SortedMap[Long, Float] = {
    val map = new util.TreeMap[Long, Float]
    val dict = json.asMap()
    dict.keySet().forEach(key => {
      map.put(key.toLong, json.get(key).getAsFloat)
    })
    map
  }

  def convertFloatToFloat16ByteArray(value: Float): Array[Byte] = {
    import java.lang._

    val floatBits = Float.floatToIntBits(value)
    val sign = (floatBits >> 16) & 0x8000 // Sign bit
    var exponent = ((floatBits >> 23) & 0xFF) - 112 // Exponent bits
    var mantissa = (floatBits & 0x7FFFFF) >> 13 // Mantissa bits
    if (exponent <= 0) if (exponent < -10) { // Too small, convert to zero
      exponent = 0
      mantissa = 0
    }
    else { // Subnormal number
      mantissa = (mantissa | 0x800000) >> (1 - exponent)
      exponent = 0
    }
    else if (exponent > 0x1F) { // Too large, convert to infinity
      exponent = 0x1F
      mantissa = 0
    }
    val float16Bits = sign | (exponent << 10) | mantissa
    // Convert to byte array
    ByteBuffer.allocate(2).putShort(float16Bits.toShort).array
  }

  def getVectorDim(field: FieldSchema): Int = {
    val params = field.getTypeParamsList.map(x => x.getKey -> x.getValue).toMap
    val dimStr = params.get("dim")
    if (dimStr.isEmpty) {
      0
    } else {
      dimStr.get.toInt
    }
  }
}
