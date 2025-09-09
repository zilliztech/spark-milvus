package example

import com.zilliz.spark.connector.FloatConverter
import com.zilliz.spark.connector.MilvusClient
import com.zilliz.spark.connector.MilvusConnectionParams

class HelloSpec extends munit.FunSuite {
  test("say hello") {
    val float16 = 1.12f
    val bytes = FloatConverter.toBFloat16Bytes(float16)
    println(bytes)
    val float32 = FloatConverter.fromBFloat16Bytes(bytes)
    println(float32)

    val floatBytes = FloatConverter.toFloat16Bytes(float16)
    println(floatBytes)
    val float32FromBytes = FloatConverter.fromFloat16Bytes(floatBytes)
    println(float32FromBytes)
  }

  test("get segment info") {
    val milvusClient = new MilvusClient(
      MilvusConnectionParams(
        uri = "http://localhost:19530",
        token = "root:Milvus",
        databaseName = "default"
      )
    )
    val segmentInfo =
      milvusClient.getSegmentInfo(458501504995144502L, 458501504996549295L)
    println(segmentInfo)
  }
}
