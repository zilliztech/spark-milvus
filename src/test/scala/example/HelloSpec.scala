package example

import com.zilliz.spark.connector.FloatConverter

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
}
