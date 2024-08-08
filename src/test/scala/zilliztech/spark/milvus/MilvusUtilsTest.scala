package zilliztech.spark.milvus

import org.junit.Test

import java.util.Base64
import scala.collection.JavaConversions._

class MilvusUtilsTest {

  @Test
  def testParseBytesToSparseVector(): Unit = {
    val bytes = Array[Byte](1, 0, 0, 0, -69, -43, -1, 62, 2, 0, 0, 0, 94, -23, 73, 63, 3, 0, 0, 0, 114, 28, -103, 62, 7, 0, 0, 0, 55, 21, -83, 61, 8, 0, 0, 0, -110, -109, 59, 63, 9, 0, 0, 0, -63, -15, 75, 63, 10, 0, 0, 0, -127, 74, 113, 62, 11, 0, 0, 0, -33, -101, 45, 63, 12, 0, 0, 0, -50, -47, 122, 62, 13, 0, 0, 0, -53, 35, -125, 62, 14, 0, 0, 0, 45, -44, -8, 61)
    val map = MilvusUtils.bytesToSparseVector(bytes)
    for (key <- map.keySet) {
      println(key + " -> " + map.get(key))
    }
  }

  @Test
  def testFloatToF16Vector(): Unit = {
    val bytes = MilvusUtils.convertFloatToFloat16ByteArray(0.123455f)
    println(bytes)
    println(bytes.length)
  }

  @Test
  def testBase64Decode(): Unit = {
    val str = "DL1/vr09MT/NPUo+qT4EPgU+NL/7PMk+ID3cPFe/Gr8wPTi/Dj8xvwE+lj4BP1e/v71hPrY+Qr9wv2K/ab+Jvg=="
    val bytes = Base64.getDecoder.decode(str)
    val decodedString = new String(bytes)
    println(decodedString)
  }

}
