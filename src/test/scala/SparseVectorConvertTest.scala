import org.junit.Test

import java.nio.{ByteBuffer, ByteOrder}
import java.util

import scala.collection.JavaConversions._

class SparseVectorConvertTest {

  @Test
  @throws[Exception]
  def testParseBytesToSparseVector(): Unit = {
    val bytes = Array[Byte](1, 0, 0, 0, -69, -43, -1, 62, 2, 0, 0, 0, 94, -23, 73, 63, 3, 0, 0, 0, 114, 28, -103, 62, 7, 0, 0, 0, 55, 21, -83, 61, 8, 0, 0, 0, -110, -109, 59, 63, 9, 0, 0, 0, -63, -15, 75, 63, 10, 0, 0, 0, -127, 74, 113, 62, 11, 0, 0, 0, -33, -101, 45, 63, 12, 0, 0, 0, -50, -47, 122, 62, 13, 0, 0, 0, -53, 35, -125, 62, 14, 0, 0, 0, 45, -44, -8, 61)
    val map = bytesToSparseVector(bytes)
    for (key <- map.keySet) {
      println(key + " -> " + map.get(key))
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
}