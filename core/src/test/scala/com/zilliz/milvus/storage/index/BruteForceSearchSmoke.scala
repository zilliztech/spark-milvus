package com.zilliz.milvus.storage.index

import java.nio.file.Files
import java.util.HashMap

import com.zilliz.milvus.jni.storage.StorageNative

/** Explicit real-native verification; missing libraries fail this main. */
object BruteForceSearchSmoke {
  def main(args: Array[String]): Unit = {
    require(
      args.length == 1 && Set("storage-first", "knowhere-first").contains(
        args(0)
      )
    )
    if (args(0) == "storage-first") storage()
    vectors()
    storage()
    vectors()
    println(s"PASS: core Knowhere BruteForce and storage, ${args(0)}")
  }

  private def storage(): Unit = {
    val directory = Files.createTempDirectory("bruteforce-storage-")
    val properties = new HashMap[String, String]()
    properties.put("fs.storage_type", "local")
    properties.put("fs.root_path", directory.toString)
    val handle = StorageNative.filesystemGet(properties, "")
    try {
      val content = Array[Byte](1, 2, 3)
      StorageNative.writeFile(handle, "check", content)
      assert(StorageNative.readFileAll(handle, "check").sameElements(content))
      StorageNative.deleteFile(handle, "check")
    } finally {
      StorageNative.filesystemDestroy(handle)
      Files.deleteIfExists(directory.resolve("check"))
      Files.delete(directory)
    }
  }

  private def vectors(): Unit = {
    val query = Array(0f, 0f)
    val l2 = new BruteForceSearch[Int](query, 3, "L2")
    try {
      val first = Vector(Array(3f, 4f), null, Array(9f, 0f))
      l2.addBatch(first.size)(first, identity)
      query(0) = Float.NaN // Later caller mutation must not change this search.
      val second = Vector(Array(0f, 2f), Array(0f, 3f), Array(0f, 0f))
      l2.addBatch(second.size)(second, i => i + 3)
      assert(l2.results.map(_.value) == Vector(5, 3, 4))
      assert(l2.results.map(_.rowOffset) == Vector(5L, 3L, 4L))
      assert(l2.results.map(_.distance) == Vector(0d, 4d, 9d))
      assert(l2.allocatedBytes == 0)
    } finally l2.close()

    val exactL2 = new BruteForceSearch[Int](Array(0f, 0f), 2, "L2")
    try {
      exactL2.addBatch(1)(_ => Array(1f, 1f), _ => 0)
      exactL2.addBatch(1)(_ => Array(-1f, -1f), _ => 1)
      // Squared L2 = 2 must remain exactly 2 through native result merging.
      assert(exactL2.results.map(_.distance) == Vector(2d, 2d))
      assert(exactL2.results.map(_.rowOffset) == Vector(0L, 1L))
      assert(exactL2.allocatedBytes == 0)
    } finally exactL2.close()

    Seq("IP", "COSINE").foreach { metric =>
      val search = new BruteForceSearch[Int](Array(1f, 0f), 9, metric)
      try {
        val data = Vector(Array(-2f, 0f), null, Array(0f, 0f), Array(1f, 1f))
        search.addBatch(data.size)(data, identity)
        assert(search.results.map(_.value) == Vector(3, 2, 0))
        val expected =
          if (metric == "IP") Vector(1d, 0d, -2d)
          else Vector(1 / math.sqrt(2), 0d, -1d)
        search.results.zip(expected).foreach { case (hit, score) =>
          assert(math.abs(hit.distance - score) < 1e-5)
        }
        assert(search.allocatedBytes == 0)
      } finally search.close()
    }
    val zero = new BruteForceSearch[Int](Array(0f, 0f), 1, "COSINE")
    try {
      zero.addBatch(1)(_ => Array(1f, 2f), identity)
      assert(zero.results.head.distance == 0)
    } finally zero.close()
    val failed = new BruteForceSearch[Int](Array(0f), 1, "L2")
    val expected = new IllegalStateException("Cannot copy selected row")
    try {
      try {
        failed.addBatch(1)(_ => Array(1f), _ => throw expected)
        throw new AssertionError("Copy failure must propagate")
      } catch {
        case error: IllegalStateException => assert(error eq expected)
      }
      assert(failed.allocatedBytes == 0)
    } finally failed.close()
  }
}
