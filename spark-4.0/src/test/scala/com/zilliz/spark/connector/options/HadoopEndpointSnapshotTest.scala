package com.zilliz.spark.connector.options

import java.nio.charset.StandardCharsets
import java.nio.file.Files

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

import com.zilliz.milvus.storage.credential.StorageProperties
import io.milvus.storage.NativeLibraryLoader

/** The endpoint a snapshot URI is recognized against is the one the driver's
  * store uses, including an endpoint only the session's Hadoop keys supply.
  * Found by a Spark job on UAT. The snapshot is read through the local native
  * backend, so the suite needs libmilvus-storage-jni and cancels without it.
  */
class HadoopEndpointSnapshotTest
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterAll {

  private lazy val spark = SparkSession
    .builder()
    .master("local[1]")
    .appName("hadoop-endpoint-snapshot")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  override protected def afterAll(): Unit = spark.stop()

  private def skipWithoutLibrary(): Unit =
    try NativeLibraryLoader.loadLibrary()
    catch {
      case _: UnsatisfiedLinkError | _: NoClassDefFoundError =>
        cancel("libmilvus-storage-jni is not on this machine")
      case _: RuntimeException =>
        cancel("libmilvus-storage-jni is not on this machine")
    }

  test(
    "a Milvus snapshot URI names the endpoint that only fs.s3a.endpoint sets"
  ) {
    skipWithoutLibrary()
    val root = Files.createTempDirectory("hadoop-endpoint")
    val key = "snapshots/10/metadata/2.json"
    Files.createDirectories(root.resolve(key).getParent)
    // A V3-only snapshot: the catalog opens no segment file for it.
    Files.write(
      root.resolve(key),
      """{
        "snapshot_info": {"name": "s", "id": 1, "collection_id": 10, "partition_ids": [20], "create_ts": 100},
        "collection": {"schema": {"name": "endpoint_case", "fields": [
          {"fieldID": 100, "name": "id", "data_type": "Int64", "is_primary_key": true}
        ]}},
        "manifest_list": [],
        "storagev2_manifest_list": [
          {"segmentID": 30, "manifest": "{\"ver\":1,\"base_path\":\"insert_log/10/20/30\"}"}
        ]
      }""".getBytes(StandardCharsets.UTF_8)
    )
    val hadoop = spark.sparkContext.hadoopConfiguration
    hadoop.set("fs.s3a.endpoint", "s3.example.com:443")
    try {
      val options = Map(
        StorageProperties.StorageType -> StorageProperties.StorageTypeLocal,
        StorageProperties.RootPath -> root.toString,
        StorageProperties.BucketName -> "b",
        MilvusOption.SnapshotPath -> s"https://s3.example.com/b/$key"
      )
      val snapshot = SnapshotSources
        .forRead(MilvusOption(options), withSegments = false)
        .snapshot()
        .fold(e => throw e, identity)
      snapshot.schema.name shouldBe "endpoint_case"
    } finally hadoop.unset("fs.s3a.endpoint")
  }
}
