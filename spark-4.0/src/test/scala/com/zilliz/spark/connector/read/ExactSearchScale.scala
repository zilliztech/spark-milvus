package com.zilliz.spark.connector.read

import java.nio.charset.StandardCharsets

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.milvus.storage.snapshot.json.{
  ManifestItemJson,
  SegmentListJson
}
import com.zilliz.milvus.storage.write.commit.JobManifest
import com.zilliz.milvus.storage.write.exec.StagingLayout
import com.zilliz.spark.connector.options.{HadoopStorageKeys, MilvusOption}
import io.milvus.grpc.common.KeyValuePair
import io.milvus.grpc.schema.{CollectionSchema, DataType, FieldSchema}

/** A brute-force search at a size that does not fit in memory, run the way a
  * job runs it: the connector writes the base as V3 segments on the local
  * filesystem, reads them back through their manifests, and `MilvusSearch`
  * searches them with the upstream Knowhere kernel.
  *
  * It answers what a scalar stand-in cannot: how the first stage partitions,
  * what it spills, and what each executor holds. The process holds at the end
  * so the Spark UI stays up; kill it when the stage has been read.
  *
  * {{{
  *   EXACT_ROOT=/path/to/scratch     where the segments are written
  *   EXACT_DIM=1024                  vector dimension
  *   EXACT_BASE_BYTES=10737418240    base vectors, in bytes
  *   EXACT_QUERY_BYTES=1073741824    query vectors, in bytes
  *   EXACT_QUERY_MAX_BYTES=67108864  milvus.search.queries.max.bytes; below
  *                                   the query set's size this takes the
  *                                   shuffle route instead of the driver
  *   EXACT_VECTORS_MAX_BYTES=...     milvus.search.vectors.max.bytes, the
  *                                   vectors one executor keeps; it is
  *                                   divided by the task slots, so under
  *                                   local[64] the default 2 GiB leaves 32
  *                                   MiB per task and a 160 MiB segment does
  *                                   not fit
  *   EXACT_TOPK=10
  *   EXACT_EXECUTORS=3               task slots, times the cores below
  *   EXACT_EXECUTOR_CORES=2
  *   EXACT_EXECUTOR_MEMORY=2048      megabytes each
  *   EXACT_GROUP_MAX_BYTES=...       milvus.search.group.max.bytes, the query
  *                                   bytes one engine call sees
  *   EXACT_BATCH_MAX_BYTES=...       milvus.read.batch.max.bytes, the base
  *                                   bytes joined before one engine call on
  *                                   the streaming path
  *   EXACT_MASTER=local[6]           local-cluster needs a Spark distribution
  *   EXACT_UI_PORT=4062
  *   EXACT_WRITE=true                write the base; false reuses what is there
  *   EXACT_SEARCH=true               false stops after the write
  *   EXACT_HOLD=true                 hold the process at the end for the UI;
  *                                   false exits once the hits are counted
  * }}}
  */
object ExactSearchScale {

  private def env(name: String, fallback: String): String =
    sys.env.get(name).map(_.trim).filter(_.nonEmpty).getOrElse(fallback)

  // Where the files live. `fs.root_path` is what a writer prefixes every key
  // with, so the reader lists under that same value.
  private val directory = env("EXACT_ROOT", "/tmp/exact-search-scale")
  private val root = s"$directory/files"
  private val dim = env("EXACT_DIM", "1024").toInt
  private val baseBytes = env("EXACT_BASE_BYTES", "1073741824").toLong
  private val queryBytes = env("EXACT_QUERY_BYTES", "107374182").toLong
  private val topK = env("EXACT_TOPK", "10").toInt
  private val executors = env("EXACT_EXECUTORS", "3").toInt
  private val executorCores = env("EXACT_EXECUTOR_CORES", "2").toInt
  private val executorMemory = env("EXACT_EXECUTOR_MEMORY", "2048").toInt

  private val rowBytes = dim.toLong * 4
  private val baseRows = baseBytes / rowBytes
  private val queryRows = queryBytes / rowBytes

  /** One Int64 primary key and one float vector: what a search needs, nothing
    * else, so the segments carry base vectors rather than payload.
    */
  private val collection = CollectionSchema(
    name = "exact_search_scale",
    fields = Seq(
      FieldSchema(
        fieldID = 100L,
        name = "id",
        dataType = DataType.Int64,
        isPrimaryKey = true
      ),
      FieldSchema(
        fieldID = 101L,
        name = "vector",
        dataType = DataType.FloatVector,
        typeParams = Seq(KeyValuePair("dim", dim.toString))
      )
    )
  )

  // The local backend: files under one root, no service and no credentials.
  // The write path still names a bucket, which the local store treats as a
  // prefix rather than a remote container.
  private def storageOptions(): Map[String, String] = Map(
    "fs.storage_type" -> "local",
    StorageProperties.RootPath -> root,
    StorageProperties.BucketName -> "exact"
  )

  def main(args: Array[String]): Unit = {
    // local-cluster needs a Spark distribution to launch its executor JVMs;
    // without one the same work runs in this process, which still shows the
    // partitioning, the spill and the collection pauses.
    val master = env("EXACT_MASTER", s"local[${executors * executorCores}]")
    println(s"""
      |base vectors      $baseRows  (${baseBytes / (1024 * 1024)} MiB of vectors)
      |query vectors     $queryRows  (${queryBytes / (1024 * 1024)} MiB of vectors)
      |dimension         $dim
      |top-k             $topK
      |pairs M x N       ${queryRows * baseRows}
      |master            $master
      |directory         $directory
      |""".stripMargin)

    val spark = SparkSession
      .builder()
      .master(master)
      .appName("exact-search-scale")
      .config("spark.ui.enabled", "true")
      .config("spark.ui.port", env("EXACT_UI_PORT", "4060"))
      .config("spark.driver.memory", "4g")
      .config("spark.executor.memory", s"${executorMemory}m")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config(
        "spark.sql.shuffle.partitions",
        (executors * executorCores).toString
      )
      .config("spark.eventLog.enabled", "false")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    println(s"Spark UI: ${spark.sparkContext.uiWebUrl.getOrElse("(none)")}")

    val storage = storageOptions()
    if (env("EXACT_WRITE", "true").toBoolean) writeBase(spark, storage)
    val manifests = readManifests(storage)
    println(s"segments: ${manifests.size}")
    if (!env("EXACT_SEARCH", "true").toBoolean) {
      println("--- EXACT_SEARCH=false: base written, no search ---")
      spark.stop()
      return
    }

    val queries = spark
      .range(queryRows)
      .select(
        col("id").as(SearchQueries.IdColumn),
        array(
          (0 until dim).map(d => (rand(d.toLong) * 2.0 - 1.0).cast("float")): _*
        ).as(SearchQueries.VectorColumn)
      )

    val options = storage ++ Map(
      MilvusOption.SnapshotManifests -> SegmentListJson.encodeManifestItems(
        manifests.zipWithIndex.map { case (path, index) =>
          ManifestItemJson(index + 1L, s"""{"ver":-1,"base_path":"$path"}""")
        }
      ),
      MilvusOption.SnapshotSchemaBytes -> java.util.Base64.getEncoder
        .encodeToString(collection.toByteArray),
      MilvusOption.SnapshotCollectionId -> "1",
      MilvusOption.SnapshotPartitionIds -> "0",
      MilvusOption.ReadColumnar -> "true"
    ) ++ sys.env
      .get("EXACT_QUERY_MAX_BYTES")
      .map(MilvusOption.SearchQueriesMaxBytes -> _) ++ sys.env
      .get("EXACT_VECTORS_MAX_BYTES")
      .map(MilvusOption.SearchVectorsMaxBytes -> _) ++ sys.env
      .get("EXACT_GROUP_MAX_BYTES")
      .map(MilvusOption.SearchGroupMaxBytes -> _) ++ sys.env
      .get("EXACT_BATCH_MAX_BYTES")
      .map(MilvusOption.ReadBatchMaxBytes -> _)

    println("--- first stage starts; watch the UI, then kill this process ---")
    val searchStarted = System.currentTimeMillis()
    val hits = MilvusSearch.search(
      spark,
      options,
      queries,
      "vector",
      topK,
      "L2",
      "exact",
      Map.empty,
      None,
      Seq.empty,
      true
    )
    println(s"hits: ${hits.count()}")
    println(
      s"search done in ${(System.currentTimeMillis() - searchStarted) / 1000}s"
    )
    if (env("EXACT_HOLD", "true").toBoolean) {
      while (true) Thread.sleep(60000L)
    } else spark.stop()
  }

  /** The base, written by the connector as V3 segments, one per task. */
  private def writeBase(
      spark: SparkSession,
      storage: Map[String, String]
  ): Unit = {
    val tasks = executors * executorCores
    val frame = spark
      .range(baseRows)
      .repartition(tasks)
      .select(
        col("id"),
        array(
          (0 until dim).map(d =>
            (rand(1000L + d) * 2.0 - 1.0).cast("float")
          ): _*
        ).as("vector")
      )
    val started = System.currentTimeMillis()
    frame.write
      .format("milvus")
      .mode("append")
      .options(
        storage ++ Map(
          MilvusOption.SnapshotMode -> "true",
          MilvusOption.SnapshotSchemaBytes -> java.util.Base64.getEncoder
            .encodeToString(collection.toByteArray),
          MilvusOption.SnapshotCollectionId -> "1",
          MilvusOption.SnapshotPartitionIds -> "0",
          MilvusOption.MilvusCollectionName -> "exact_search_scale",
          MilvusOption.MilvusInsertMaxBatchSize -> "20000"
        )
      )
      .save()
    println(
      s"base written in ${(System.currentTimeMillis() - started) / 1000}s"
    )
  }

  /** The base paths of the one committed job under the staging prefix. */
  private def readManifests(storage: Map[String, String]): Seq[String] = {
    val store = HadoopStorageKeys.storeFrom(storage)
    try {
      val committed = store
        .list(s"$root/staging", recursive = false)
        .filter(_.isDirectory)
        .map(entry =>
          StagingLayout(root, entry.path.stripSuffix("/").split("/").last)
        )
        .filter(layout => store.exists(layout.marker))
      require(
        committed.size == 1,
        s"one committed job expected under $root/staging, found ${committed.size}"
      )
      JobManifest
        .fromJson(
          new String(
            store.readAll(committed.head.manifest),
            StandardCharsets.UTF_8
          )
        )
        .fold(error => throw error, identity)
        .segments
        .map(_.basePath)
    } finally store.close()
  }
}
