package com.zilliz.spark.connector.read

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import com.zilliz.milvus.jni.vector.NativeVectorLibrary
import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.milvus.storage.manifest.SegmentManifestFixture
import com.zilliz.milvus.storage.write.commit.JobManifest
import com.zilliz.spark.connector.options.MilvusOption
import com.zilliz.spark.connector.procedure.{
  BuildIndexProcedure,
  ProcedureArgs,
  WriteSnapshotProcedure
}

/** The index counterpart of [[ExactSearchScale]]: over the base that harness
  * wrote, run `build_index` (HNSW) on every segment, `write_snapshot`, and then
  * search the snapshot in `index` mode, measuring build time, index size, and
  * recall against an `exact` search over the same queries.
  *
  * No Milvus and no object store: the snapshot document the build plans against
  * is synthesized here from the committed job manifest, the way
  * `LocalEndToEndSmoke` does it. The written snapshot is connector-only
  * (`restorable => false`), because the base sits under `staging/` and the
  * indexes under `built/`.
  *
  * {{{
  *   INDEX_ROOT=/tmp/exact-2g          where ExactSearchScale wrote the base
  *   INDEX_DIM=1024
  *   INDEX_QUERY_BYTES=268435456       query vectors, in bytes (same seeds as ExactSearchScale)
  *   INDEX_SAMPLE=4096                 queries used for the recall comparison
  *   INDEX_TYPE=HNSW  INDEX_PARAMS=M=16,efConstruction=200
  *   INDEX_EFS=64,128,256              search widths swept on the sample
  *   INDEX_MASTER=local[8]
  *   INDEX_BUILD=true                  false reuses INDEX_JOB / INDEX_SNAPSHOT
  *   INDEX_EXACT_BATCH_MAX_BYTES=4194304
  *   INDEX_VECTORS_PER_TASK_MIB=64     exact-sample vector budget per task slot
  *   INDEX_SKIP_SAMPLE=false           true skips the exact/recall sample and only times the full search
  *   INDEX_FULL_EF=128
  * }}}
  */
object IndexSearchScale {

  private def env(name: String, fallback: String): String =
    sys.env.get(name).map(_.trim).filter(_.nonEmpty).getOrElse(fallback)

  private val directory = env("INDEX_ROOT", "/tmp/exact-2g")
  private val root = s"$directory/files"
  private val dim = env("INDEX_DIM", "1024").toInt
  private val rowBytes = dim.toLong * 4
  private val queryRows =
    env("INDEX_QUERY_BYTES", "268435456").toLong / rowBytes
  private val sample = env("INDEX_SAMPLE", "4096").toLong
  private val topK = 10
  private val collectionName = "exact_search_scale"
  private val collectionId = 1L
  private val partitionId = 0L

  private val storage: Map[String, String] = Map(
    "fs.storage_type" -> "local",
    StorageProperties.RootPath -> root,
    StorageProperties.BucketName -> "exact"
  )

  def main(args: Array[String]): Unit = {
    val started = System.nanoTime()
    def step(name: String): Unit =
      println(f"[${(System.nanoTime() - started) / 1e9}%7.1fs] $name")

    NativeVectorLibrary.load()
    val master = env("INDEX_MASTER", "local[8]")
    val spark = SparkSession
      .builder()
      .master(master)
      .appName("index-search-scale")
      .config("spark.ui.enabled", "true")
      .config("spark.ui.port", env("INDEX_UI_PORT", "4063"))
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.shuffle.partitions", "16")
      .config("spark.eventLog.enabled", "false")
      .config("spark.driver.maxResultSize", "8g")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    // ---- the base, as the committed write job describes it -----------------
    val job = committedJob()
    step(
      s"base: ${job.segments.size} segments, ${job.segments.map(_.rowCount).sum} rows, job ${job.jobId}"
    )

    // ---- a snapshot document over those segments ---------------------------
    val sourceKey = s"snapshots/$collectionId/metadata/1.json"
    writeSnapshotDocument(job, sourceKey)
    val snapshotOptions = storage + (MilvusOption.SnapshotPath -> sourceKey)
    step(s"snapshot document written at $sourceKey")

    // ---- build_index -------------------------------------------------------
    val indexType = env("INDEX_TYPE", "HNSW")
    val params = env("INDEX_PARAMS", "M=16,efConstruction=200")
    val (jobId, builtSnapshot) =
      if (env("INDEX_BUILD", "true").toBoolean) {
        val buildStarted = System.nanoTime()
        val built = BuildIndexProcedure.run(
          ProcedureArgs(
            values = Map(
              "collection" -> collectionName,
              "field" -> "vector",
              "output" -> "built",
              "index_type" -> indexType,
              "metric" -> "L2",
              "params" -> params,
              "build_id" -> System.currentTimeMillis()
            ),
            options = snapshotOptions
          )
        )
        val buildSeconds = (System.nanoTime() - buildStarted) / 1e9
        val indexedRows = built.map(_.getLong(2)).sum
        val indexBytes = built.map(_.getLong(4)).sum
        val objects = built.map(_.getInt(3)).sum
        val id = built.head.getString(6)
        println(
          f"RESULT build index_type=$indexType params=$params segments=${built.size} rows=$indexedRows " +
            f"objects=$objects index_bytes=$indexBytes index_mib=${indexBytes / 1048576.0}%.1f " +
            f"build_s=$buildSeconds%.1f vectors_per_s=${indexedRows / buildSeconds}%.0f job=$id"
        )
        step(f"build_index done in $buildSeconds%.1fs")

        val writeStarted = System.nanoTime()
        val written = WriteSnapshotProcedure.run(
          ProcedureArgs(
            values = Map(
              "collection" -> collectionName,
              "job" -> id,
              "input" -> "built",
              "snapshot_id" -> 5000L,
              "snapshot_name" -> "built-5000",
              "restorable" -> false
            ),
            options = snapshotOptions
          )
        )
        val key = written.head.getString(0)
        println(
          f"RESULT write_snapshot key=$key segments=${written.head
              .getInt(3)} indexes=${written.head.getInt(4)} " +
            f"bytes=${written.head.getLong(5)} write_s=${(System.nanoTime() - writeStarted) / 1e9}%.1f"
        )
        step(s"write_snapshot done: $key")
        (id, key)
      } else (env("INDEX_JOB", ""), env("INDEX_SNAPSHOT", ""))

    // ---- queries: the same seeds ExactSearchScale uses ---------------------
    val queries = spark
      .range(queryRows)
      .select(
        col("id").as(SearchQueries.IdColumn),
        array(
          (0 until dim).map(d => (rand(d.toLong) * 2.0 - 1.0).cast("float")): _*
        ).as(SearchQueries.VectorColumn)
      )
      .cache()
    queries.count()
    val sampled = queries.filter(col(SearchQueries.IdColumn) < sample).cache()
    sampled.count()
    step(s"queries ready: $queryRows total, $sample sampled")

    val slots = spark.sparkContext.defaultParallelism
    val builtOptions = storage ++ Map(
      MilvusOption.SnapshotPath -> builtSnapshot,
      MilvusOption.ReadColumnar -> "true",
      MilvusOption.SearchSegmentsMaxBytes -> (slots.toLong * env(
        "INDEX_VECTORS_PER_TASK_MIB",
        "64"
      ).toLong * 1048576L).toString,
      MilvusOption.ReadBatchMaxBytes -> env(
        "INDEX_EXACT_BATCH_MAX_BYTES",
        "4194304"
      )
    )

    def hits(
        mode: String,
        frame: DataFrame,
        parameters: Map[String, String]
    ): (Map[Long, Seq[Long]], Double) = {
      val t0 = System.nanoTime()
      val rows = MilvusSearch
        .search(
          spark,
          builtOptions,
          frame,
          "vector",
          topK,
          "L2",
          mode,
          parameters,
          None,
          Seq("id"),
          false
        )
        .select(
          col(SearchQueries.IdColumn),
          col("rank"),
          col("id")
        )
        .collect()
      val seconds = (System.nanoTime() - t0) / 1e9
      val byQuery = rows
        .groupBy(_.getLong(0))
        .map { case (q, rs) =>
          q -> rs.sortBy(_.getInt(1)).map(_.getLong(2)).toSeq
        }
      (byQuery, seconds)
    }

    // ---- exact baseline on the sample --------------------------------------
    if (!env("INDEX_SKIP_SAMPLE", "false").toBoolean) {
      val (exact, exactSeconds) = hits("exact", sampled, Map.empty)
      println(
        f"RESULT exact queries=$sample topk=$topK seconds=$exactSeconds%.1f answered=${exact.size} " +
          f"pairs_per_s=${sample * job.segments.map(_.rowCount).sum / exactSeconds}%.0f"
      )
      step(f"exact on the sample: $exactSeconds%.1fs")

      // ---- index search: recall sweep on the sample --------------------------
      env("INDEX_EFS", "64,128,256")
        .split(",")
        .map(_.trim)
        .filter(_.nonEmpty)
        .foreach { ef =>
          val (found, seconds) = hits("index", sampled, Map("ef" -> ef))
          val recall = exact.map { case (q, want) =>
            val got = found.getOrElse(q, Seq.empty).toSet
            want.count(got).toDouble / want.size
          }.sum / exact.size
          println(
            f"RESULT index ef=$ef queries=$sample topk=$topK seconds=$seconds%.1f answered=${found.size} " +
              f"recall_at_$topK=$recall%.4f queries_per_s=${sample / seconds}%.0f"
          )
          step(f"index ef=$ef on the sample: $seconds%.1fs, recall $recall%.4f")
        }
    }

    // ---- index search: the whole query set at one width ----------------------
    val fullEf = env("INDEX_FULL_EF", "128")
    val fullStarted = System.nanoTime()
    val fullHits = MilvusSearch
      .search(
        spark,
        builtOptions,
        queries,
        "vector",
        topK,
        "L2",
        "index",
        Map("ef" -> fullEf),
        None,
        Seq.empty,
        false
      )
      .count()
    val fullSeconds = (System.nanoTime() - fullStarted) / 1e9
    println(
      f"RESULT index_full ef=$fullEf queries=$queryRows topk=$topK seconds=$fullSeconds%.1f " +
        f"hits=$fullHits queries_per_s=${queryRows / fullSeconds}%.0f"
    )
    step(f"index ef=$fullEf on all $queryRows queries: $fullSeconds%.1fs")
    spark.stop()
  }

  /** The one committed job under `{root}/staging`, read from the local
    * filesystem.
    */
  private def committedJob(): JobManifest = {
    val staging = Paths.get(root, "staging")
    val manifests = Files
      .list(staging)
      .iterator()
      .asScalaSeq
      .map(_.resolve("manifest.json"))
      .filter(Files.isRegularFile(_))
    require(
      manifests.size == 1,
      s"one committed job expected under $staging, found ${manifests.size}"
    )
    JobManifest
      .fromJson(new String(Files.readAllBytes(manifests.head), UTF_8))
      .fold(error => throw error, identity)
  }

  private implicit class IteratorOps[A](it: java.util.Iterator[A]) {
    def asScalaSeq: Seq[A] = {
      val b = Seq.newBuilder[A]
      while (it.hasNext) b += it.next()
      b.result()
    }
  }

  /** A snapshot document over the job's segments, as Milvus would write one:
    * one Avro segment manifest per segment and the V3 base paths.
    */
  private def writeSnapshotDocument(job: JobManifest, key: String): Unit = {
    val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
    val node = mapper.createObjectNode()
    val info = node.putObject("snapshot_info")
    info
      .put("name", collectionName)
      .put("id", 1L)
      .put("collection_id", collectionId)
      .put("create_ts", 1L)
    info.putArray("partition_ids").add(partitionId)
    val schema = node.putObject("collection").putObject("schema")
    schema.put("name", collectionName)
    val fields = schema.putArray("fields")
    fields
      .addObject()
      .put("fieldID", 100L)
      .put("name", "id")
      .put("data_type", "Int64")
      .put("nullable", false)
      .put("is_primary_key", true)
    val vector = fields
      .addObject()
      .put("fieldID", 101L)
      .put("name", "vector")
      .put("data_type", "FloatVector")
      .put("nullable", false)
      .put("is_primary_key", false)
    vector
      .putArray("type_params")
      .addObject()
      .put("key", "dim")
      .put("value", dim.toString)
    node.put("format_version", 4)
    val definition = node
      .putArray("indexes")
      .addObject()
      .put("collectionID", collectionId)
      .put("fieldID", 101L)
      .put("indexID", 900L)
      .put("index_name", "vector_hnsw")
    definition
      .putArray("index_params")
      .addObject()
      .put("key", "index_type")
      .put("value", "HNSW")
    node.putArray("build_ids")
    val manifests = node.putArray("manifest_list")
    val dataManifests = node.putArray("storagev2_manifest_list")
    job.segments.zipWithIndex.foreach { case (segment, i) =>
      val segmentId = 1000L + i
      val manifestKey = s"snapshots/$collectionId/manifests/1/$segmentId.avro"
      val target = Paths.get(root, manifestKey)
      Files.createDirectories(target.getParent)
      Files.write(
        target,
        SegmentManifestFixture.encode(
          segmentId = segmentId,
          partitionId = partitionId,
          rows = segment.rowCount
        )
      )
      manifests.add(manifestKey)
      val manifest = mapper
        .createObjectNode()
        .put("ver", segment.manifestVersion)
        .put("base_path", segment.basePath)
      dataManifests
        .addObject()
        .put("segmentID", segmentId)
        .put("manifest", mapper.writeValueAsString(manifest))
    }
    val target = Paths.get(root, key)
    Files.createDirectories(target.getParent)
    Files.write(target, mapper.writeValueAsBytes(node))
  }
}
