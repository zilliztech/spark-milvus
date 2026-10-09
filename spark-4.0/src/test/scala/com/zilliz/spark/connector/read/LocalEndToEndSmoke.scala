package com.zilliz.spark.connector.read

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths, StandardCopyOption}
import java.util.{Base64, Comparator}
import scala.collection.JavaConverters._

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{BigIntVector, VectorSchemaRoot}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.SparkSession

import com.zilliz.milvus.jni.vector.NativeVectorLibrary
import com.zilliz.milvus.storage.manifest.SnapshotSegmentFixture
import com.zilliz.milvus.storage.snapshot.json.{
  ManifestItemJson,
  SegmentListJson
}
import com.zilliz.milvus.storage.write.commit.{CommittedSegment, JobManifest}
import com.zilliz.milvus.storage.write.exec.V3SegmentWriter
import com.zilliz.spark.connector.options.MilvusOption
import com.zilliz.spark.connector.procedure.{
  BuildIndexProcedure,
  ProcedureArgs,
  WriteSnapshotProcedure
}
import io.milvus.grpc.common.KeyValuePair
import io.milvus.grpc.schema.{CollectionSchema, DataType, FieldSchema}
import io.milvus.storage.{MilvusStorageProperties, MilvusStorageTransaction}

/** One local run through the connector, no Milvus and no object store.
  *
  * Step 1 writes a DataFrame as V3 segments (`df.write.format("milvus")`).
  *
  * Step 2 reads the segments back through the connector, with pushed filters.
  *
  * Step 3 adds a delete file to every segment and writes a snapshot over them.
  *
  * Step 4 checks an exact search over a query set against brute force.
  *
  * Step 5 runs `build_index` (HNSW) and `write_snapshot`, then measures an
  * index search through that snapshot against the exact answer.
  *
  * Step 6 writes the search result out as a second collection and reads it
  * back.
  *
  * Every vector is a function of its id, so every expected answer is computed
  * here and no step is judged against another step.
  */
object LocalEndToEndSmoke {
  private val Rows = 20000
  private val Dim = 32
  private val Clusters = 40
  private val Queries = 64
  private val K = 10
  private val CollectionId = 10L
  private val PartitionId = 20L
  private val Collection = "local_e2e"

  private val collection = CollectionSchema(
    name = Collection,
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
        typeParams = Seq(KeyValuePair("dim", Dim.toString))
      ),
      FieldSchema(
        fieldID = 102L,
        name = "category",
        dataType = DataType.Int64
      ),
      FieldSchema(
        fieldID = 103L,
        name = "name",
        dataType = DataType.VarChar,
        typeParams = Seq(KeyValuePair("max_length", "32"))
      ),
      FieldSchema(
        fieldID = 104L,
        name = "weight",
        dataType = DataType.Int64,
        nullable = true
      )
    )
  )

  private val resultCollection = CollectionSchema(
    name = "local_e2e_hits",
    fields = Seq(
      FieldSchema(
        fieldID = 100L,
        name = "rid",
        dataType = DataType.Int64,
        isPrimaryKey = true
      ),
      FieldSchema(fieldID = 101L, name = "query_id", dataType = DataType.Int64),
      FieldSchema(fieldID = 102L, name = "hit", dataType = DataType.Int64),
      FieldSchema(fieldID = 103L, name = "score", dataType = DataType.Double)
    )
  )

  // --- the data, as functions of the id ---------------------------------

  private def center(cluster: Int): Array[Float] = {
    val random = new java.util.Random(cluster.toLong)
    Array.fill(Dim)((random.nextFloat() * 2f - 1f) * 10f)
  }

  def vectorOf(id: Long): Array[Float] = {
    val c = center((id % Clusters).toInt)
    val random = new java.util.Random(id)
    c.map(v => v + (random.nextFloat() - 0.5f))
  }

  def queryOf(q: Long): Array[Float] = {
    val c = center((q % Clusters).toInt)
    val random = new java.util.Random(100000L + q)
    c.map(v => v + (random.nextFloat() - 0.5f) * 0.2f)
  }

  private def categoryOf(id: Long): Long = id % 10
  private def weightOf(id: Long): Option[Long] =
    if (id % 7 == 0) None else Some(id * 3)
  private def deleted(id: Long): Boolean = id % 1000 == 0

  private def l2(a: Array[Float], b: Array[Float]): Float = {
    var s = 0f
    var i = 0
    while (i < a.length) { val d = a(i) - b(i); s += d * d; i += 1 }
    s
  }

  private def keep(id: Long, filtered: Boolean): Boolean =
    !deleted(id) && (!filtered || (categoryOf(id) % 2 == 1 && id < 15000))

  /** Brute force answer: the k nearest ids of one query, with squared L2. */
  private def bruteForce(q: Long, filtered: Boolean): Seq[(Long, Float)] = {
    val query = queryOf(q)
    (0L until Rows.toLong)
      .filter(keep(_, filtered))
      .map(id => id -> l2(query, vectorOf(id)))
      .sortBy { case (id, score) => (score, id) }
      .take(K)
  }

  def main(args: Array[String]): Unit = {
    val root = Files.createTempDirectory("local-e2e-smoke-")
    val allocator = new RootAllocator()
    val properties =
      Map("fs.storage_type" -> "local", "fs.root_path" -> root.toString)
    var spark: SparkSession = null
    val started = System.nanoTime()
    def step(name: String): Unit =
      println(f"[${(System.nanoTime() - started) / 1e9}%7.1fs] $name")
    try {
      NativeVectorLibrary.load()
      spark = SparkSession
        .builder()
        .master("local[4]")
        .appName("local-e2e-smoke")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.warehouse.dir", root.resolve("warehouse").toString)
        .getOrCreate()
      spark.sparkContext.setLogLevel("WARN")
      val session = spark
      import session.implicits._

      // ---- 1. write: 20k rows, four tasks, four V3 segments -------------
      val vectorUdf = udf((id: Long) => vectorOf(id))
      val weightUdf = udf((id: Long) => weightOf(id))
      val source = spark
        .range(Rows)
        .select(
          col("id"),
          vectorUdf(col("id")).as("vector"),
          (col("id") % 10).as("category"),
          concat(lit("row-"), col("id")).as("name"),
          weightUdf(col("id")).as("weight")
        )
        .repartition(4)
      val tableOptions = properties ++ Map(
        MilvusOption.SnapshotMode -> "true",
        MilvusOption.SnapshotSchemaBytes -> Base64.getEncoder.encodeToString(
          collection.toByteArray
        ),
        MilvusOption.SnapshotCollectionId -> CollectionId.toString,
        MilvusOption.SnapshotPartitionIds -> PartitionId.toString,
        MilvusOption.MilvusCollectionName -> Collection
      )
      source.write.format("milvus").mode("append").options(tableOptions).save()
      val manifest = committedJob(root)
      assert(manifest.segments.size == 4, manifest.toJson)
      assert(manifest.rowCount == Rows, manifest.toJson)
      step(
        s"PASS write: ${manifest.segments.size} V3 segments, ${manifest.rowCount} rows, job ${manifest.jobId}"
      )

      // ---- 2. read back through the manifests the write produced ---------
      val manifests = SegmentListJson.encodeManifestItems(
        manifest.segments.zipWithIndex.map { case (segment, i) =>
          ManifestItemJson(
            i + 1L,
            s"""{"ver":${segment.manifestVersion},"base_path":"${segment.basePath}"}"""
          )
        }
      )
      val written = spark.read
        .format("milvus")
        .options(tableOptions + (MilvusOption.SnapshotManifests -> manifests))
        .load()
      assert(written.count() == Rows, written.count())
      assert(
        written.filter(col("category") === 3).count() == Rows / 10,
        "category filter"
      )
      assert(
        written.filter(col("weight").isNull).count() ==
          (0L until Rows.toLong).count(weightOf(_).isEmpty),
        "null filter"
      )
      val row123 = written.filter(col("id") === 123L).collect()
      assert(row123.length == 1, row123.mkString(","))
      assert(row123.head.getAs[Seq[Float]]("vector") == vectorOf(123L).toSeq)
      assert(row123.head.getAs[String]("name") == "row-123")
      assert(row123.head.getAs[Long]("weight") == 369L)
      step(
        "PASS read: every row, pushed scalar and null filters, one row's values"
      )

      // ---- 3. deletes and a snapshot document over the written segments --
      val deletedIds = (0L until Rows.toLong).filter(deleted)
      val versions = manifest.segments.map { segment =>
        segment -> addDeletes(root, allocator, properties, segment, deletedIds)
      }
      Files.write(
        root.resolve("snapshot.json"),
        snapshotJson(root, versions, snapshotId = 1L).getBytes(UTF_8)
      )
      val snapshotOptions =
        properties + (MilvusOption.SnapshotPath -> "snapshot.json")
      val fromSnapshot =
        spark.read.format("milvus").options(snapshotOptions).load()
      assert(
        fromSnapshot.count() == Rows - deletedIds.size,
        s"${fromSnapshot.count()} rows after ${deletedIds.size} deletes"
      )
      assert(
        fromSnapshot.filter(col("id") % 1000 === 0).count() == 0L,
        "deleted ids must not come back"
      )
      step(
        s"PASS snapshot: ${deletedIds.size} deletes applied, ${Rows - deletedIds.size} rows remain"
      )

      // ---- 4. exact search versus brute force ----------------------------
      val queries = (0L until Queries.toLong)
        .map(q => q -> queryOf(q))
        .toDF(SearchQueries.IdColumn, SearchQueries.VectorColumn)
      val filter = "category in [1,3,5,7,9] and id < 15000"
      def search(
          options: Map[String, String],
          mode: String,
          filtered: Boolean,
          parameters: Map[String, String] = Map.empty
      ): Map[Long, Seq[(Long, Float)]] =
        MilvusSearch
          .search(
            spark,
            options,
            queries,
            "vector",
            K,
            "L2",
            mode,
            parameters,
            if (filtered) Some(filter) else None,
            Seq("id", "category"),
            false
          )
          .collect()
          .toSeq
          .map(row =>
            (
              row.getAs[Long]("query_id"),
              row.getAs[Int]("rank"),
              row.getAs[Long]("id"),
              row.getAs[Double]("_score").toFloat,
              row.getAs[Long]("category")
            )
          )
          .groupBy(_._1)
          .map { case (q, hits) =>
            hits.foreach { case (_, _, id, _, category) =>
              assert(!deleted(id), s"query $q returned deleted id $id")
              assert(category == categoryOf(id), s"category of $id")
              if (filtered)
                assert(keep(id, filtered), s"query $q: $id violates the filter")
            }
            q -> hits.sortBy(_._2).map(h => (h._3, h._4))
          }

      def checkExact(
          got: Map[Long, Seq[(Long, Float)]],
          filtered: Boolean,
          what: String
      ): Unit = {
        assert(got.size == Queries, s"$what: ${got.size} queries answered")
        got.foreach { case (q, hits) =>
          val expected = bruteForce(q, filtered)
          assert(hits.size == K, s"$what query $q: ${hits.size} hits")
          assert(
            hits.map(_._1) == expected.map(_._1),
            s"$what query $q: got ${hits.map(_._1)} expected ${expected.map(_._1)}"
          )
          hits.zip(expected).foreach { case ((_, score), (_, want)) =>
            assert(
              math.abs(score - want) <= 1e-3f * math.max(1f, want),
              s"$what query $q: score $score, brute force $want"
            )
          }
        }
      }
      checkExact(
        search(snapshotOptions, "exact", filtered = false),
        false,
        "exact"
      )
      checkExact(
        search(snapshotOptions, "exact", filtered = true),
        true,
        "exact+filter"
      )
      step(
        s"PASS exact search: $Queries queries x top-$K, with and without a scalar filter, ids and squared L2 equal brute force"
      )

      // ---- 5. build_index, write_snapshot, index search ------------------
      val built = BuildIndexProcedure.run(
        ProcedureArgs(
          values = Map(
            "collection" -> Collection,
            "field" -> "vector",
            "output" -> "built",
            "index_type" -> "HNSW",
            "metric" -> "L2",
            "params" -> "M=16,efConstruction=200",
            "build_id" -> 7000L,
            "index_version" -> 1L,
            "store_path_version" -> 0L
          ),
          options = snapshotOptions
        )
      )
      assert(built.size == 4, built.mkString(","))
      assert(built.map(_.getLong(2)).sum == Rows, built.mkString(","))
      val jobId = built.head.getString(6)
      val publish = WriteSnapshotProcedure.run(
        ProcedureArgs(
          values = Map(
            "collection" -> Collection,
            "job" -> jobId,
            "input" -> "built",
            "snapshot_id" -> 5000L,
            "snapshot_name" -> "built-5000",
            // The base sits under staging/, outside the snapshot's root, so
            // this snapshot is one only the connector reads.
            "restorable" -> false
          ),
          options = snapshotOptions
        )
      )
      val snapshotKey = publish.head.getString(0)
      assert(publish.head.getInt(4) == 4, publish.mkString(","))
      step(
        s"PASS build_index + write_snapshot: 4 HNSW indexes, snapshot $snapshotKey"
      )

      val builtOptions = properties + (MilvusOption.SnapshotPath -> snapshotKey)
      checkExact(
        search(builtOptions, "exact", filtered = false),
        false,
        "exact@built"
      )
      Seq(false, true).foreach { filtered =>
        val index = search(builtOptions, "index", filtered, Map("ef" -> "128"))
        assert(index.size == Queries, s"index: ${index.size} queries answered")
        val recall = index.map { case (q, hits) =>
          val expected = bruteForce(q, filtered).map(_._1).toSet
          hits.count(h => expected(h._1)).toDouble / K
        }.sum / Queries
        index.foreach { case (q, hits) =>
          hits.foreach { case (id, score) =>
            val want = l2(queryOf(q), vectorOf(id))
            assert(
              math.abs(score - want) <= 1e-3f * math.max(1f, want),
              s"index query $q: score of $id is $score, distance is $want"
            )
          }
        }
        assert(recall >= 0.95, f"index recall $recall%.3f (filtered=$filtered)")
        step(
          f"PASS index search (filtered=$filtered): recall@$K = $recall%.3f against brute force, scores are true distances"
        )
      }

      // ---- 6. write the hits as a second collection and read them back ---
      val hits = MilvusSearch
        .search(
          spark,
          builtOptions,
          queries,
          "vector",
          K,
          "L2",
          "index",
          Map("ef" -> "128"),
          None,
          Seq("id"),
          false
        )
        .select(
          (col("query_id") * 100 + col("rank")).as("rid"),
          col("query_id"),
          col("id").as("hit"),
          col("_score").as("score")
        )
      // A column read from a Milvus table carries `milvus.field_id` metadata.
      // An alias keeps it (Spark 4.0 keeps it even through
      // `as(name, Metadata.empty)`), and the next write compares it with its
      // own schema and refuses `hit` = field 100. The metadata is dropped by
      // rebuilding the frame with a plain schema.
      val plainHits = spark.createDataFrame(
        hits.rdd,
        org.apache.spark.sql.types.StructType(
          hits.schema.fields.map(_.copy(metadata = Metadata.empty))
        )
      )
      val resultRoot = root.resolve("results")
      Files.createDirectories(resultRoot)
      val resultProperties =
        Map("fs.storage_type" -> "local", "fs.root_path" -> resultRoot.toString)
      val resultOptions = resultProperties ++ Map(
        MilvusOption.SnapshotMode -> "true",
        MilvusOption.SnapshotSchemaBytes -> Base64.getEncoder.encodeToString(
          resultCollection.toByteArray
        ),
        MilvusOption.SnapshotCollectionId -> "11",
        MilvusOption.SnapshotPartitionIds -> "21",
        MilvusOption.MilvusCollectionName -> resultCollection.name
      )
      plainHits.write
        .format("milvus")
        .mode("append")
        .options(resultOptions)
        .save()
      val resultJob = committedJob(resultRoot)
      assert(resultJob.rowCount == Queries * K, resultJob.toJson)
      val back = spark.read
        .format("milvus")
        .options(
          resultOptions + (MilvusOption.SnapshotManifests ->
            SegmentListJson.encodeManifestItems(
              resultJob.segments.zipWithIndex.map { case (segment, i) =>
                ManifestItemJson(
                  i + 1L,
                  s"""{"ver":${segment.manifestVersion},"base_path":"${segment.basePath}"}"""
                )
              }
            ))
        )
        .load()
      assert(back.count() == Queries * K, back.count())
      val best = back
        .groupBy("query_id")
        .agg(min("score").as("best"))
        .collect()
        .map(r => r.getAs[Long]("query_id") -> r.getAs[Double]("best"))
        .toMap
      assert(best.size == Queries, best.size)
      best.foreach { case (q, score) =>
        val want = bruteForce(q, filtered = false).head._2
        assert(
          math.abs(score - want) <= 1e-3 * math.max(1.0, want),
          s"query $q: best written score $score, brute force $want"
        )
      }
      step(
        s"PASS write results: ${Queries * K} hits written as a second collection and read back; each query's best hit is the true nearest"
      )
      println("ALL PASS")
    } finally {
      if (spark != null) spark.stop()
      try allocator.close()
      finally {
        val paths = Files.walk(root)
        try
          paths
            .sorted(Comparator.reverseOrder[Path]())
            .forEach(Files.deleteIfExists(_))
        finally paths.close()
      }
    }
  }

  /** Where the local backend put a key: it appends the key to `fs.root_path`.
    */
  private def local(root: Path, key: String): Path =
    Paths.get(root.toString + "/" + key).normalize()

  /** The one committed job under `{root}/staging`. */
  private def committedJob(root: Path): JobManifest = {
    val walk = Files.walk(root)
    val manifests =
      try
        walk.iterator.asScala
          .filter(p =>
            p.getFileName.toString == "manifest.json" &&
              p.getParent.getParent.getFileName.toString == "staging" &&
              Files.exists(p.getParent.resolve("_committed"))
          )
          .toList
      finally walk.close()
    assert(manifests.size == 1, s"one committed job expected, got $manifests")
    JobManifest
      .fromJson(new String(Files.readAllBytes(manifests.head), UTF_8))
      .fold(e => throw e, identity)
  }

  /** Writes one delete file into the segment and commits it as a delta log;
    * returns the manifest version that carries it.
    */
  private def addDeletes(
      root: Path,
      allocator: RootAllocator,
      properties: Map[String, String],
      segment: CommittedSegment,
      ids: Seq[Long]
  ): Long = {
    val arrow = new Schema(
      Seq("pk", "ts")
        .map(n =>
          new Field(n, FieldType.notNullable(new ArrowType.Int(64, true)), null)
        )
        .asJava
    )
    val scratch =
      s"delete-scratch/${segment.partitionId}-${segment.basePath.hashCode.abs}"
    val writer = new V3SegmentWriter(scratch, arrow, properties, allocator)
    val target = local(root, segment.basePath).resolve("_delta/delete.parquet")
    try {
      val batch = VectorSchemaRoot.create(arrow, allocator)
      try {
        batch.allocateNew()
        val pk = batch.getVector("pk").asInstanceOf[BigIntVector]
        val ts = batch.getVector("ts").asInstanceOf[BigIntVector]
        ids.zipWithIndex.foreach { case (id, i) =>
          pk.setSafe(i, id); ts.setSafe(i, Long.MaxValue / 2)
        }
        batch.setRowCount(ids.size)
        writer.write(batch)
      } finally batch.close()
      val groups = writer.finish()
      try {
        Files.createDirectories(target.getParent)
        Files.copy(
          local(root, groups.files(0).head),
          target,
          StandardCopyOption.REPLACE_EXISTING
        )
      } finally groups.close()
    } finally writer.close()
    val nativeProperties = new MilvusStorageProperties()
    var transaction: MilvusStorageTransaction = null
    try {
      nativeProperties.create(properties)
      transaction = new MilvusStorageTransaction()
      transaction.begin(segment.basePath, nativeProperties.getPtr, -1L, 0, 1)
      transaction.addDeltaLog("delete.parquet", ids.size.toLong)
      transaction.commit()
    } finally {
      try if (transaction != null) transaction.destroy()
      finally nativeProperties.free()
    }
  }

  /** A snapshot document over the written segments, as Milvus would write one:
    * no index yet, one Avro manifest per segment, the storage manifest at the
    * version that carries the deletes.
    */
  private def snapshotJson(
      root: Path,
      segments: Seq[(CommittedSegment, Long)],
      snapshotId: Long
  ): String = {
    val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
    val node = mapper.createObjectNode()
    val info = node.putObject("snapshot_info")
    info
      .put("name", Collection)
      .put("id", snapshotId)
      .put("collection_id", CollectionId)
      .put("create_ts", 1L)
    info.putArray("partition_ids").add(PartitionId)
    val schema = node.putObject("collection").putObject("schema")
    schema.put("name", collection.name)
    val fields = schema.putArray("fields")
    collection.fields.foreach { field =>
      val f = fields
        .addObject()
        .put("fieldID", field.fieldID)
        .put("name", field.name)
        .put("data_type", field.dataType.toString)
        .put("nullable", field.nullable)
        .put("is_primary_key", field.isPrimaryKey)
      if (field.typeParams.nonEmpty) {
        val params = f.putArray("type_params")
        field.typeParams.foreach(p =>
          params.addObject().put("key", p.key).put("value", p.value)
        )
      }
    }
    node.put("format_version", 4)
    val definitions = node
      .putArray("indexes")
      .addObject()
      .put("collectionID", CollectionId)
      .put("fieldID", 101L)
      .put("indexID", 900L)
      .put("index_name", "vector_hnsw")
    definitions
      .putArray("index_params")
      .addObject()
      .put("key", "index_type")
      .put("value", "HNSW")
    node.putArray("build_ids")
    val manifests = node.putArray("manifest_list")
    val dataManifests = node.putArray("storagev2_manifest_list")
    segments.zipWithIndex.foreach { case ((segment, version), i) =>
      val segmentId = 1000L + i
      val key =
        s"files/snapshots/$CollectionId/manifests/$snapshotId/$segmentId.avro"
      val target = local(root, key)
      Files.createDirectories(target.getParent)
      Files.write(
        target,
        SnapshotSegmentFixture.encode(
          segmentId = segmentId,
          partitionId = PartitionId,
          rows = segment.rowCount
        )
      )
      manifests.add(key)
      val manifest = mapper
        .createObjectNode()
        .put("ver", version)
        .put("base_path", segment.basePath)
      dataManifests
        .addObject()
        .put("segmentID", segmentId)
        .put("manifest", mapper.writeValueAsString(manifest))
    }
    mapper.writeValueAsString(node)
  }
}
